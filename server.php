<?php

/**
 * PHP Voice Agent Starter - Backend Server
 *
 * Simple WebSocket proxy to Deepgram's Voice Agent API using Ratchet + ReactPHP.
 * Forwards all messages (JSON and binary) bidirectionally between client and Deepgram.
 *
 * Routes:
 *   GET  /api/session       - Issue JWT session token
 *   GET  /api/metadata      - Project metadata from deepgram.toml
 *   WS   /api/voice-agent   - WebSocket proxy to Deepgram Agent API (auth required)
 */

require __DIR__ . '/vendor/autoload.php';

use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use Ratchet\Http\HttpServer;
use Ratchet\Server\IoServer;
use Ratchet\WebSocket\WsServer;
use Ratchet\WebSocket\WsServerInterface;
use Ratchet\Http\Router;
use Symfony\Component\Routing\RouteCollection;
use Symfony\Component\Routing\Route;
use Symfony\Component\Routing\Matcher\UrlMatcher;
use Symfony\Component\Routing\RequestContext;
use React\EventLoop\Loop;
use Ratchet\Client\Connector as WsConnector;
use Firebase\JWT\JWT;
use Firebase\JWT\Key;
use Dotenv\Dotenv;
use Yosymfony\Toml\Toml;

// ============================================================================
// CONFIGURATION
// ============================================================================

// Load environment variables
if (file_exists(__DIR__ . '/.env')) {
    $dotenv = Dotenv::createImmutable(__DIR__);
    $dotenv->load();
}

// Validate required environment variables
if (empty($_ENV['DEEPGRAM_API_KEY']) && empty(getenv('DEEPGRAM_API_KEY'))) {
    fwrite(STDERR, "ERROR: DEEPGRAM_API_KEY environment variable is required\n");
    fwrite(STDERR, "Please copy sample.env to .env and add your API key\n");
    exit(1);
}

$config = [
    'deepgramApiKey' => $_ENV['DEEPGRAM_API_KEY'] ?? getenv('DEEPGRAM_API_KEY'),
    'deepgramAgentUrl' => 'wss://agent.deepgram.com/v1/agent/converse',
    'port' => (int)($_ENV['PORT'] ?? getenv('PORT') ?: 8081),
    'host' => $_ENV['HOST'] ?? getenv('HOST') ?: '0.0.0.0',
];

// ============================================================================
// SESSION AUTH - JWT tokens for production security
// ============================================================================

$SESSION_SECRET = $_ENV['SESSION_SECRET'] ?? getenv('SESSION_SECRET') ?: bin2hex(random_bytes(32));
$JWT_EXPIRY = 3600; // 1 hour

/**
 * Validates JWT from WebSocket subprotocol: access_token.<jwt>
 * Returns the protocol string if valid, null if invalid.
 *
 * @param string|null $protocols Comma-separated subprotocol header
 * @return string|null The valid protocol string or null
 */
function validateWsToken(?string $protocols): ?string
{
    global $SESSION_SECRET;

    if (!$protocols) {
        return null;
    }

    $list = array_map('trim', explode(',', $protocols));
    $tokenProto = null;
    foreach ($list as $proto) {
        if (str_starts_with($proto, 'access_token.')) {
            $tokenProto = $proto;
            break;
        }
    }

    if (!$tokenProto) {
        return null;
    }

    $token = substr($tokenProto, strlen('access_token.'));
    try {
        JWT::decode($token, new Key($SESSION_SECRET, 'HS256'));
        return $tokenProto;
    } catch (\Exception $e) {
        return null;
    }
}

// ============================================================================
// WEBSOCKET VOICE AGENT PROXY
// ============================================================================

/**
 * WebSocket proxy handler for /api/voice-agent.
 * Forwards all messages bidirectionally between client and Deepgram Agent API.
 */
class VoiceAgentProxy implements MessageComponentInterface, WsServerInterface
{
    /** @var array<int, ConnectionInterface> Active Deepgram connections keyed by client resource ID */
    private array $deepgramConnections = [];

    /** @var \React\EventLoop\LoopInterface ReactPHP event loop */
    private $loop;

    /** @var array Server configuration */
    private array $config;

    /**
     * @param \React\EventLoop\LoopInterface $loop ReactPHP event loop for async WS client
     * @param array $config Server configuration array
     */
    public function __construct($loop, array $config)
    {
        $this->loop = $loop;
        $this->config = $config;
    }

    /**
     * Returns the list of supported subprotocols.
     * Accepts any access_token.* subprotocol so the client sees it echoed back.
     *
     * @param array $protocols Requested subprotocols
     * @return array Accepted subprotocols
     */
    public function getSubProtocols(): array
    {
        return [];
    }

    /**
     * Handle new client WebSocket connection.
     * Validates JWT auth and establishes upstream Deepgram connection.
     *
     * @param ConnectionInterface $clientConn The new client connection
     */
    public function onOpen(ConnectionInterface $clientConn)
    {
        // Validate JWT from subprotocol
        $protocols = $clientConn->httpRequest->getHeader('Sec-WebSocket-Protocol');
        $protocolStr = !empty($protocols) ? implode(', ', $protocols) : null;
        $validProto = validateWsToken($protocolStr);

        if (!$validProto) {
            echo "WebSocket auth failed: invalid or missing token\n";
            $clientConn->close();
            return;
        }

        echo "Client connected to /api/voice-agent (authenticated)\n";

        // Connect to Deepgram Agent API using Pawl WebSocket client
        $connector = new WsConnector($this->loop);
        $deepgramUrl = $this->config['deepgramAgentUrl'];
        $apiKey = $this->config['deepgramApiKey'];

        echo "Initiating Deepgram connection...\n";

        $connector($deepgramUrl, [], [
            'Authorization' => "Token {$apiKey}",
        ])->then(
            function (\Ratchet\Client\WebSocket $deepgramConn) use ($clientConn) {
                $resourceId = $clientConn->resourceId;
                $this->deepgramConnections[$resourceId] = $deepgramConn;

                echo "Connected to Deepgram Agent API\n";

                // Forward all messages from Deepgram to client
                $deepgramConn->on('message', function ($msg) use ($clientConn) {
                    if ($clientConn->writable ?? true) {
                        if ($msg instanceof \Ratchet\RFC6455\Messaging\MessageInterface) {
                            if ($msg->isBinary()) {
                                // Binary audio data
                                $frame = new \Ratchet\RFC6455\Messaging\Frame(
                                    $msg->getPayload(),
                                    true,
                                    \Ratchet\RFC6455\Messaging\Frame::OP_BINARY
                                );
                                $clientConn->send($frame);
                            } else {
                                // JSON control messages
                                $clientConn->send($msg->getPayload());
                            }
                        } else {
                            $clientConn->send((string)$msg);
                        }
                    }
                });

                // Handle Deepgram connection close
                $deepgramConn->on('close', function ($code = null, $reason = null) use ($clientConn, $resourceId) {
                    echo "Deepgram connection closed: {$code} {$reason}\n";
                    unset($this->deepgramConnections[$resourceId]);
                    if ($clientConn->writable ?? true) {
                        // Map reserved close codes to 1000
                        $reservedCodes = [1004, 1005, 1006, 1015];
                        $closeCode = (is_int($code) && $code >= 1000 && $code <= 4999 && !in_array($code, $reservedCodes))
                            ? $code : 1000;
                        $clientConn->close($closeCode);
                    }
                });

                // Handle Deepgram errors
                $deepgramConn->on('error', function (\Exception $e) use ($clientConn) {
                    echo "Deepgram WebSocket error: " . $e->getMessage() . "\n";
                    if ($clientConn->writable ?? true) {
                        $clientConn->send(json_encode([
                            'type' => 'Error',
                            'description' => $e->getMessage() ?: 'Deepgram connection error',
                            'code' => 'PROVIDER_ERROR',
                        ]));
                    }
                });
            },
            function (\Exception $e) use ($clientConn) {
                echo "Failed to connect to Deepgram: " . $e->getMessage() . "\n";
                if ($clientConn->writable ?? true) {
                    $clientConn->send(json_encode([
                        'type' => 'Error',
                        'description' => 'Failed to establish proxy connection',
                        'code' => 'CONNECTION_FAILED',
                    ]));
                    $clientConn->close();
                }
            }
        );
    }

    /**
     * Forward client messages to Deepgram.
     * Handles both binary audio and JSON control messages.
     *
     * @param ConnectionInterface $clientConn The client connection
     * @param string $msg The message data
     */
    public function onMessage(ConnectionInterface $clientConn, $msg)
    {
        $resourceId = $clientConn->resourceId;
        if (isset($this->deepgramConnections[$resourceId])) {
            $deepgramConn = $this->deepgramConnections[$resourceId];

            if ($msg instanceof \Ratchet\RFC6455\Messaging\MessageInterface) {
                if ($msg->isBinary()) {
                    // Forward binary audio data
                    $frame = new \Ratchet\RFC6455\Messaging\Frame(
                        $msg->getPayload(),
                        true,
                        \Ratchet\RFC6455\Messaging\Frame::OP_BINARY
                    );
                    $deepgramConn->send($frame);
                } else {
                    // Forward JSON control messages
                    $deepgramConn->send($msg->getPayload());
                }
            } else {
                $deepgramConn->send((string)$msg);
            }
        }
    }

    /**
     * Handle client disconnect. Clean up Deepgram connection.
     *
     * @param ConnectionInterface $clientConn The disconnected client
     */
    public function onClose(ConnectionInterface $clientConn)
    {
        $resourceId = $clientConn->resourceId;
        echo "Client disconnected\n";

        if (isset($this->deepgramConnections[$resourceId])) {
            $this->deepgramConnections[$resourceId]->close();
            unset($this->deepgramConnections[$resourceId]);
        }
    }

    /**
     * Handle client WebSocket errors.
     *
     * @param ConnectionInterface $clientConn The client connection
     * @param \Exception $e The error
     */
    public function onError(ConnectionInterface $clientConn, \Exception $e)
    {
        echo "Client WebSocket error: " . $e->getMessage() . "\n";
        $resourceId = $clientConn->resourceId;

        if (isset($this->deepgramConnections[$resourceId])) {
            $this->deepgramConnections[$resourceId]->close();
            unset($this->deepgramConnections[$resourceId]);
        }

        $clientConn->close();
    }
}

// ============================================================================
// HTTP REQUEST HANDLER
// ============================================================================

/**
 * HTTP handler for REST endpoints (session, metadata).
 * Also handles CORS preflight and unknown routes.
 */
class HttpHandler implements \Ratchet\Http\HttpServerInterface
{
    /** @var array Server configuration */
    private array $config;

    /**
     * @param array $config Server configuration array
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    /**
     * Handle incoming HTTP request.
     * Routes to session or metadata endpoints.
     *
     * @param ConnectionInterface $conn The HTTP connection
     * @param \Psr\Http\Message\RequestInterface|null $request The HTTP request
     */
    public function onOpen(ConnectionInterface $conn, ?\Psr\Http\Message\RequestInterface $request = null)
    {
        $path = $request ? $request->getUri()->getPath() : '/';
        $method = $request ? $request->getMethod() : 'GET';

        // CORS headers for all responses
        $corsHeaders = [
            'Access-Control-Allow-Origin' => '*',
            'Access-Control-Allow-Methods' => 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers' => 'Content-Type, Authorization',
        ];

        // Handle CORS preflight
        if ($method === 'OPTIONS') {
            $this->sendResponse($conn, 204, '', $corsHeaders);
            return;
        }

        if ($path === '/api/session' && $method === 'GET') {
            $this->handleSession($conn, $corsHeaders);
        } elseif ($path === '/api/metadata' && $method === 'GET') {
            $this->handleMetadata($conn, $corsHeaders);
        } elseif ($path === '/health' && $method === 'GET') {
            $this->sendResponse($conn, 200, json_encode(['status' => 'ok']), array_merge($corsHeaders, ['Content-Type' => 'application/json']));
        } else {
            $this->sendResponse($conn, 404, json_encode([
                'error' => 'NOT_FOUND',
                'message' => 'Endpoint not found',
            ]), array_merge($corsHeaders, ['Content-Type' => 'application/json']));
        }
    }

    /**
     * GET /api/session - Issues a signed JWT for session authentication.
     *
     * @param ConnectionInterface $conn The HTTP connection
     * @param array $corsHeaders CORS headers to include
     */
    private function handleSession(ConnectionInterface $conn, array $corsHeaders): void
    {
        global $SESSION_SECRET, $JWT_EXPIRY;

        $now = time();
        $payload = [
            'iat' => $now,
            'exp' => $now + $JWT_EXPIRY,
        ];

        $token = JWT::encode($payload, $SESSION_SECRET, 'HS256');

        $this->sendResponse($conn, 200, json_encode(['token' => $token]), array_merge($corsHeaders, [
            'Content-Type' => 'application/json',
        ]));
    }

    /**
     * GET /api/metadata - Returns project metadata from deepgram.toml.
     *
     * @param ConnectionInterface $conn The HTTP connection
     * @param array $corsHeaders CORS headers to include
     */
    private function handleMetadata(ConnectionInterface $conn, array $corsHeaders): void
    {
        try {
            $tomlPath = __DIR__ . '/deepgram.toml';
            if (!file_exists($tomlPath)) {
                throw new \RuntimeException('deepgram.toml not found');
            }

            $parsed = Toml::parseFile($tomlPath);

            if (!isset($parsed['meta'])) {
                $this->sendResponse($conn, 500, json_encode([
                    'error' => 'INTERNAL_SERVER_ERROR',
                    'message' => 'Missing [meta] section in deepgram.toml',
                ]), array_merge($corsHeaders, ['Content-Type' => 'application/json']));
                return;
            }

            $this->sendResponse($conn, 200, json_encode($parsed['meta']), array_merge($corsHeaders, [
                'Content-Type' => 'application/json',
            ]));
        } catch (\Exception $e) {
            echo "Error reading metadata: " . $e->getMessage() . "\n";
            $this->sendResponse($conn, 500, json_encode([
                'error' => 'INTERNAL_SERVER_ERROR',
                'message' => 'Failed to read metadata from deepgram.toml',
            ]), array_merge($corsHeaders, ['Content-Type' => 'application/json']));
        }
    }

    /**
     * Send an HTTP response and close the connection.
     *
     * @param ConnectionInterface $conn The connection
     * @param int $statusCode HTTP status code
     * @param string $body Response body
     * @param array $headers Response headers
     */
    private function sendResponse(ConnectionInterface $conn, int $statusCode, string $body, array $headers = []): void
    {
        $statusTexts = [
            200 => 'OK',
            204 => 'No Content',
            404 => 'Not Found',
            500 => 'Internal Server Error',
        ];

        $statusText = $statusTexts[$statusCode] ?? 'Unknown';
        $response = "HTTP/1.1 {$statusCode} {$statusText}\r\n";

        foreach ($headers as $key => $value) {
            $response .= "{$key}: {$value}\r\n";
        }

        if ($body !== '') {
            $response .= "Content-Length: " . strlen($body) . "\r\n";
        }

        $response .= "\r\n" . $body;

        $conn->send($response);
        $conn->close();
    }

    public function onMessage(ConnectionInterface $conn, $msg) {}

    public function onClose(ConnectionInterface $conn) {}

    public function onError(ConnectionInterface $conn, \Exception $e)
    {
        echo "HTTP error: " . $e->getMessage() . "\n";
        $conn->close();
    }
}

// ============================================================================
// SERVER BOOTSTRAP
// ============================================================================

// Set up routing
$voiceAgentProxy = new VoiceAgentProxy(Loop::get(), $config);
$httpHandler = new HttpHandler($config);

// WebSocket handler with custom negotiator for access_token.* subprotocols
$wsServer = new WsServer($voiceAgentProxy);

// Ratchet's WsServer rejects dynamic subprotocols by default.
// Install a custom ServerNegotiator that accepts access_token.* protocols.
$customNegotiator = new class(new \Ratchet\RFC6455\Handshake\RequestVerifier()) extends \Ratchet\RFC6455\Handshake\ServerNegotiator {
    public function __construct(\Ratchet\RFC6455\Handshake\RequestVerifier $verifier) {
        parent::__construct($verifier);
        $this->setStrictSubProtocolCheck(false);
    }
    public function handshake(\Psr\Http\Message\RequestInterface $request): \Psr\Http\Message\ResponseInterface {
        $response = parent::handshake($request);
        if ($response->getStatusCode() === 101 && !$response->hasHeader('Sec-WebSocket-Protocol')) {
            $protocols = $request->getHeader('Sec-WebSocket-Protocol');
            $all = array_map('trim', explode(',', implode(',', $protocols)));
            foreach ($all as $proto) {
                if (str_starts_with($proto, 'access_token.')) {
                    $response = $response->withHeader('Sec-WebSocket-Protocol', $proto);
                    break;
                }
            }
        }
        return $response;
    }
};
$ref = new \ReflectionProperty($wsServer, 'handshakeNegotiator');
$ref->setAccessible(true);
$ref->setValue($wsServer, $customNegotiator);

$routes = new RouteCollection();

// WebSocket route for voice agent proxy
$routes->add('voice-agent', new Route('/api/voice-agent', [
    '_controller' => $wsServer,
], [], [], '', [], ['GET']));

// HTTP routes for session and metadata
$routes->add('session', new Route('/api/session', [
    '_controller' => $httpHandler,
], [], [], '', [], ['GET', 'OPTIONS']));

$routes->add('metadata', new Route('/api/metadata', [
    '_controller' => $httpHandler,
], [], [], '', [], ['GET', 'OPTIONS']));

// HTTP route: /health
$routes->add('health', new Route('/health', [
    '_controller' => $httpHandler,
], [], [], '', [], ['GET', 'OPTIONS']));

// Catch-all for other HTTP requests (returns 404 with CORS)
$routes->add('fallback', new Route('/{path}', [
    '_controller' => $httpHandler,
], ['path' => '.*'], [], '', [], ['GET', 'POST', 'OPTIONS']));

$urlMatcher = new UrlMatcher($routes, new RequestContext());
$router = new Router($urlMatcher);

$PORT = $config['port'];
$HOST = $config['host'];

// Use the shared Loop::get() singleton so outbound WebSocket connections
// (Deepgram proxy) run on the same loop as the server.
// IoServer::factory() creates a separate loop via LoopFactory::create() which
// would prevent the Deepgram connector's promises from resolving.
$loop = \React\EventLoop\Loop::get();
$socket = new \React\Socket\SocketServer("{$HOST}:{$PORT}", [], $loop);
$server = new IoServer(new HttpServer($router), $socket, $loop);

// Graceful shutdown on SIGINT/SIGTERM
if (function_exists('pcntl_signal')) {
    $shutdownHandler = function (int $signal) use ($server) {
        echo "\nShutting down server...\n";
        $server->socket->close();
        $server->loop->addTimer(1, function () use ($server) {
            $server->loop->stop();
        });
    };
    $server->loop->addSignal(SIGTERM, $shutdownHandler);
    $server->loop->addSignal(SIGINT, $shutdownHandler);
}

// ============================================================================
// STARTUP
// ============================================================================

echo "\n" . str_repeat('=', 70) . "\n";
echo "Backend API Server running at http://localhost:{$config['port']}\n";
echo "\n";
echo "GET  /api/session\n";
echo "WS   /api/voice-agent (auth required)\n";
echo "GET  /api/metadata\n";
echo "GET  /health\n";
echo str_repeat('=', 70) . "\n\n";

$server->run();
