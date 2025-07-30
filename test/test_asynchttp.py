from python.mujinasync.asynchttp import (
    HttpClient,
    HttpConnection,
    HttpRequest,
    HttpResponse,
    HttpResponseProcessing,
    HttpServer,
)
from python.mujinasync.asynctcp import TcpContext

TIMEOUT = 0.01
MAX_RETRY_ATTEMPTS = 10


class HttpAPI:
    def HandleHttpRequest(self, request: HttpRequest, **kwargs):
        response = HttpResponse(request, statusCode=200)
        response.body = b"Hello World"
        return response


class HttpClientAPI:
    def __init__(self):
        self.responses_received = []

    def HandleHttpResponse(self, response: HttpResponse, **kwargs):
        self.responses_received.append(response)


class ExceptionAPI:
    def HandleHttpRequest(self, request: HttpRequest, **kwargs):
        if not request.userState:
            request.userState = 0

        if request.userState < 2:
            request.userState += 1
            raise HttpResponseProcessing

        return HttpResponse(request, statusCode=200)


class TestAsyncHttp:
    def test_HttpRequestResponse(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 13001)

        client_api = HttpClientAPI()

        server = HttpServer(ctx, endpoint, HttpAPI())
        client = HttpClient(ctx, endpoint, client_api)

        # Wait for connection
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        assert len(server._connections) == 1, "Server should have 1 connection"
        assert len(client._connections) == 1, "Client should have 1 connection"

        # Send HTTP request
        request = HttpRequest()
        request.method = "GET"
        request.path = "/test"
        client.SendHttpRequest(request)

        # Process the request/response
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(client_api.responses_received) > 0:
                break

        assert len(client_api.responses_received) == 1, "Should receive one response"
        response = client_api.responses_received[0]
        assert response.statusCode == 200
        assert response.body == b"Hello World"
        assert response.request.path == "/test"

        server.Destroy()
        client.Destroy()

    def test_HttpResponseProcessingException(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 13002)

        server = HttpServer(ctx, endpoint, ExceptionAPI())
        client = HttpClient(ctx, endpoint, HttpClientAPI())

        # Wait for connection
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        # Send HTTP request
        request = HttpRequest()
        request.method = "GET"
        request.path = "/test"
        client.SendHttpRequest(request)

        connection: HttpConnection = server._connections[0]

        # Should meet HttpResponseProcessing
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if connection.hasPendingWork and connection.pendingRequest is not None:
                break

        assert connection.hasPendingWork is True, "Should have pending work"
        assert connection.pendingRequest is not None, "Should have pending request"

        # Should not be blocked
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if connection.hasPendingWork is False and connection.pendingRequest is None:
                break

        assert connection.hasPendingWork is False, "Should not have pending work"
        assert connection.pendingRequest is None, "Should not have pending request"

        server.Destroy()
        client.Destroy()
