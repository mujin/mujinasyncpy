import errno
import socket

from pytest_mock import MockerFixture
from python.mujinasync.asynctcp import TcpClient, TcpContext, TcpServer

TIMEOUT = 0.01
MAX_RETRY_ATTEMPTS = 10


class TestAsyncTcp:
    def test_ClientSserverConnection(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12345)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        assert server._serverSocket is not None, "Server should be listening"

        client = TcpClient(ctx, endpoint)

        connected = False
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                connected = True
                break

        assert connected, "Connection should be established"
        assert len(server._connections) == 1, "Server should have 1 client connection"
        assert len(client._connections) == 1, "Client should have 1 server connection"

        server.Destroy()
        client.Destroy()

    def test_DataExchange(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12346)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()

        client = TcpClient(ctx, endpoint)

        testData = b"Hello Server!"

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        if client._connections:
            connection = client._connections[0]
            connection.sendBuffer.writeView[: len(testData)] = testData
            connection.sendBuffer.size = len(testData)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if server._connections and server._connections[0].receiveBuffer.size > 0:
                break

        assert len(server._connections) > 0
        serverConnection = server._connections[0]
        clientConnection = client._connections[0]
        assert serverConnection.receiveBuffer.size > 0
        assert clientConnection.sendBuffer.size < len(testData)
        received_data = bytes(serverConnection.receiveBuffer.readView)
        assert received_data == testData

        server.Destroy()
        client.Destroy()

    def test_Handle_EAGAIN_OnSend(self, mocker: MockerFixture) -> None:
        """Test EAGAIN / EWOULDBLOCK handling on send"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12347)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        if client._connections:
            connection = client._connections[0]
            testData = b"Test data for EAGAIN"
            connection.sendBuffer.writeView[: len(testData)] = testData
            connection.sendBuffer.size = len(testData)

            callCount = 0

            def MockSend(data):
                nonlocal callCount
                callCount += 1
                if callCount == 1:
                    error = socket.error()
                    error.errno = errno.EAGAIN
                    raise error
                elif callCount == 2:
                    error = socket.error()
                    error.errno = errno.EWOULDBLOCK
                    raise error
                else:
                    # For the successful call, we need to return a reasonable value
                    return len(data)

            mockSocketSend = mocker.patch.object(
                socket.socket, "send", side_effect=MockSend
            )

            # First call: EAGAIN
            ctx.SpinOnce(timeout=TIMEOUT)
            assert connection.closeType is None
            assert connection.sendBuffer.size == len(testData)

            # Second call: EWOULDBLOCK
            ctx.SpinOnce(timeout=TIMEOUT)
            assert connection.closeType is None
            assert connection.sendBuffer.size == len(testData)

            # Third call: original result (should succeed)
            ctx.SpinOnce(timeout=TIMEOUT)
            assert connection.sendBuffer.size == 0

            assert mockSocketSend.call_count == 3

        server.Destroy()
        client.Destroy()

    def test_Handle_EAGAIN_OnRecv(self, mocker: MockerFixture) -> None:
        """Test EAGAIN / EWOULDBLOCK handling on recv_into"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12348)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        if client._connections and server._connections:
            clientConnection = client._connections[0]
            test_data = b"Test data"
            clientConnection.sendBuffer.writeView[: len(test_data)] = test_data
            clientConnection.sendBuffer.size = len(test_data)

            # Let data be sent first
            ctx.SpinOnce(timeout=TIMEOUT)

            serverConnection = server._connections[0]

            callCount = 0

            def MockRecvInto(buffer):
                nonlocal callCount
                callCount += 1
                if callCount == 1:
                    error = socket.error()
                    error.errno = errno.EAGAIN
                    raise error
                elif callCount == 2:
                    error = socket.error()
                    error.errno = errno.EWOULDBLOCK
                    raise error
                else:
                    # For the successful call, simulate receiving some data
                    test_response = b"response"
                    buffer[: len(test_response)] = test_response
                    return len(test_response)

            mockSocketRecv = mocker.patch.object(
                socket.socket, "recv_into", side_effect=MockRecvInto
            )

            # First call: EAGAIN
            ctx.SpinOnce(timeout=TIMEOUT)
            assert serverConnection.closeType is None

            # Second call: EWOULDBLOCK
            ctx.SpinOnce(timeout=TIMEOUT)
            assert serverConnection.closeType is None

            # Third call: should work normally
            ctx.SpinOnce(timeout=TIMEOUT)
            assert serverConnection.closeType is None

            assert mockSocketRecv.call_count >= 2

        server.Destroy()
        client.Destroy()

    def test_DynamicServerClientAddRemoval(self) -> None:
        """Test dynamic addition and removal of servers and clients"""
        ctx = TcpContext()

        serverEndpoint = ("127.0.0.1", 12349)
        server = TcpServer(ctx, serverEndpoint)
        server._EnsureServerSocket()

        clients = []
        for i in range(3):
            client = TcpClient(ctx, serverEndpoint)
            clients.append(client)

            for _ in range(MAX_RETRY_ATTEMPTS):
                ctx.SpinOnce(timeout=TIMEOUT)
                if len(client._connections) > 0:
                    break

            assert len(server._connections) == i + 1, (
                f"After adding client {i}, server should have {i + 1} connections"
            )

        for i in range(3):
            client = clients.pop()
            client.Destroy()

            for _ in range(MAX_RETRY_ATTEMPTS):
                ctx.SpinOnce(timeout=TIMEOUT)

            expectedConnections = 3 - i - 1
            assert len(server._connections) == expectedConnections, (
                f"After removing client, server should have {expectedConnections} connections"
            )

        server2Endpoint = ("127.0.0.1", 12350)
        server2 = TcpServer(ctx, server2Endpoint)
        server2._EnsureServerSocket()

        client1 = TcpClient(ctx, serverEndpoint)
        client2 = TcpClient(ctx, server2Endpoint)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if (
                len(server._connections) > 0
                and len(server2._connections) > 0
                and len(client1._connections) > 0
                and len(client2._connections) > 0
            ):
                break

        assert len(server._connections) == 1, "Original server should have 1 connection"
        assert len(server2._connections) == 1, "New server should have 1 connection"

        # Remove first server while keeping second
        server.Destroy()

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)

        # Second server should still work
        assert len(server2._connections) == 1, (
            "Second server should still have its connection"
        )
        assert len(client2._connections) == 1, (
            "Client to second server should still be connected"
        )

        # Cleanup
        client1.Destroy()
        client2.Destroy()
        server2.Destroy()

    def test_MultipleServersMultipleClients(self) -> None:
        """Test multiple servers with multiple clients connecting to each"""
        SERVER_COUNT = 10
        CLIENT_PER_SERVER = 5
        ctx = TcpContext()

        assert ctx._selector

        servers = []
        for i in range(SERVER_COUNT):
            endpoint = ("127.0.0.1", 12351 + i)
            server = TcpServer(ctx, endpoint)
            server._EnsureServerSocket()
            servers.append(server)

        clients = []
        for server in servers:
            for _ in range(CLIENT_PER_SERVER):
                client = TcpClient(ctx, server._endpoint)
                clients.append(client)

        initialRegisteredSockets = len(ctx._selector.get_map())

        # Process CLIENT_PER_SERVER clients at a time
        for i in range(0, len(clients), CLIENT_PER_SERVER):
            for _ in range(MAX_RETRY_ATTEMPTS):
                ctx.SpinOnce(timeout=TIMEOUT)

            # The number of registered sockets should grow as connections are added
            currentRegistered = len(ctx._selector.get_map())
            assert currentRegistered >= initialRegisteredSockets, (
                "Registered sockets should increase or stay same"
            )

        totalConnections = sum(len(server._connections) for server in servers)
        assert totalConnections == SERVER_COUNT * CLIENT_PER_SERVER, (
            f"Expected {SERVER_COUNT * CLIENT_PER_SERVER} total connections, got {totalConnections}"
        )

        finalRegistered = len(ctx._selector.get_map())
        expectedSockets = SERVER_COUNT + totalConnections
        assert finalRegistered >= expectedSockets, (
            f"Should have at least {expectedSockets} registered sockets, got {finalRegistered}"
        )

        testData = b"Multi server/client test data"
        messagesSent = 0

        # Send data from last client for each server to create mixed read/write activity
        for i in range(0, len(clients), CLIENT_PER_SERVER):
            client = clients[i]
            if client._connections:
                conn = client._connections[0]
                conn.sendBuffer.writeView[: len(testData)] = testData
                conn.sendBuffer.size = len(testData)
                messagesSent += 1

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)

        messages_received = 0
        for server in servers:
            for conn in server._connections:
                if conn.receiveBuffer.size > 0:
                    messages_received += 1

        assert messages_received == messagesSent, (
            f"Expected {messagesSent} messages received, got {messages_received}"
        )

        for client in clients:
            client.Destroy()
        for server in servers:
            server.Destroy()
