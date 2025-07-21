import errno
import socket

import pytest
from pytest_mock import MockerFixture
from python.mujinasync.asynctcp import TcpClient, TcpContext, TcpServer

TIMEOUT = 0.05


class TestAsyncTcp:
    def test_ClientSserverConnection(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12345)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        assert server._serverSocket is not None, "Server should be listening"

        client = TcpClient(ctx, endpoint)

        connected = False
        for _ in range(20):
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

        for _ in range(10):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        if client._connections:
            connection = client._connections[0]
            connection.sendBuffer.writeView[: len(testData)] = testData
            connection.sendBuffer.size = len(testData)

        for _ in range(10):
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

    def test_MultipleClients(self) -> None:
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12347)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()

        # Create 3 clients one by one
        for _ in range(3):
            client = TcpClient(ctx, endpoint)

            for _ in range(10):
                ctx.SpinOnce(timeout=TIMEOUT)
                if len(client._connections) > 0:
                    break

        assert len(server._connections) == 3
        connectedClients = sum(
            1 for client in ctx._clients if len(client._connections) > 0
        )
        assert connectedClients == 3

        for client in ctx._clients:
            client.Destroy()
        server.Destroy()

    def test_Handle_EAGAIN_OnSend(self, mocker: MockerFixture) -> None:
        """Test EAGAIN / EWOULDBLOCK handling on send"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12348)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(10):
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
        endpoint = ("127.0.0.1", 12349)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(10):
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
