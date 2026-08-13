import errno
import socket
import struct

import pytest
from pytest_mock import MockerFixture
from python.mujinasync.asynchttp import (
    HttpClient,
    HttpConnection,
    HttpRequest,
    HttpResponse,
    HttpServer,
)
from python.mujinasync.asynctcp import (
    TcpBuffer,
    TcpClient,
    TcpContext,
    TcpSendBuffer,
    TcpServer,
)
from python.mujinasync.asyncwebsocket import WebSocketConnection, WebSocketServer

TIMEOUT = 0.01
MAX_RETRY_ATTEMPTS = 10


def QueueData(buffer: TcpBuffer, data: bytes) -> None:
    """Append data to a buffer the way application code is expected to."""
    while buffer.size + len(data) > buffer.capacity:
        buffer.capacity *= 2
    writeView = buffer.writeView
    assert len(writeView) >= len(data), (
        f"writeView only offers {len(writeView)} bytes for {len(data)} bytes of data, "
        f"with size {buffer.size} and capacity {buffer.capacity}"
    )
    writeView[: len(data)] = data
    buffer.size += len(data)


def QueueDataSplitAcrossBuffers(buffer: TcpBuffer, data: bytes, splitAt: int) -> None:
    """Append data so that it ends up split across both of a buffer's underlying buffers."""
    # sizing the buffer being read to end exactly at the split leaves the rest to be staged
    buffer.capacity = buffer.size + splitAt
    QueueData(buffer, data[:splitAt])
    QueueData(buffer, data[splitAt:])
    assert buffer._stagingEnd > 0, "Data should be split across the two buffers"


def MakeHttpRequestData(path: str, body: bytes) -> bytes:
    """Return the bytes of an http request with a body of a known length."""
    return (
        f"POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    ).encode("utf-8") + body


def MakeWebSocketFrameData(
    payload: bytes, opcode: int, fin: bool = True, mask: bytes = b"\x01\x02\x03\x04"
) -> bytes:
    """Return the bytes of a masked websocket frame, the way a client sends one."""
    frame = bytearray([(0x80 if fin else 0x00) | opcode])
    if len(payload) <= 125:
        frame.append(0x80 | len(payload))
    elif len(payload) <= 0xFFFF:
        frame.append(0x80 | 0x7E)
        frame += struct.pack(">H", len(payload))
    else:
        frame.append(0x80 | 0x7F)
        frame += struct.pack(">Q", len(payload))
    frame += mask
    frame += bytearray(payload[index] ^ mask[index % 4] for index in range(len(payload)))
    return bytes(frame)


class RecordingHttpApi:
    """Http server api that records the requests it is handed."""

    def __init__(self) -> None:
        self.requests: list[HttpRequest] = []

    def HandleHttpRequest(self, request, connection, server):
        self.requests.append(request)
        response = HttpResponse(request)
        response.body = ("body for %s" % request.path).encode("utf-8")
        return response


class RecordingHttpResponseApi:
    """Http client api that records the responses it is handed."""

    def __init__(self) -> None:
        self.responses: list[HttpResponse] = []

    def HandleHttpResponse(self, response, connection, client):
        self.responses.append(response)


class RecordingWebSocketApi:
    """Websocket server api that records the messages it is handed."""

    def __init__(self) -> None:
        self.messages: list[str] = []

    def HandleWebSocketMessage(self, message, connection, server):
        self.messages.append(message)


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

        assert len(client._connections) == 0
        assert len(server._connections) == 0

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

        clients: list[TcpClient] = []
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

    def test_BufferCapacityExpansion(self) -> None:
        """Test automatic buffer capacity expansion when receive buffer is full"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12360)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        assert len(server._connections) > 0, "Server should have connection"
        serverConnection = server._connections[0]

        initialCapacity = serverConnection.receiveBuffer.capacity
        serverConnection.receiveBuffer.size = initialCapacity

        ctx.SpinOnce(timeout=TIMEOUT)

        assert serverConnection.receiveBuffer.capacity == initialCapacity * 2, (
            f"Buffer capacity should be doubled from {initialCapacity} to {initialCapacity * 2}, "
            f"got {serverConnection.receiveBuffer.capacity}"
        )

        server.Destroy()
        client.Destroy()

    def test_MultipleSimultaneousAccepts(self) -> None:
        """Test server handling multiple simultaneous connection attempts"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12361)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()

        clients = []
        for i in range(5):
            client = TcpClient(ctx, endpoint)
            clients.append(client)

        connectionsSeen = set()
        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)

            currentConnections = set()
            for conn in server._connections:
                if conn.connectionSocket:
                    currentConnections.add(id(conn.connectionSocket))

            connectionsSeen.update(currentConnections)

            # Check if all clients are connected
            if len(server._connections) == 5 and all(
                len(c._connections) > 0 for c in clients
            ):
                break

        assert len(server._connections) == 5, (
            f"Expected 5 connections, got {len(server._connections)}"
        )
        assert len(connectionsSeen) == 5, (
            f"Expected 5 unique connections, saw {len(connectionsSeen)}"
        )

        for i, client in enumerate(clients):
            assert len(client._connections) == 1, f"Client {i} should have 1 connection"

        for client in clients:
            client.Destroy()
        server.Destroy()

    def test_MultipleServersMultipleClients(self) -> None:
        """Test multiple servers with multiple clients connecting to each"""
        SERVER_COUNT = 10
        CLIENT_PER_SERVER = 5
        ctx = TcpContext()

        assert ctx._selector

        servers: list[TcpServer] = []
        for i in range(SERVER_COUNT):
            endpoint = ("127.0.0.1", 12351 + i)
            server = TcpServer(ctx, endpoint)
            server._EnsureServerSocket()
            servers.append(server)

        clients: list[TcpClient] = []
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

    def test_ConnectionCleanupDuringDestroy(self) -> None:
        """Test proper cleanup when servers/clients are destroyed with active connections"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12362)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        clients: list[TcpClient] = []

        for _ in range(3):
            client = TcpClient(ctx, endpoint)
            clients.append(client)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) == 3 and all(
                len(c._connections) > 0 for c in clients
            ):
                break

        assert len(server._connections) == 3, "Should have 3 server connections"

        testData = b"Test data before cleanup"
        for client in clients:
            if client._connections:
                conn = client._connections[0]
                conn.sendBuffer.writeView[: len(testData)] = testData
                conn.sendBuffer.size = len(testData)

        ctx.SpinOnce(timeout=TIMEOUT)

        initRegisteredSocketCount = len(ctx._selector.get_map()) if ctx._selector else 0

        server.Destroy()
        assert len(server._connections) == 0, (
            "Server should have no connections after destroy"
        )

        finalRegisteredSocketCount = (
            len(ctx._selector.get_map()) if ctx._selector else 0
        )
        assert finalRegisteredSocketCount < initRegisteredSocketCount, (
            f"Selector should have fewer sockets after cleanup: {finalRegisteredSocketCount} < {initRegisteredSocketCount}"
        )

        for client in clients:
            client.Destroy()
            assert len(client._connections) == 0, (
                "Client should have no connections after destroy"
            )

    def test_LargeSendOverManyWrites(self, mocker: MockerFixture) -> None:
        """Test a large payload sent over many partial writes, with more data queued mid send"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12363)

        server = TcpServer(ctx, endpoint)
        server._EnsureServerSocket()
        client = TcpClient(ctx, endpoint)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(server._connections) > 0 and len(client._connections) > 0:
                break

        assert len(server._connections) > 0 and len(client._connections) > 0

        clientConnection = client._connections[0]
        serverConnection = server._connections[0]
        assert clientConnection.connectionSocket is not None

        # only ever accept a small prefix of what is offered, so that the payload takes many
        # writes to send and spends most of that time partially sent. the client connection is
        # the only socket sending anything here, so it is safe to bind the mock to it.
        WRITE_LIMIT = 4096
        clientSocket = clientConnection.connectionSocket
        originalSend = socket.socket.send

        def MockSend(data: memoryview) -> int:
            return originalSend(clientSocket, data[:WRITE_LIMIT])

        mocker.patch.object(socket.socket, "send", side_effect=MockSend)

        payload = bytes(bytearray((index * 7) % 251 for index in range(200 * 1024)))
        trailer = b"queued while the payload was still being sent"

        QueueData(clientConnection.sendBuffer, payload)

        received = bytearray()
        trailerQueued = False
        for _ in range(2000):
            ctx.SpinOnce(timeout=TIMEOUT)
            received += bytes(serverConnection.receiveBuffer.readView)
            serverConnection.receiveBuffer.size = 0

            # queue more data once the payload has started but not finished being sent
            if not trailerQueued and 0 < clientConnection.sendBuffer.size < len(payload):
                QueueData(clientConnection.sendBuffer, trailer)
                trailerQueued = True

            if len(received) >= len(payload) + len(trailer):
                break

        assert trailerQueued, "Payload should have taken more than one write to send"
        assert clientConnection.sendBuffer.size == 0, "Everything queued should be sent"
        assert received == payload + trailer, (
            f"Received {len(received)} bytes, expected {len(payload) + len(trailer)}, "
            "the stream should come through in the order it was queued"
        )

        client.Destroy()
        server.Destroy()


class TestTcpSendBuffer:
    def test_QueueAndConsume(self) -> None:
        """Test the basic queue, read and consume cycle of a send buffer"""
        sendBuffer = TcpSendBuffer()
        assert sendBuffer.size == 0
        assert len(sendBuffer.readView) == 0

        QueueData(sendBuffer, b"hello world")
        assert sendBuffer.size == len(b"hello world")
        assert bytes(sendBuffer.readView) == b"hello world"

        sendBuffer.size -= len(b"hello ")
        assert sendBuffer.size == len(b"world")
        assert bytes(sendBuffer.readView) == b"world", (
            "Consumed bytes should be dropped from the front of the readable data"
        )

        sendBuffer.size -= len(b"world")
        assert sendBuffer.size == 0
        assert len(sendBuffer.readView) == 0

        QueueData(sendBuffer, b"again")
        assert bytes(sendBuffer.readView) == b"again", (
            "An emptied buffer should be reusable from its start"
        )

    def test_BufferBeingSentIsNotModified(self) -> None:
        """Test that data queued mid send neither moves nor reallocates the buffer being sent"""
        sendBuffer = TcpSendBuffer()
        payload = bytes(bytearray((index * 3) % 251 for index in range(256 * 1024)))
        QueueData(sendBuffer, payload)

        sendData = sendBuffer._readData
        sendDataSnapshot = bytes(sendData)

        consumed = 0
        while consumed < len(payload) // 2:
            assert bytes(sendBuffer.readView) == payload[consumed:], (
                "Data still to be sent should not move"
            )
            sendBuffer.size -= 4096
            consumed += 4096
            QueueData(sendBuffer, b"x" * 512)
            assert sendBuffer._readData is sendData, (
                "The buffer being sent should not be reallocated"
            )
            assert bytes(sendData) == sendDataSnapshot, (
                "The buffer being sent should not be written to"
            )

    def test_StagedDataIsSentAfterSwap(self) -> None:
        """Test that data queued mid send is sent once the buffer being sent is emptied"""
        sendBuffer = TcpSendBuffer()
        QueueData(sendBuffer, b"first")
        sendBuffer.size -= len(b"fir")

        QueueData(sendBuffer, b"second")
        assert sendBuffer.size == len(b"stsecond")
        assert bytes(sendBuffer.readView) == b"st", (
            "Only the data of the buffer being sent should be readable"
        )

        stagingData = sendBuffer._stagingData
        sendData = sendBuffer._readData
        sendBuffer.size -= len(b"st")

        assert bytes(sendBuffer.readView) == b"second", (
            "Staged data should become readable once the previous buffer is sent"
        )
        assert sendBuffer._readData is stagingData, "Buffers should be swapped"
        assert sendBuffer._stagingData is sendData, (
            "The sent buffer should be kept for staging rather than reallocated"
        )

        QueueData(sendBuffer, b"third")
        assert bytes(sendBuffer.readView) == b"secondthird", (
            "With nothing sent yet, new data should go to the buffer being sent"
        )

    def test_ConsumeAcrossBothBuffers(self) -> None:
        """Test dropping more data than the buffer being sent holds"""
        sendBuffer = TcpSendBuffer()
        QueueData(sendBuffer, b"first")
        sendBuffer.size -= len(b"fir")
        QueueData(sendBuffer, b"second")

        sendBuffer.size -= len(b"stsec")
        assert bytes(sendBuffer.readView) == b"ond"

        QueueData(sendBuffer, b"third")
        sendBuffer.size = 0
        assert sendBuffer.size == 0
        assert len(sendBuffer.readView) == 0

    def test_CapacityWhilePartiallySent(self) -> None:
        """Test that growing capacity mid send stays proportional to the data being queued"""
        sendBuffer = TcpSendBuffer()
        payload = b"p" * (4 * 1024 * 1024)
        QueueData(sendBuffer, payload)
        sendBuffer.size -= 1024

        sendCapacity = len(sendBuffer._readData)
        QueueData(sendBuffer, b"q" * 100 * 1024)

        assert len(sendBuffer._readData) == sendCapacity, (
            "The buffer being sent should not be grown to fit newly queued data"
        )
        assert sendBuffer._stagingData is not None
        assert len(sendBuffer._stagingData) < sendCapacity, (
            f"Staging buffer of {len(sendBuffer._stagingData)} bytes should be sized after the "
            "100KB staged, not after the megabytes still being sent"
        )
        assert sendBuffer.capacity - sendBuffer.size == len(sendBuffer.writeView), (
            "Capacity should account for the room left for new data"
        )

    def test_InvalidSizeAndCapacity(self) -> None:
        """Test that out of range sizes and capacities are rejected"""
        sendBuffer = TcpSendBuffer()
        QueueData(sendBuffer, b"data")

        with pytest.raises(IndexError):
            sendBuffer.size = -1
        with pytest.raises(IndexError):
            sendBuffer.size = sendBuffer.capacity + 1
        with pytest.raises(IndexError):
            sendBuffer.capacity = sendBuffer.size - 1

        assert sendBuffer.size == len(b"data"), "Rejected assignments should change nothing"
        assert bytes(sendBuffer.readView) == b"data"


class TestTcpBuffer:
    def test_ReceiveAndConsume(self) -> None:
        """Test the basic receive, read and consume cycle of a buffer"""
        buffer = TcpBuffer()
        QueueData(buffer, b"hello world")
        assert buffer.size == len(b"hello world")
        assert bytes(buffer.readView) == b"hello world"
        assert buffer.PeekBytes(5) == b"hello"
        assert buffer.PeekBytes(5, offset=6) == b"world"
        assert buffer.Find(b"world") == 6
        assert buffer.Find(b"world", 7) == -1
        assert buffer.Find(b"missing") == -1

        buffer.size -= len(b"hello ")
        assert bytes(buffer.readView) == b"world", (
            "Consumed bytes should be dropped from the front of the data"
        )
        assert buffer.Find(b"world") == 0

        buffer.size -= len(b"world")
        assert buffer.size == 0
        assert len(buffer.readView) == 0

    def test_ConsumeDoesNotMoveData(self) -> None:
        """Test that consuming from the front neither moves nor rewrites the data behind it"""
        buffer = TcpBuffer()
        payload = bytes(bytearray((index * 11) % 251 for index in range(16 * 1024)))
        QueueData(buffer, payload)

        readData = buffer._readData
        readDataSnapshot = bytes(readData)

        consumed = 0
        while consumed < len(payload) - 1024:
            buffer.size -= 1024
            consumed += 1024
            assert buffer._readData is readData, "The buffer should not be reallocated"
            assert bytes(readData) == readDataSnapshot, "The buffer should not be rewritten"
            assert bytes(buffer.readView) == payload[consumed:], (
                "Data that has not been consumed should not move"
            )

    def test_StagingWhenBufferIsFull(self) -> None:
        """Test that data received into a full buffer is staged behind it rather than moved in"""
        buffer = TcpBuffer()
        capacity = buffer.capacity
        first = b"a" * capacity
        QueueData(buffer, first)
        assert buffer._stagingEnd == 0, "Data should go into the buffer being read while it fits"

        second = b"b" * 1000
        QueueData(buffer, second)
        assert buffer._stagingEnd == len(second), "A full buffer should have new data staged"
        assert len(buffer._readData) == capacity, (
            "The buffer being read should not be grown to fit staged data"
        )
        assert buffer.size == len(first) + len(second)

        assert bytes(buffer.readView) == first + second, (
            "readView should present staged data together with the data being read"
        )
        assert buffer._stagingEnd == 0, "readView should have joined the staged data in"

    def test_PeekBytesAndFindSpanBuffers(self) -> None:
        """Test reading data that is split across both buffers without joining them"""
        buffer = TcpBuffer()
        data = b"".join(b"chunk%03d." % index for index in range(50)) + b"\r\n\r\ntail"
        QueueDataSplitAcrossBuffers(buffer, data, splitAt=len(data) // 3)

        assert buffer.size == len(data)
        assert buffer.PeekBytes(len(data)) == data, "A copy spanning both buffers should be whole"
        assert buffer.PeekBytes(10, offset=len(data) // 3 - 5) == data[len(data) // 3 - 5:][:10], (
            "A copy straddling the boundary should take the part in each buffer"
        )
        assert buffer.PeekBytes(4, offset=len(data) - 4) == data[-4:]
        assert buffer._stagingEnd > 0, "PeekBytes should not have to join the buffers"

        assert buffer.Find(b"chunk007.") == data.index(b"chunk007.")
        assert buffer.Find(b"\r\n\r\n") == data.index(b"\r\n\r\n"), (
            "A match past the boundary should still be found"
        )

    def test_ConsumeAcrossBuffersSwapsThem(self) -> None:
        """Test that the staged data is read next once the buffer being read is consumed"""
        buffer = TcpBuffer()
        data = b"first part|second part"
        splitAt = len(b"first part|")
        QueueDataSplitAcrossBuffers(buffer, data, splitAt)

        readData = buffer._readData
        stagingData = buffer._stagingData
        buffer.size -= splitAt

        assert bytes(buffer.readView) == b"second part"
        assert buffer._readData is stagingData, "Buffers should be swapped"
        assert buffer._stagingData is readData, (
            "The consumed buffer should be kept for staging rather than reallocated"
        )

        buffer.size -= len(b"second part")
        assert buffer.size == 0

    def test_InvalidPeekBytesAndFind(self) -> None:
        """Test that out of range reads are rejected"""
        buffer = TcpBuffer()
        QueueData(buffer, b"data")

        assert buffer.PeekBytes(0) == b""
        assert buffer.PeekBytes(0, offset=4) == b""
        with pytest.raises(IndexError):
            buffer.PeekBytes(5)
        with pytest.raises(IndexError):
            buffer.PeekBytes(2, offset=3)
        with pytest.raises(IndexError):
            buffer.PeekBytes(-1)
        with pytest.raises(IndexError):
            buffer.Find(b"data", -1)

        assert bytes(buffer.readView) == b"data", "Rejected reads should change nothing"


class TestHttp:
    def _MakeServer(self, api: RecordingHttpApi, port: int):
        server = HttpServer(TcpContext(), ("127.0.0.1", port), api=api)
        connection = HttpConnection(connectionSocket=None, remoteAddress=("127.0.0.1", 1234))  # type: ignore[arg-type]
        return server, connection

    def test_PipelinedRequests(self) -> None:
        """Test parsing many requests received into the buffer at once"""
        api = RecordingHttpApi()
        server, connection = self._MakeServer(api, 12364)

        requestCount = 50
        bodies = [b"body of request %d" % index for index in range(requestCount)]
        QueueData(
            connection.receiveBuffer,
            b"".join(
                MakeHttpRequestData("/path%d" % index, body)
                for index, body in enumerate(bodies)
            ),
        )
        server._HandleTcpReceive(connection)

        assert [request.path for request in api.requests] == [
            "/path%d" % index for index in range(requestCount)
        ]
        assert [bytes(request.body) for request in api.requests] == bodies
        assert connection.receiveBuffer.size == 0, "Every request should be consumed"

    def test_RequestSplitAcrossBuffers(self) -> None:
        """Test parsing a request whose header or body is split across both buffers"""
        body = b"body of the request that is split up"
        requestData = MakeHttpRequestData("/path", body)
        headerLength = requestData.index(b"\r\n\r\n") + len(b"\r\n\r\n")

        # split inside the request line, inside the headers, right at the header terminator, and
        # inside the body
        for splitAt in (5, headerLength - 10, headerLength - 2, headerLength, headerLength + 10):
            api = RecordingHttpApi()
            server, connection = self._MakeServer(api, 12364)
            QueueDataSplitAcrossBuffers(connection.receiveBuffer, requestData, splitAt)

            server._HandleTcpReceive(connection)

            assert len(api.requests) == 1, f"Request split at {splitAt} should have been parsed"
            assert api.requests[0].path == "/path"
            assert bytes(api.requests[0].body) == body
            assert connection.receiveBuffer.size == 0

    def test_LargeBodyReceivedInChunks(self) -> None:
        """Test a body larger than the buffer arriving over many receives"""
        api = RecordingHttpApi()
        server, connection = self._MakeServer(api, 12364)

        body = bytes(bytearray((index * 13) % 251 for index in range(300 * 1024)))
        requestData = MakeHttpRequestData("/big", body)
        for offset in range(0, len(requestData), 4096):
            QueueData(connection.receiveBuffer, requestData[offset:offset + 4096])
            # the event loop attempts to parse on every receive, only the last one can succeed
            server._HandleTcpReceive(connection)
            if offset + 4096 < len(requestData):
                assert not api.requests, "An incomplete request should not be handled"

        assert len(api.requests) == 1
        assert bytes(api.requests[0].body) == body
        assert connection.receiveBuffer.size == 0

    def test_LargeResponseRoundTrip(self) -> None:
        """Test large response bodies round tripping through the http server and client"""
        ctx = TcpContext()
        endpoint = ("127.0.0.1", 12365)

        serverApi = RecordingHttpApi()
        server = HttpServer(ctx, endpoint, api=serverApi)
        clientApi = RecordingHttpResponseApi()
        client = HttpClient(ctx, endpoint, api=clientApi)

        for _ in range(MAX_RETRY_ATTEMPTS):
            ctx.SpinOnce(timeout=TIMEOUT)
            if client._connections and server._connections:
                break

        paths = ["/first", "/second", "/third"]
        for path in paths:
            request = HttpRequest()
            request.path = path
            request.headers = {"Connection": "keep-alive"}
            client.SendHttpRequest(request)

        for _ in range(2000):
            ctx.SpinOnce(timeout=TIMEOUT)
            if len(clientApi.responses) >= len(paths):
                break

        assert len(clientApi.responses) == len(paths)
        for path, response in zip(paths, clientApi.responses):
            assert response.statusCode == 200
            assert response.body is not None
            assert bytes(response.body) == ("body for %s" % path).encode("utf-8")

        client.Destroy()
        server.Destroy()


class TestWebSocket:
    def _MakeServer(self, api: RecordingWebSocketApi):
        server = WebSocketServer(TcpContext(), ("127.0.0.1", 12366), api=api)
        connection = WebSocketConnection(connectionSocket=None, remoteAddress=("127.0.0.1", 1234))  # type: ignore[arg-type]
        connection.upgradedToWebSocket = True
        return server, connection

    def test_FrameSplitAcrossBuffers(self) -> None:
        """Test parsing a frame that is split across both buffers"""
        api = RecordingWebSocketApi()
        server, connection = self._MakeServer(api)

        message = "message that is long enough to be split up " * 20
        frameData = MakeWebSocketFrameData(message.encode("utf-8"), opcode=0x1)
        QueueDataSplitAcrossBuffers(connection.receiveBuffer, frameData, splitAt=5)

        server._HandleTcpReceive(connection)

        assert api.messages == [message]
        assert connection.receiveBuffer.size == 0

    def test_ContinuationFrames(self) -> None:
        """Test parsing a message sent as a first frame followed by continuation frames"""
        api = RecordingWebSocketApi()
        server, connection = self._MakeServer(api)

        parts = ["first part ", "second part ", "third part"]
        frameData = b"".join(
            MakeWebSocketFrameData(
                part.encode("utf-8"),
                opcode=0x1 if index == 0 else 0x0,
                fin=index == len(parts) - 1,
            )
            for index, part in enumerate(parts)
        )
        QueueData(connection.receiveBuffer, frameData)

        server._HandleTcpReceive(connection)

        assert api.messages == ["".join(parts)]
        assert connection.receiveBuffer.size == 0

    def test_IncompleteFrameWaitsForData(self) -> None:
        """Test that a frame is only handled once all of it has been received"""
        api = RecordingWebSocketApi()
        server, connection = self._MakeServer(api)

        message = "message received in pieces"
        frameData = MakeWebSocketFrameData(message.encode("utf-8"), opcode=0x1)
        for offset in range(0, len(frameData), 4):
            QueueData(connection.receiveBuffer, frameData[offset:offset + 4])
            server._HandleTcpReceive(connection)
            if offset + 4 < len(frameData):
                assert not api.messages, "An incomplete frame should not be handled"

        assert api.messages == [message]
        assert connection.receiveBuffer.size == 0
