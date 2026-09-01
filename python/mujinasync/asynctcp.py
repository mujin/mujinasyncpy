# -*- coding: utf-8 -*-

import errno
import select
import selectors
import socket
import ssl

import logging
from typing import Any, Literal, Optional, Type, Union, Callable

log = logging.getLogger(__name__)

TcpServerClient = Union['TcpServer', 'TcpClient']

_defaultBufferCapacity = 64 * 1024  # capacity in bytes that buffers are created with


class TcpBuffer(object):
    """Buffer object to manage socket receive and send

    To avoid shuffling data around inside of buffers, use a double-buffered implementation.
    Incoming and outgoing data are each their own buffer, swapped as necessary, with simple pointers to track read/write offsets.
    Callers should prefer the Find and PeekBytes APIs to seamlessly traverse multiple buffers,
    readView has to join the buffers to present a contiguous block of memory.
    """

    _readData: bytearray # buffer that data is being read out of
    _readOffset: int # offset in _readData of the first byte that has not been read yet
    _readEnd: int # offset in _readData one past the last byte of valid data
    _stagingData: Optional[bytearray] # buffer that new data accumulates in when it cannot go into _readData, allocated on first use
    _stagingEnd: int # offset in _stagingData one past the last byte of valid data

    def __init__(self):
        self._readData = bytearray(_defaultBufferCapacity)
        self._readOffset = 0
        self._readEnd = 0
        self._stagingData = None
        self._stagingEnd = 0

    def _CanAppendToReadData(self) -> bool:
        """Can we still append new data to the current read buffer?

        Always forcing new writes to buffer while the read buffer is non-full would split data more than necessary.
        """
        return self._readEnd < len(self._readData)

    @property
    def _isStaging(self) -> bool:
        """Whether new data accumulates in the staging buffer instead of the buffer being read
        """
        # data that is already staged has to stay ahead of anything appended after it
        return self._stagingEnd > 0 or not self._CanAppendToReadData()

    @property
    def _stagingCapacity(self) -> int:
        """Size in bytes the staging buffer has, or would have once it is allocated
        """
        if self._stagingData is None:
            return _defaultBufferCapacity
        return len(self._stagingData)

    def _GetStagingData(self) -> bytearray:
        """Return the staging buffer, allocating it the first time data has to be staged
        """
        if self._stagingData is None:
            self._stagingData = bytearray(_defaultBufferCapacity)
        return self._stagingData

    def _SwapBuffers(self) -> None:
        """Start reading the staged data, keeping the buffer that was just read for staging
        """
        if self._stagingEnd > 0 and self._stagingData is not None:
            self._readData, self._stagingData = self._stagingData, self._readData
            self._readEnd = self._stagingEnd
            self._stagingEnd = 0
        else:
            # nothing was staged, so both buffers are empty now. keep the larger one for reading, so
            # that a buffer that has grown large is reused instead of regrown on the next large write
            if self._stagingData is not None and len(self._stagingData) > len(self._readData):
                self._readData, self._stagingData = self._stagingData, self._readData
            self._readEnd = 0
        self._readOffset = 0

    def _JoinBuffers(self) -> None:
        """Move the staged data in behind the data being read, so that all of the data is contiguous
        """
        stagingData = self._GetStagingData()
        readSize = self._readEnd - self._readOffset
        size = readSize + self._stagingEnd

        # If all of the data fits in the existing buffer, we can just drop what's already been read
        if size <= len(self._readData):
            self._readData[:readSize] = self._readData[self._readOffset:self._readEnd]

        # If it doesn't, we need to expand the buffer. Do a standard geometric growth pattern.
        else:
            data = bytearray(max(2 * len(self._readData), size))
            data[:readSize] = self._readData[self._readOffset:self._readEnd]
            self._readData = data

        self._readData[readSize:size] = stagingData[:self._stagingEnd]
        self._readOffset = 0
        self._readEnd = size
        self._stagingEnd = 0

    def _Consume(self, count: int) -> None:
        """Drop count bytes from the front of the data, once they have been read
        """
        while count > 0 and self._readOffset < self._readEnd:
            readSize = self._readEnd - self._readOffset
            if count < readSize:
                self._readOffset += count
                return
            # the buffer has been read in full, so whatever was staged behind it is read next
            count -= readSize
            self._SwapBuffers()

    @property
    def writeView(self):
        """Return a memory view safe for writing into buffer

        Each write call must re-acquire the write view in case the buffer got swapped.
        """
        if self._isStaging:
            return memoryview(self._GetStagingData())[self._stagingEnd:]
        return memoryview(self._readData)[self._readEnd:]

    @property
    def readView(self):
        """Return a memory view safe for reading from buffer

        Covers all data in the buffer, so any data backed up into the staging area must be moved first.
        Use Find and PeekBytes to avoid this consolidation.
        """
        if self._stagingEnd > 0:
            self._JoinBuffers()
        return memoryview(self._readData)[self._readOffset:self._readEnd]

    def Find(self, data: bytes, start: int = 0) -> int:
        """Return the offset in the buffer of the first occurrence of data, or -1 if not found

        :param data: byte sequence to look for
        :param start: offset in the buffer to start looking from
        """
        if start < 0:
            raise IndexError
        readSize = self._readEnd - self._readOffset
        if start < readSize:
            index = self._readData.find(data, self._readOffset + start, self._readEnd)
            if index >= 0:
                return index - self._readOffset
        if self._stagingEnd == 0:
            return -1
        # an occurrence can straddle the two buffers, so join them to search across the boundary
        self._JoinBuffers()
        index = self._readData.find(data, self._readOffset + start, self._readEnd)
        if index < 0:
            return -1
        return index - self._readOffset

    def PeekBytes(self, count: int, offset: int = 0) -> bytearray:
        """Return a copy of count bytes of the buffer at offset, without consuming them

        :param count: number of bytes to copy out of the buffer
        :param offset: offset in the buffer of the first byte to copy out
        """
        if count < 0 or offset < 0 or offset + count > self.size:
            raise IndexError
        readSize = self._readEnd - self._readOffset
        if offset + count <= readSize:
            start = self._readOffset + offset
            return self._readData[start:start + count]
        stagingData = self._GetStagingData()
        if offset >= readSize:
            start = offset - readSize
            return stagingData[start:start + count]

        # The requested data straddles the two buffers, take the part that is in each
        data = self._readData[self._readOffset + offset:self._readEnd]
        data += stagingData[:count - len(data)]
        return data

    @property
    def size(self):
        """Length in bytes of valid data in buffer
        """
        return (self._readEnd - self._readOffset) + self._stagingEnd

    @size.setter
    def size(self, size: int):
        if size < 0:
            raise IndexError

        # If data at the front has been read, drop it.
        # Checked before capacity so that reading doesn't have to check the staging buffer.
        currentSize = self.size
        if size <= currentSize:
            self._Consume(currentSize - size)
            return

        # Can't set a size greater than actual capacity
        if size > self.capacity:
            raise IndexError

        # Data was just written through writeView, count it in the buffer it landed in
        if self._isStaging:
            self._stagingEnd += size - currentSize
        else:
            self._readEnd += size - currentSize

    @property
    def capacity(self):
        """Total capacity of buffer in bytes

        Counts the data still to be read plus the room of the buffer taking new data,
        so that capacity minus size is always how many bytes writeView can accept.
        """
        if self._isStaging:
            return (self._readEnd - self._readOffset) + self._stagingCapacity
        return len(self._readData) - self._readOffset

    @capacity.setter
    def capacity(self, capacity: int):
        # Can't resize below the held data watermark
        if capacity < self.size:
            raise IndexError

        # If we don't have anything staged, we can just grow the buffer
        if not self._isStaging:
            readSize = self._readEnd - self._readOffset
            data = bytearray(capacity)
            data[:readSize] = self._readData[self._readOffset:self._readEnd]
            self._readData = data
            self._readOffset = 0
            self._readEnd = readSize
            return

        # If we're mid-read, we can't grow that buffer, we have to grow the staging buffer.
        # Requested capacity includes the data still to be read, so only apply the remainder to the staging buffer.
        stagingData = self._GetStagingData()
        stagingCapacity = capacity - (self._readEnd - self._readOffset)
        if stagingCapacity <= len(stagingData):
            return

        # When growing, we shouldn't use the full size of the combined buffers as a baseline for geometric growth.
        # Otherwise, repeated increases with a large front buffer will dramatically increase the back buffer.
        stagingCapacity = min(stagingCapacity, 2 * len(stagingData))
        data = bytearray(stagingCapacity)
        data[:self._stagingEnd] = stagingData[:self._stagingEnd]
        self._stagingData = data


class TcpSendBuffer(TcpBuffer):
    """Buffer object to manage socket send

    Data that has started being sent must not move, since the socket is only ever handed a part of it at a time,
    so unlike TcpBuffer no data is appended to the buffer being sent once any of it has reached the socket.
    Everything queued from then on is staged behind it, and starts being sent once the buffer being sent has been sent in full.
    """

    def _CanAppendToReadData(self) -> bool:
        # If we haven't started sending data yet, allow more data to be buffered to the front buffer
        return self._readOffset == 0

    @property
    def readView(self):
        """Return a memory view of the data that can be sent right now

        Only covers the front buffer, staged data becomes readable once this buffer has been fully drained.
        This avoids moving data around within buffers, instead we just toggle to the other buffer when ready.
        """
        return memoryview(self._readData)[self._readOffset:self._readEnd]


class TcpConnection(object):
    """
    Accepted TCP connection.
    """

    connectionSocket: Optional[socket.socket] # accepted socket object
    remoteAddress: tuple[str, int] # remote address
    closeType: Optional[Union[Literal['AfterSend'], Literal['Immediate']]] = None # Immediate, AfterSend
    sendBuffer: TcpSendBuffer # buffer to hold data waiting to be sent
    receiveBuffer: TcpBuffer # buffer to hold data received before consumption
    hasPendingWork: bool = False # should this socket be submitted as a 'readable' socket even if no new data is received?

    def __init__(self, connectionSocket: socket.socket, remoteAddress: tuple[str, int]):
        self.connectionSocket = connectionSocket
        self.remoteAddress = remoteAddress
        self.closeType = None
        self.sendBuffer = TcpSendBuffer()
        self.receiveBuffer = TcpBuffer()
        self.hasPendingWork = False

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join([
            ('%s=%r' % (key, getattr(self, key)))
            for key in ('remoteAddress',)
        ]))


class TcpServerClientBase(object):

    _ctx: Optional['TcpContext'] # a TcpContext
    _endpoint: tuple[str, int] # connection endpoint, should be a tuple (host, port)
    _api: Optional[Any] = None # an optional api object to receive callback on
    _connectionClass: Type[TcpConnection] # class to hold accepted connection data
    _connections: list[TcpConnection] # a list of instances of connectionClass
    _sslContext: Optional[ssl.SSLContext] = None  # a ssl.SSLContext

    def __init__(self, ctx, endpoint, api=None, connectionClass=TcpConnection, sslContext=None):
        """Create a TCP client.

        :param endpoint: a tuple of (hostname, port)
        :param api: an api object to receive callback on
        :param connectionClass: the class to create for each TCP connection
        """
        self._ctx = ctx
        self._endpoint = endpoint
        self._api = api
        self._connectionClass = connectionClass
        self._connections = []
        self._sslContext = sslContext

    def __del__(self):
        self.Destroy()

    def Destroy(self):
        self._CloseAllConnections()

    def _CloseConnection(self, connection: TcpConnection) -> None:
        """ Close a connected connection
        1. Unregister its connectionSocket from context's selector
        2. Shutdown and close the socket
        3. Remove this connection from self._connections
        """
        if connection not in self._connections:
            return
        if connection.connectionSocket is not None:
            try:
                if self._ctx is not None:
                    self._ctx._UnregisterSocket(connection.connectionSocket)
                try:
                    connection.connectionSocket.shutdown(socket.SHUT_RDWR)
                except OSError as e:
                    # Socket may already be disconnected
                    if e.errno not in (errno.ENOTCONN, errno.EBADF):
                        raise
                connection.connectionSocket.close()
            except Exception as e:
                log.exception('failed to close connection socket: %s', e)
            connection.connectionSocket = None
        self._connections.remove(connection)

    def _CloseAllConnections(self):
        """Close all connected connections
        """
        connections = list(self._connections)  # Make a copy to avoid modification during iteration
        for connection in connections:
            self._CloseConnection(connection)
        for connection in connections:
            self._HandleTcpDisconnect(connection)

    def _HandleTcpConnect(self, connection):
        """Handle new connection.

        :param connection: instance of TcpConnection
        """
        self._CallApi('HandleTcpConnect', connection=connection, server=self)

    def _HandleTcpDisconnect(self, connection):
        """Handle disconnect.

        :param connection: instance of TcpConnection
        """
        self._CallApi('HandleTcpDisconnect', connection=connection, server=self)

    def _HandleTcpReceive(self, connection):
        """Handle recieve new data.

        :param connection: instance of TcpConnection
        """
        self._CallApi('HandleTcpReceive', connection=connection, server=self)

    def _CallApi(self, functionName, **kwargs):
        """Call hooks in passed in api object.

        :param functionName: name of the hook to call
        :return: pass through return value from hook function, or None if no hook called
        """
        if self._api is not None and hasattr(self._api, functionName):
            function = getattr(self._api, functionName)
            if callable(function):
                return function(**kwargs)
        return None


class TcpClient(TcpServerClientBase):
    """
    TCP client base.
    """

    def __init__(self, ctx, endpoint, api=None, connectionClass=TcpConnection, useSsl=False, sslKeyCert=None):
        """Create a TCP client.

        :param endpoint: a tuple of (hostname, port) to connect to
        :param api: an api object to receive callback on
        :param connectionClass: the class to create for each TCP connection
        """
        sslContext = None
        if useSsl or sslKeyCert is not None:
            sslContext = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            sslContext.check_hostname = False
            sslContext.verify_mode = ssl.CERT_NONE
            if sslKeyCert is not None:
                sslContext.load_cert_chain(sslKeyCert)
        super(TcpClient, self).__init__(ctx, endpoint=endpoint, api=api, connectionClass=connectionClass, sslContext=sslContext)
        assert self._ctx
        self._ctx.RegisterClient(self)

    def Destroy(self):
        super(TcpClient, self).Destroy()
        if self._ctx is not None:
            self._ctx.UnregisterClient(self)
            self._ctx = None


class TcpServer(TcpServerClientBase):
    """
    TCP server base.
    """
    _serverSocket: Optional[socket.socket] = None # listening socket
    _backlog: int = 5 # number of connection to backlog before accepting
    _resuseAddress: bool = True # allow reuse of TCP port

    def __init__(self, ctx, endpoint, api=None, connectionClass=TcpConnection, sslKeyCert=None):
        """Create a TCP server.

        :param endpoint: a tuple of (hostname, port), set hostname to empty string to listen wildcard
        :param api: an api object to receive callback on
        :param connectionClass: the class to create for each TCP connection
        """
        sslContext = None
        if sslKeyCert is not None:
            sslContext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            sslContext.load_cert_chain(sslKeyCert)
        super(TcpServer, self).__init__(ctx, endpoint=endpoint, api=api, connectionClass=connectionClass, sslContext=sslContext)
        assert self._ctx
        self._ctx.RegisterServer(self)

    def Destroy(self):
        self._DestroyServerSocket()
        super(TcpServer, self).Destroy()
        if self._ctx is not None:
            self._ctx.UnregisterServer(self)
            self._ctx = None

    def _EnsureServerSocket(self):
        """Ensure server socket to listen for incoming TCP connections.
        """
        # set up listening socket to accept connection
        if self._serverSocket is None:
            serverSocket = None
            try:
                serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self._resuseAddress:
                    serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                serverSocket.setblocking(False)
                serverSocket.bind(self._endpoint)
                serverSocket.listen(self._backlog)
                self._serverSocket = serverSocket
                assert self._ctx
                self._ctx._RegisterSocket(self._serverSocket, selectors.EVENT_READ)
                serverSocket = None
                log.debug('server socket listening on %s:%d', self._endpoint[0], self._endpoint[1])
            except Exception as e:
                log.exception('failed to create server socket: %s', e)
            finally:
                try:
                    if serverSocket is not None:
                        serverSocket.close()
                        serverSocket = None
                except Exception:
                    pass

    def _DestroyServerSocket(self):
        """Close listening server socket.
        """
        if self._serverSocket is not None:
            try:
                if self._ctx is not None:
                    self._ctx._UnregisterSocket(self._serverSocket)
                self._serverSocket.close()
            except Exception as e:
                log.exception('failed to close server socket: %s', e)
            self._serverSocket = None

class TcpContext(object):

    _servers: list[TcpServer] # list of TcpServer
    _clients: list[TcpClient] # lits of TcpClient
    _selector: Optional[selectors.DefaultSelector] # selector, on Debian it will be EpollSelector
    _registeredSocketMaskBySocket: dict[socket.socket, int] # dict of socket -> socketMask(int)

    # Logging function to use for verbose messages.
    # If the log facility allows for it, this will be log.verbose. If not, we will fall back to log.debug.
    _verboseLog: Callable[..., None]

    def __init__(self):
        self._servers = []
        self._clients = []
        self._selector = selectors.DefaultSelector()
        self._registeredSocketMaskBySocket = {}

        # If our logger has a verbose option, cache it to take advantage of more log levels.
        # If it doesn't, any logs that _would_ be verbose are bumped to debug.
        if hasattr(log, 'verbose'):
            self._verboseLog = log.verbose
        else:
            self._verboseLog = log.debug

    def __del__(self):
        self.Destroy()

    def Destroy(self):
        if self._selector is not None:
            self._selector.close()
            self._selector = None
        self._servers = []

    def RegisterServer(self, server: TcpServer):
        if server not in self._servers:
            self._servers.append(server)

    def UnregisterServer(self, server: TcpServer):
        if server in self._servers:
            self._servers.remove(server)

    def RegisterClient(self, client: TcpClient):
        if client not in self._clients:
            self._clients.append(client)

    def UnregisterClient(self, client: TcpClient):
        if client in self._clients:
            self._clients.remove(client)

    def _RegisterSocket(self, sock: socket.socket, mask: int) -> None:
        """
        Register socket to selector
        Should be called when socket is created
        """
        assert self._selector, "selector is not ready"
        existingMask = self._registeredSocketMaskBySocket.get(sock)
        if existingMask == mask:
            return

        try:
            if existingMask is None:
                # register new socket to selector
                self._selector.register(sock, mask, data=sock)
            else:
                # update exisiting socket in selector
                self._selector.modify(sock, mask, data=sock)
            self._registeredSocketMaskBySocket[sock] = mask
        except (OSError, ValueError) as e:
            log.warning('failed to register socket %s: %s', sock, e)

    def _UnregisterSocket(self, sock: socket.socket) -> None:
        """
        Unregister socket from selector
        Should be called when socket is destoryed
        """
        existingMask = self._registeredSocketMaskBySocket.get(sock)
        if existingMask is None:
            return

        try:
            if self._selector:
                self._selector.unregister(sock)
            self._registeredSocketMaskBySocket.pop(sock)
        except (OSError, ValueError, KeyError) as e:
            log.warning('failed to unregister unused socket %s: %s', sock, e)

    def SpinOnce(self, timeout:float = 0):
        """Spin all sockets once, without creating threads.

        :param timeout: in seconds, pass in 0 to not wait for socket events, otherwise, will wait up to specified timeout
        """
        assert self._selector, "selector is not ready"
        newConnections: list[tuple[TcpServerClient, TcpConnection]] = [] # list of tuple (serverClient, connection)
        socketConnections: dict[socket.socket, tuple[TcpServerClient, TcpConnection]] = {} # Track socket->connection mapping and server sockets

        # bind and listen for server
        tcpServersBySocket: dict[socket.socket, TcpServer] = {} # map from serverSocket to server
        for server in self._servers:
            server._EnsureServerSocket()
            if server._serverSocket is not None:
                tcpServersBySocket[server._serverSocket] = server

        # connect for client
        for client in self._clients:
            if not client._connections:
                clientSocket = None
                try:
                    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    if client._sslContext is not None:
                        clientSocket = client._sslContext.wrap_socket(clientSocket, server_side=False)
                    clientSocket.connect(client._endpoint)
                    self._verboseLog('new connection to %s', client._endpoint)
                    clientSocket.setblocking(False) # TODO: deferred non-blocking after connect finishes, not ideal
                except Exception as e:
                    if clientSocket:
                        clientSocket.close()
                    log.exception('error while trying to create client connection to %s: %s', client._endpoint, e)
                    continue
                connection = client._connectionClass(connectionSocket=clientSocket, remoteAddress=client._endpoint)
                client._connections.append(connection)
                newConnections.append((client, connection))
                timeout = 0 # force no wait at select later since we have a new connection to report right away

        # pool all the sockets
        for serverClient in self._servers + self._clients:
            for connection in serverClient._connections:
                if connection.connectionSocket is None:
                    continue
                if connection.receiveBuffer.size >= connection.receiveBuffer.capacity:
                    connection.receiveBuffer.capacity *= 2
                mask = selectors.EVENT_READ
                if connection.sendBuffer.size > 0:
                    mask |= selectors.EVENT_WRITE
                sock = connection.connectionSocket
                self._RegisterSocket(sock, mask)
                socketConnections[sock] = (serverClient, connection)

        # wait for events
        while True:
            try:
                events = self._selector.select(timeout)
                break
            except (OSError, select.error) as e:
                if e.args[0] != errno.EINTR:
                    raise

        # keep select-style
        rlist: list[socket.socket] = []
        wlist: list[socket.socket] = []
        for key, mask in events:
            sock = key.data
            if mask & selectors.EVENT_READ:
                rlist.append(sock)
            if mask & selectors.EVENT_WRITE:
                wlist.append(sock)

        # handle sockets that can read
        receivedConnections: list[tuple[TcpServerClient, TcpConnection]] = [] # list of tuple (serverClient, connection)
        for rsocket in rlist:
            server = tcpServersBySocket.get(rsocket)
            if server is not None:
                try:
                    assert server._serverSocket
                    connectionSocket, remoteAddress = server._serverSocket.accept()
                    self._verboseLog('new connection from %s on endpoint %s', remoteAddress, server._endpoint)
                    if server._sslContext is not None:
                        connectionSocket = server._sslContext.wrap_socket(connectionSocket, server_side=True)
                    connectionSocket.setblocking(False)
                except Exception as e:
                    log.exception('error while trying to accept connection: %s', e)
                    continue
                connection = server._connectionClass(connectionSocket=connectionSocket, remoteAddress=remoteAddress)
                server._connections.append(connection)
                newConnections.append((server, connection))
                continue

            if rsocket not in socketConnections:
                continue  # socket was removed during cleanup
            serverClient, connection = socketConnections[rsocket]
            try:
                received = rsocket.recv_into(connection.receiveBuffer.writeView)
            except socket.error as e:
                if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    connection.closeType = 'Immediate'
                    log.exception('error while trying to receive from connection %s: %s', connection, e)
                continue
            except Exception as e:
                connection.closeType = 'Immediate'
                log.exception('error while trying to receive from connection %s: %s', connection, e)
                continue

            if received == 0:
                connection.closeType = 'AfterSend'
                self._verboseLog('received nothing from connection, maybe closed: %s', connection)
                continue

            connection.receiveBuffer.size += received
            receivedConnections.append((serverClient, connection))

        # handle sockets that can write
        for wsocket in wlist:
            if wsocket not in socketConnections:
                continue  # socket was removed during cleanup
            serverClient, connection = socketConnections[wsocket]
            if connection.sendBuffer.size > 0:
                try:
                    sent = wsocket.send(connection.sendBuffer.readView)
                except socket.error as e:
                    if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                        connection.closeType = 'Immediate'
                        log.exception('error while trying to send on connection %s: %s', connection, e)
                    continue
                except Exception as e:
                    connection.closeType = 'Immediate'
                    log.exception('error while trying to send on connection %s: %s', connection, e)
                    continue
                if sent > 0:
                    connection.sendBuffer.size -= sent


        # handle closed connections
        closeConnections: list[tuple[TcpServerClient, TcpConnection]] = [] # list of tuple (serverClient, connection)
        for serverClient in self._servers + self._clients:
            for connection in serverClient._connections:
                if connection.closeType == 'Immediate':
                    closeConnections.append((serverClient, connection))
                elif connection.closeType == 'AfterSend' and connection.sendBuffer.size == 0:
                    closeConnections.append((serverClient, connection))
        for serverClient, connection in closeConnections:
            self._verboseLog('closing connection from %s on endpoint %s', connection.remoteAddress, serverClient._endpoint)
            serverClient._CloseConnection(connection)

        # Handle server sockets that are processing non-blocking work
        for server in self._servers:
            for connection in server._connections:
                if connection.hasPendingWork:
                    receivedConnections.append((server, connection))

        # let user code run at the very end
        for serverClient, connection in newConnections:
            serverClient._HandleTcpConnect(connection)

        for serverClient, connection in receivedConnections:
            serverClient._HandleTcpReceive(connection)

        for serverClient, connection in closeConnections:
            serverClient._HandleTcpDisconnect(connection)
