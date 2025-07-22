# -*- coding: utf-8 -*-

import errno
import select
import selectors
import socket
import ssl

import logging
from typing import Any, Literal, Optional, Type, Union

log = logging.getLogger(__name__)

TcpServerClient = Union['TcpServer', 'TcpClient']

class TcpBuffer(object):
    """Buffer object to manage socket receive and send
    """

    _data: bytearray
    _size: int = 0

    def __init__(self):
        self._data = bytearray(64 * 1024)

    @property
    def writeView(self):
        """Return a memory view safe for writing into buffer
        """
        return memoryview(self._data)[self._size:]

    @property
    def readView(self):
        """Return a memory view safe for reading from buffer
        """
        return memoryview(self._data)[:self._size]

    @property
    def size(self):
        """Length in bytes of valid data in buffer
        """
        return self._size

    @size.setter
    def size(self, size: int):
        if size < 0 or size > len(self._data):
            raise IndexError
        if size < self._size:
            self._data[:size] = self._data[self._size - size:self._size]
        self._size = size

    @property
    def capacity(self):
        """Total capacity of buffer in bytes
        """
        return len(self._data)

    @capacity.setter
    def capacity(self, capacity: int):
        if capacity < self._size:
            raise IndexError
        data = bytearray(capacity)
        data[:self._size] = self._data[:self._size]
        self._data = data


class TcpConnection(object):
    """
    Accepted TCP connection.
    """

    connectionSocket: Optional[socket.socket] # accepted socket object
    remoteAddress: tuple[str, int] # remote address
    closeType: Optional[Union[Literal['AfterSend'], Literal['Immediate']]] = None # Immediate, AfterSend
    sendBuffer: TcpBuffer # buffer to hold data waiting to be sent
    receiveBuffer: TcpBuffer # buffer to hold data received before consumption
    hasPendingWork: bool = False # should this socket be submitted as a 'readable' socket even if no new data is received?

    def __init__(self, connectionSocket: socket.socket, remoteAddress: tuple[str, int]):
        self.connectionSocket = connectionSocket
        self.remoteAddress = remoteAddress
        self.closeType = None
        self.sendBuffer = TcpBuffer()
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

    def _CloseAllConnections(self):
        """Close all connected connections
        """
        connections = self._connections
        self._connections = []
        for connection in connections:
            if connection.connectionSocket is not None:
                try:
                    connection.connectionSocket.shutdown(socket.SHUT_RDWR)
                    connection.connectionSocket.close()
                except Exception as e:
                    log.exception('failed to close connection socket: %s', e)
                connection.connectionSocket = None
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
                self._serverSocket.close()
            except Exception as e:
                log.exception('failed to close server socket: %s', e)
            self._serverSocket = None

class TcpContext(object):

    _servers: list[TcpServer] # list of TcpServer
    _clients: list[TcpClient] # lits of TcpClient
    _selector: Optional[selectors.DefaultSelector] # selector, on Debian it will be EpollSelector

    def __init__(self):
        self._servers = []
        self._clients = []
        self._selector = selectors.DefaultSelector()

    def __del__(self):
        self.Destroy()

    def Destroy(self):
        if self._selector is not None:
            self._selector.close()
            self._selector = None
        self._servers = []

    def RegisterServer(self, server):
        if server not in self._servers:
            self._servers.append(server)

    def UnregisterServer(self, server):
        if server in self._servers:
            self._servers.remove(server)

    def RegisterClient(self, client):
        if client not in self._clients:
            self._clients.append(client)

    def UnregisterClient(self, client):
        if client in self._clients:
            self._clients.remove(client)

    def _RegisterOrModifySocket(self, sock: socket.socket, expectedMask: int, socketToKey: dict) -> None:
        """
        Register or modify a socket in the selector.

        sock: Socket to register/modify
        expectedMask: Expected event mask (EVENT_READ, EVENT_WRITE, etc.)
        socketToKey: Dictionary mapping socket to SelectorKey
        """
        assert self._selector, "selector is not ready"
        if sock not in socketToKey:
            # register new socket to selector
            try:
                self._selector.register(sock, expectedMask, data=sock)
            except (OSError, ValueError) as e:
                log.warning('failed to register socket %s: %s', sock, e)
        elif socketToKey[sock].events != expectedMask:
            # update old socket in selector
            try:
                self._selector.modify(sock, expectedMask, data=sock)
            except (OSError, ValueError, KeyError) as e:
                log.warning('failed to modify socket %s: %s', sock, e)

    def SpinOnce(self, timeout=0):
        """Spin all sockets once, without creating threads.

        :param timeout: in seconds, pass in 0 to not wait for socket events, otherwise, will wait up to specified timeout
        """
        assert self._selector, "selector is not ready"
        newConnections: list[tuple[TcpServerClient, TcpConnection]] = [] # list of tuple (serverClient, connection)

        selectorMap = dict(self._selector.get_map()) # fd -> SelectorKey
        socketToKey = {key.fileobj: key for key in selectorMap.values()} # socket -> SelectorKey
        socketConnections: dict[socket.socket, tuple[TcpServerClient, TcpConnection]] = {} # Track socket->connection mapping and server sockets

        # bind and listen for server
        serverSockets: dict[socket.socket, TcpServer] = {} # map from serverSocket to server
        for server in self._servers:
            server._EnsureServerSocket()
            if server._serverSocket is not None:
                serverSockets[server._serverSocket] = server
                sock = server._serverSocket
                expectedMask = selectors.EVENT_READ

                # Register socket to listen for event
                self._RegisterOrModifySocket(sock, expectedMask, socketToKey)
                socketToKey.pop(sock, None)

        # connect for client
        for client in self._clients:
            if not client._connections:
                clientSocket = None
                try:
                    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    if client._sslContext is not None:
                        clientSocket = client._sslContext.wrap_socket(clientSocket, server_side=False)
                    clientSocket.connect(client._endpoint)
                    log.debug('new connection to %s', client._endpoint)
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
                expectedMask = selectors.EVENT_READ
                if connection.sendBuffer.size > 0:
                    expectedMask |= selectors.EVENT_WRITE
                sock = connection.connectionSocket
                self._RegisterOrModifySocket(sock, expectedMask, socketToKey)
                socketConnections[sock] = (serverClient, connection)
                socketToKey.pop(sock, None)

        # Unregister any remaining sockets (those that are no longer needed)
        for sock in socketToKey:
            try:
                self._selector.unregister(sock)
            except (OSError, ValueError, KeyError) as e:
                log.warning('failed to unregister unused socket %s: %s', sock, e)
        
        # wait for events
        while True:
            try:
                events = self._selector.select(timeout if timeout > 0 else None)
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
            server = serverSockets.get(rsocket)
            if server is not None:
                try:
                    assert server._serverSocket
                    connectionSocket, remoteAddress = server._serverSocket.accept()
                    log.debug('new connection from %s on endpoint %s', remoteAddress, server._endpoint)
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
                log.debug('received nothing from connection, maybe closed: %s', connection)
                continue

            connection.receiveBuffer.size += received
            receivedConnections.append((serverClient, connection))

        # handle sockets that can write
        for wsocket in wlist:
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
            if connection.connectionSocket is not None:
                log.debug('closing connection from %s on endpoint %s', connection.remoteAddress, serverClient._endpoint)
                try:
                    connection.connectionSocket.shutdown(socket.SHUT_RDWR)
                    connection.connectionSocket.close()
                except Exception as e:
                    log.exception('failed to close connection socket: %s', e)
                connection.connectionSocket = None
            serverClient._connections.remove(connection)

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
