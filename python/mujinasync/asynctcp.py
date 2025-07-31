# -*- coding: utf-8 -*-

import errno
import select
import selectors
import socket
import ssl

import logging
from typing import Any, Generic, Literal, Optional, Type, TypeVar, Union, cast

log = logging.getLogger(__name__)

TConnectionType = TypeVar('TConnectionType', bound='TcpConnection')
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


class TcpServerClientBase(Generic[TConnectionType]):

    _ctx: Optional['TcpContext'] # a TcpContext
    _endpoint: tuple[str, int] # connection endpoint, should be a tuple (host, port)
    _api: Optional[Any] = None # an optional api object to receive callback on
    _connectionClass: Type[TConnectionType] # class to hold accepted connection data
    _connections: list[TConnectionType] # a list of instances of connectionClass
    _sslContext: Optional[ssl.SSLContext] = None  # a ssl.SSLContext

    def __init__(self, ctx: 'TcpContext', endpoint: tuple[str, int], api=None, connectionClass: Type[TConnectionType] = cast(Type[TConnectionType], TcpConnection), sslContext=None):
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

    def _CloseConnection(self, connection: TConnectionType) -> None:
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

    def _HandleTcpConnect(self, connection: TConnectionType) -> None:
        """Handle new connection.

        :param connection: instance of TConnectionType
        """
        self._CallApi('HandleTcpConnect', connection=connection, server=self)

    def _HandleTcpDisconnect(self, connection: TConnectionType) -> None:
        """Handle disconnect.

        :param connection: instance of TConnectionType
        """
        self._CallApi('HandleTcpDisconnect', connection=connection, server=self)

    def _HandleTcpReceive(self, connection: TConnectionType) -> None:
        """Handle receive new data.

        :param connection: instance of TConnectionType
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


class TcpClient(TcpServerClientBase[TConnectionType]):
    """
    TCP client base.
    """

    def __init__(self, ctx: 'TcpContext', endpoint: tuple[str, int], api=None, connectionClass: Type[TConnectionType] = cast(Type[TConnectionType], TcpConnection), useSsl=False, sslKeyCert=None):
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


class TcpServer(TcpServerClientBase[TConnectionType]):
    """
    TCP server base.
    """
    _serverSocket: Optional[socket.socket] = None # listening socket
    _backlog: int = 5 # number of connection to backlog before accepting
    _resuseAddress: bool = True # allow reuse of TCP port

    def __init__(self, ctx: 'TcpContext', endpoint: tuple[str, int], api=None, connectionClass: Type[TConnectionType] = cast(Type[TConnectionType], TcpConnection), sslKeyCert=None):
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
    _clients: list[TcpClient] # list of TcpClient
    _selector: Optional[selectors.DefaultSelector] # selector, on Debian it will be EpollSelector
    _registeredSocketMaskBySocket: dict[socket.socket, int] # dict of socket -> socketMask(int)

    def __init__(self):
        self._servers = []
        self._clients = []
        self._selector = selectors.DefaultSelector()
        self._registeredSocketMaskBySocket = {}

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

    def SpinOnce(self, timeout: float = 0):
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
            if rsocket in tcpServersBySocket:
                server = tcpServersBySocket[rsocket]
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
                log.debug('received nothing from connection, maybe closed: %s', connection)
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
            log.debug('closing connection from %s on endpoint %s', connection.remoteAddress, serverClient._endpoint)
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
