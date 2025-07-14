# -*- coding: utf-8 -*-

import errno
import select
import socket
import ssl

import logging
from typing import Optional, Type, cast

log = logging.getLogger(__name__)


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
    def size(self, size):
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
    def capacity(self, capacity):
        if capacity < self._size:
            raise IndexError
        data = bytearray(capacity)
        data[:self._size] = self._data[:self._size]
        self._data = data


class TcpConnection(object):
    """
    Accepted TCP connection.
    """

    connectionSocket = None # accepted socket object
    remoteAddress = None # remote address
    closeType = None # Immediate, AfterSend
    sendBuffer: TcpBuffer # buffer to hold data waiting to be sent
    receiveBuffer: TcpBuffer # buffer to hold data received before consumption
    hasPendingWork: bool = False # should this socket be submitted as a 'readable' socket even if no new data is received?

    def __init__(self, connectionSocket, remoteAddress):
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
    _api = None # an optional api object to receive callback on
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
    _backlog = 5 # number of connection to backlog before accepting
    _resuseAddress = True # allow reuse of TCP port

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

    def __init__(self):
        self._servers = []
        self._clients = []

    def __del__(self):
        self.Destroy()

    def Destroy(self):
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

    def SpinOnce(self, timeout=0):
        """Spin all sockets once, without creating threads.

        :param timeout: in seconds, pass in 0 to not wait for socket events, otherwise, will wait up to specified timeout
        """
        newConnections = [] # list of tuple (serverClient, connection)

        # construct a list of connections to select on
        rsockets = []
        wsockets = []
        xsockets = []

        # bind and listen for server
        serverSockets: dict[socket.socket, TcpServer] = {} # map from serverSocket to server
        for server in self._servers:
            server._EnsureServerSocket()
            if server._serverSocket is not None:
                serverSockets[server._serverSocket] = server
                rsockets.append(server._serverSocket)
                xsockets.append(server._serverSocket)

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
        socketConnections = {}
        for serverClient in self._servers + self._clients:
            for connection in serverClient._connections:
                if connection.receiveBuffer.size >= connection.receiveBuffer.capacity:
                    connection.receiveBuffer.capacity *= 2
                rsockets.append(connection.connectionSocket)
                if connection.sendBuffer.size > 0:
                    wsockets.append(connection.connectionSocket)
                xsockets.append(connection.connectionSocket)
                socketConnections[connection.connectionSocket] = (serverClient, connection)

        # use epoll if available, fallback to poll
        use_epoll = hasattr(select, 'epoll')
        poller = select.epoll() if use_epoll else select.poll()
        
        try:
            socket_events = {}
            all_sockets = set(rsockets + wsockets + xsockets)
            
            for sock in all_sockets:
                events = 0
                if sock in rsockets:
                    events |= select.EPOLLIN if use_epoll else select.POLLIN
                if sock in wsockets:
                    events |= select.EPOLLOUT if use_epoll else select.POLLOUT
                events |= select.EPOLLERR if use_epoll else select.POLLERR
                
                try:
                    poller.register(sock.fileno(), events)
                    socket_events[sock.fileno()] = sock
                except (OSError, ValueError) as e:
                    log.warning('failed to register socket %s: %s', sock, e)
                    continue

            timeout_ms = int(timeout * 1000) if timeout > 0 else 0
            while True:
                try:
                    events = poller.poll(timeout_ms)
                    break
                except (OSError, select.error) as e:
                    if e.args[0] != errno.EINTR:
                        raise
            
            # convert events to select-style lists
            rlist, wlist, xlist = [], [], []
            for fd, event in events:
                sock = socket_events.get(fd)
                if sock is None:
                    continue
                    
                if use_epoll:
                    if event & select.EPOLLIN:
                        rlist.append(sock)
                    if event & select.EPOLLOUT:
                        wlist.append(sock)
                    if event & (select.EPOLLERR | select.EPOLLHUP):
                        xlist.append(sock)
                else:
                    if event & select.POLLIN:
                        rlist.append(sock)
                    if event & select.POLLOUT:
                        wlist.append(sock)
                    if event & (select.POLLERR | select.POLLHUP):
                        xlist.append(sock)
        finally:
            try:
                if use_epoll:
                    cast(select.epoll, poller).close()
            except Exception:
                pass

        # handle sockets that can read
        receivedConnections = [] # list of tuple (serverClient, connection)
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
                except Exception as e:
                    connection.closeType = 'Immediate'
                    log.exception('error while trying to send on connection %s: %s', connection, e)
                    continue
                if sent > 0:
                    connection.sendBuffer.size -= sent

        # handle sockets with exceptions
        for xsocket in xlist:
            server = serverSockets.get(xsocket)
            if server is not None:
                log.error('error in server socket, will recreate')
                server._DestroyServerSocket()
                continue

            serverClient, connection = socketConnections[xsocket]
            connection.closeType = 'Immediate'
            log.error('error in connection, maybe closed: %s', connection)

        # handle closed connections
        closeConnections = [] # list of tuple (serverClient, connection)
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
