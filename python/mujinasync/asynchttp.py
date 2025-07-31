# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional, Any

from .asynctcp import TcpContext, TcpServer, TcpConnection, TcpClient

import logging
log = logging.getLogger(__name__)

class HttpResponseProcessing(Exception):
    # Exception class that user logic can throw during HandleHttpRequest to indicate that they have received this
    # request, but are not yet ready to return the response (e.g, calling a slow external resource), and the
    # connection should not block for a response.
    # If this is thrown, then the active request will be stashed on the connection and the http server will
    # continue processing any other active requests
    pass


class HttpConnection(TcpConnection):
    # If this connection has already parsed a request, and that request has been pushed to the caller but backpressured,
    # then it will be stashed on the connection so that it can be re-submitted each spin until the user logic is ready
    # to return a response for it.
    pendingRequest: Optional[HttpRequest]  = None

class HttpRequest(object):
    method: str = 'GET'
    path: str = '/'
    httpVersion: str = 'HTTP/1.1'

    headers: dict[str, str]
    body: Optional[bytes] = None

    response: Optional['HttpResponse'] = None

    # For user programs that make use of non-blocking handlers
    # Any information necessary to track the progress of the request can be stored here
    userState: Optional[Any] = None

    def __init__(self):
        self.headers = {}

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join([
            ('%s=%r' % (key, getattr(self, key)))
            for key in ('method', 'path')
        ]))

class HttpResponse(object):
    request: HttpRequest

    httpVersion: str = 'HTTP/1.1'
    statusCode: int = 200
    statusText: str = 'OK'

    headers: dict[str, str]
    body: Optional[bytes] = None

    def __init__(self, request: HttpRequest, httpVersion: Optional[str] = None, statusCode=200, statusText='OK'):
        self.request = request
        self.httpVersion = httpVersion or request.httpVersion
        self.statusCode = statusCode
        self.statusText = statusText
        self.headers = {}

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join([
            ('%s=%r' % (key, getattr(self, key)))
            for key in ('statusCode', 'statusText', 'request')
        ]))


class HttpServer(TcpServer[HttpConnection]):
    """Simple HTTP server, only support requests and responses with Content-Length.
    """

    _defaultHttpResponseHeaders: dict[str, str]

    def __init__(self, ctx: TcpContext, endpoint: tuple[str, int], api=None, serverString='HttpServer/0.1', connectionClass=HttpConnection, **kwargs):
        super(HttpServer, self).__init__(ctx=ctx, endpoint=endpoint, api=api, connectionClass=connectionClass, **kwargs)
        self._defaultHttpResponseHeaders = {
            'Server': serverString,
        }

    def _HandleTcpReceive(self, connection: HttpConnection) -> None:
        while self._HandleHttpReceiveOnce(connection):
            pass

    def _HandleHttpReceiveOnce(self, connection: HttpConnection) -> bool:
        request = self._TryReceiveHttpRequest(connection)
        if not request:
            return False # not enough data received

        response = None
        try:
            response = self._HandleHttpRequest(connection, request)
        except HttpResponseProcessing:
            # if the current request would block, then cache it and re-submit it next loop to check if it is complete.
            # send no response on the connection here - the request is still processing
            connection.pendingRequest = request
            connection.hasPendingWork = True
            return False
        except Exception as e:
            log.exception('caught exception while handling http request %r: %s', request, e)

        if response is None:
            response = HttpResponse(request, statusCode=500, statusText='Internal Server Error')

        self._SendHttpResponse(connection, request, response)

        return True # handled one request, try next one

    def _HandleHttpRequest(self, connection: HttpConnection, request: HttpRequest):
        return self._CallApi('HandleHttpRequest', request=request, connection=connection, server=self)

    def _TryReceiveHttpRequest(self, connection: HttpConnection) -> Optional[HttpRequest]:
        # If this connection already has a pending request, pop it off and return it
        if connection.pendingRequest:
            request = connection.pendingRequest
            connection.pendingRequest = None
            connection.hasPendingWork = False
            return request

        bufferData = connection.receiveBuffer.readView.tobytes()
        if b'\r\n\r\n' not in bufferData:
            if len(bufferData) > 10240:
                connection.closeType = 'Immediate'
            return None

        header = bufferData.split(b'\r\n\r\n', 1)[0]
        bufferConsumed = len(header) + len(b'\r\n\r\n')

        lines = header.decode('utf-8').split('\r\n')
        parts = lines[0].split()
        if len(parts) != 3:
            log.error('failed to parse http request line %s: %s', connection, lines[0])
            connection.closeType = 'Immediate'
            return None
        method, path, httpVersion = parts

        headers: dict[str, str] = {}
        for line in lines[1:]:
            name, value = line.split(':', 1)
            headers[name.lower().strip()] = value.strip()

        body = None
        if 'content-length' in headers:
            length = int(headers['content-length'])
            if len(bufferData) < length + bufferConsumed:
                return None
            body = bytearray(bufferData[bufferConsumed:bufferConsumed + length])
            bufferConsumed += length

        request = HttpRequest()
        request.method = method.upper()
        request.path = path
        request.httpVersion = httpVersion.upper()
        request.headers = headers
        request.body = body

        connection.receiveBuffer.size -= bufferConsumed
        return request

    def _SendHttpResponse(self, connection: HttpConnection, request: HttpRequest, response: HttpResponse) -> None:
        headers = {}
        headers.update(self._defaultHttpResponseHeaders)
        if response.headers:
            headers.update(response.headers)
        body = None
        headers['Content-Length'] = '0'
        if response.body:
            body = response.body
            headers['Content-Length'] = str(len(body))

        httpKeepAlive = False
        if 'Connection' not in headers:
            if request.httpVersion == 'HTTP/1.1' and (request.headers or {}).get('connection', '') == 'keep-alive':
                httpKeepAlive = True
            headers['Connection'] = 'keep-alive' if httpKeepAlive else 'close'
        else:
            httpKeepAlive = True

        header = bytearray()
        header += ('%s %d %s\r\n' % (response.httpVersion.upper(), response.statusCode, response.statusText)).encode('utf-8')
        for name, value in headers.items():
            header += ('%s: %s\r\n' % (name, value)).encode('utf-8')
        header += b'\r\n'

        totalLength = len(header)
        if body:
            totalLength += len(body)
        while connection.sendBuffer.size + totalLength > connection.sendBuffer.capacity:
            connection.sendBuffer.capacity *= 2

        connection.sendBuffer.writeView[:len(header)] = header
        connection.sendBuffer.size += len(header)

        if body:
            connection.sendBuffer.writeView[:len(body)] = body
            connection.sendBuffer.size += len(body)

        if not httpKeepAlive:
            connection.closeType = 'AfterSend'


class HttpClient(TcpClient[HttpConnection]):
    """Simple HTTP client, only support requests and responses with Content-Length.
    """

    _defaultHttpRequestHeaders: dict[str, str]
    _inflightRequests: dict[HttpConnection, list[HttpRequest]]

    def __init__(self, ctx: TcpContext, endpoint: tuple[str, int], api=None, userAgentString='HttpClient/0.1', connectionClass=HttpConnection, **kwargs):
        super(HttpClient, self).__init__(ctx=ctx, endpoint=endpoint, api=api, connectionClass=connectionClass, **kwargs)
        self._defaultHttpRequestHeaders = {
            'User-Agent': userAgentString,
        }
        self._inflightRequests = {}

    def _HandleTcpReceive(self, connection: HttpConnection) -> None:
        while self._HandleHttpReceiveOnce(connection):
            pass

    def _HandleTcpConnect(self, connection: HttpConnection) -> None:
        self._inflightRequests[connection] = []

    def _HandleTcpDisconnect(self, connection: HttpConnection) -> None:
        self._inflightRequests.pop(connection, None)

    def _HandleHttpReceiveOnce(self, connection: HttpConnection) -> bool:
        response = self._TryReceiveHttpResponse(connection)
        if not response:
            return False # not enough data received
        self._HandleHttpResponse(connection, response)
        return True # handled one request, try next one

    def _HandleHttpResponse(self, connection: HttpConnection, response: HttpResponse):
        return self._CallApi('HandleHttpResponse', response=response, connection=connection, client=self)

    def _TryReceiveHttpResponse(self, connection: HttpConnection) -> Optional[HttpResponse]:
        bufferData = connection.receiveBuffer.readView.tobytes()
        if b'\r\n\r\n' not in bufferData:
            if len(bufferData) > 10240:
                connection.closeType = 'Immediate'
            return None

        header = bufferData.split(b'\r\n\r\n', 1)[0]
        bufferConsumed = len(header) + len(b'\r\n\r\n')

        lines = header.decode('utf-8').split('\r\n')
        parts = lines[0].split()
        if len(parts) < 2:
            log.error('failed to parse http response line %s: %s', connection, lines[0])
            connection.closeType = 'Immediate'
            return None
        httpVersion = parts[0]
        statusCode = int(parts[1])
        statusText = ' '.join(parts[2:])

        headers: dict[str, str] = {}
        for line in lines[1:]:
            name, value = line.split(':', 1)
            headers[name.lower().strip()] = value.strip()

        body = None
        if 'content-length' in headers:
            length = int(headers['content-length'])
            if len(bufferData) < length + bufferConsumed:
                return None
            body = bytearray(bufferData[bufferConsumed:bufferConsumed + length])
            bufferConsumed += length

        request = self._inflightRequests[connection].pop(0)
        response = HttpResponse(request=request, httpVersion=httpVersion, statusCode=statusCode, statusText=statusText)
        response.headers = headers
        response.body = body
        request.response = response  # type: ignore

        connection.receiveBuffer.size -= bufferConsumed
        if headers.get('connection', '') == 'close':
            connection.closeType = 'Immediate'
        return response

    def SendHttpRequest(self, request: HttpRequest) -> None:
        headers = {}
        headers.update(self._defaultHttpRequestHeaders)
        if request.headers:
            headers.update(request.headers)
        body = None
        headers['Content-Length'] = '0'
        if request.body:
            body = request.body
            headers['Content-Length'] = str(len(body))

        if 'Connection' not in headers:
            if request.httpVersion == 'HTTP/1.1':
                headers['Connection'] = 'keep-alive'
            else:
                headers['Connection'] = 'close'

        header = bytearray()
        header += ('%s %s %s\r\n' % (request.method.upper(), request.path, request.httpVersion.upper())).encode('utf-8')
        for name, value in headers.items():
            header += ('%s: %s\r\n' % (name, value)).encode('utf-8')
        header += b'\r\n'

        totalLength = len(header)
        if body:
            totalLength += len(body)
        connection = self._connections[0]
        while connection.sendBuffer.size + totalLength > connection.sendBuffer.capacity:
            connection.sendBuffer.capacity *= 2

        connection.sendBuffer.writeView[:len(header)] = header
        connection.sendBuffer.size += len(header)

        if body:
            connection.sendBuffer.writeView[:len(body)] = body
            connection.sendBuffer.size += len(body)

        self._inflightRequests[connection].append(request)
