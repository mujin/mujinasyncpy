# -*- coding: utf-8 -*-


import base64
import enum
import hashlib
import struct

from .asynchttp import HttpServer, HttpConnection, HttpResponse

import logging
log = logging.getLogger(__name__)


class WebSocketConnection(HttpConnection):
    upgradedToWebSocket = False


class WebSocketOpcode(enum.Enum):
    Continuation = 0x0
    Text = 0x1
    Binary = 0x2
    Reserved3 = 0x3
    Reserved4 = 0x4
    Reserved5 = 0x5
    Reserved6 = 0x6
    Reserved7 = 0x7
    CloseConnection = 0x8
    Ping = 0x9
    Pong = 0xA
    Reserved11 = 0xB
    Reserved12 = 0xC
    Reserved13 = 0xD
    Reserved14 = 0xE
    Reserved15 = 0xF


class WebSocketServer(HttpServer):
    """WebSocket server, supports Upgrade from HTTP to WebSocket.
    """

    def __init__(self, ctx, endpoint, api=None, connectionClass=WebSocketConnection, **kwargs):
        super(WebSocketServer, self).__init__(ctx=ctx, endpoint=endpoint, api=api, connectionClass=connectionClass, **kwargs)

    def ListWebSocketConnections(self):
        """List websocket connections, oldest first, newest last.
        """
        return [
            connection
            for connection in self._connections
            if connection.upgradedToWebSocket
        ]

    def DisconnectWebSocket(self, connection):
        """Disconnect client.

        :param connection: instance of WebSocketConnection
        """
        self._SendWebSocketFrame(connection, WebSocketOpcode.CloseConnection)

    def SendWebSocketPing(self, connection, framePayload=None):
        """Send ping to client.
        """
        self._SendWebSocketFrame(connection, WebSocketOpcode.Ping, framePayload)

    def SendWebSocketMessage(self, connection, message):
        """Send text message to client.

        :param connection: instance of WebSocketConnection, optional
        :param message: unicode string
        """
        payload = message.encode('utf-8')
        self._SendWebSocketFrame(connection, WebSocketOpcode.Text, payload)

    def BroadcastWebSocketMessage(self, message):
        """Broadcast text message to all connected clients.

        :param message: unicode string
        """
        payload = message.encode('utf-8')
        for connection in self._connections:
            if connection.upgradedToWebSocket:
                self._SendWebSocketFrame(connection, WebSocketOpcode.Text, payload)

    def _HandleTcpReceive(self, connection):
        while True:
            if not connection.upgradedToWebSocket:
                if not self._HandleHttpReceiveOnce(connection):
                    break
                continue

            if not self._HandleWebSocketReceiveOnce(connection):
                break

    def _HandleWebSocketReceiveOnce(self, connection):
        received = self._TryReceiveWebSocketFrame(connection)
        if not received:
            return False  # not enough data received
        frameOpcode, framePayload = received
        if frameOpcode == WebSocketOpcode.CloseConnection:
            connection.closeType = 'AfterSend'
            return True
        if frameOpcode == WebSocketOpcode.Ping:
            self._SendWebSocketFrame(connection, WebSocketOpcode.Pong, framePayload)
            self._CallApi('HandleWebSocketPing', framePayload=framePayload, connection=connection, server=self)
            return True
        if frameOpcode == WebSocketOpcode.Pong:
            self._CallApi('HandleWebSocketPong', framePayload=framePayload, connection=connection, server=self)
            return True
        if frameOpcode == WebSocketOpcode.Text:
            message = framePayload.decode('utf-8')
            try:
                self._CallApi('HandleWebSocketMessage', message=message, connection=connection, server=self)
            except Exception as e:
                log.exception('caught exception while handling websocket message %r: %s', message, e)
            return True
        return True

    def _TryReceiveWebSocketFrame(self, connection):
        frameOpcode = None
        framePayload = None

        bufferHead = connection.receiveBuffer.readView
        bufferConsumed = 0
        while True:
            if len(bufferHead) < 2:
                return None

            fin = bufferHead[0] & 0x80
            opcode = WebSocketOpcode(bufferHead[0] & 0x0F)
            masked = bufferHead[1] & 0x80
            length = bufferHead[1] & 0x7F
            bufferHead = bufferHead[2:]
            bufferConsumed += 2

            # get length
            if length == 0x7E:
                if len(bufferHead) < 2:
                    return None
                length, bufferHead = struct.unpack('>H', bufferHead[:2])[0], bufferHead[2:]
                bufferConsumed += 2
            elif length == 0x7F:
                if len(bufferHead) < 8:
                    return None
                length, bufferHead = struct.unpack('>Q', bufferHead[:8])[0], bufferHead[8:]
                bufferConsumed += 8

            # get mask
            mask = None
            if masked:
                if len(bufferHead) < 4:
                    return None
                mask, bufferHead = bufferHead[:4], bufferHead[4:]
                bufferConsumed += 4

            # need entire payload
            if len(bufferHead) < length:
                return None
            payload, bufferHead = bufferHead[:length], bufferHead[length:]
            bufferConsumed += length

            # unmask the payload
            if masked:
                unmasked = bytearray()
                for byte in payload:
                    byte ^= mask[len(unmasked) % 4]
                    unmasked.append(byte)
                payload = unmasked

            # for first frame
            if opcode != WebSocketOpcode.Continuation:
                frameOpcode = opcode
                framePayload = payload
            else:
                # for continuation frame
                framePayload.extend(payload)

            # for last frame
            if fin:
                connection.receiveBuffer.size -= bufferConsumed
                return frameOpcode, framePayload

    def _SendWebSocketFrame(self, connection, frameOpcode, framePayload=None):
        data = bytearray()
        data.append(0x80 | frameOpcode.value)
        length = len(framePayload)
        if length <= 125:
            data.append(length)
        elif 126 <= length <= 65535:
            data.append(0x7E)
            data.extend(struct.pack('>H', length))
        else:
            data.append(0x7F)
            data.extend(struct.pack('>Q', length))
        data.extend(framePayload)

        while connection.sendBuffer.size + len(data) > connection.sendBuffer.capacity:
            connection.sendBuffer.capacity *= 2

        connection.sendBuffer.writeView[: len(data)] = data
        connection.sendBuffer.size += len(data)

    def _HandleHttpRequest(self, connection, request):
        if request.method == 'GET' and request.headers.get('upgrade', '').lower() == 'websocket':
            response = self._CallApi('HandleWebSocketUpgrade', request=request, connection=connection, server=self)
            if response is None:
                content = request.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
                digest = hashlib.sha1(content.encode('utf-8')).digest()
                response = HttpResponse(request, 101, 'Switching Protocols')
                response.headers = {
                    'Sec-WebSocket-Accept': base64.b64encode(digest).strip().decode('utf-8'),
                    'Upgrade': 'websocket',
                    'Connection': 'upgrade',
                }
                connection.upgradedToWebSocket = True
                self._CallApi('HandleWebSocketUpgraded', request=request, connection=connection, server=self)
            return response

        return super(WebSocketServer, self)._HandleHttpRequest(connection, request)
