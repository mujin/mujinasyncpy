# Mujin Async Networking Library

A python package that provides async implementation of common network server and client:

- `TcpServer` and `TcpClient` in `mujinasync.asynctcp`
- `HttpServer` and `HttpClient` in `mujinasync.asynchttp`
- `WebSocketServer` in `mujinasync.asyncwebsocket`

## Example

```python

# create one context to manage all socket activities
ctx = TcpContext()

# create as many server and client as you need
client = TcpClient(ctx, ('127.0.0.1', 23))

class HttpAPI:
    def HandleHttpRequest(self, connection, request):
        return HttpResponse(request, statusCode=200, statusText='OK')

server = HttpServer(ctx, ('127.0.0.1', 8000), HttpAPI())

# no thread is created
# spin once to process network events
while True:
    # wait and process network events
    # wait up to 0.05 seconds
    ctx.SpinOnce(0.05)
    # do other things
    pass
