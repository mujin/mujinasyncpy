# Changelog

## 0.2.0 (2026-08-13)

### Features

- Double buffer `TcpBuffer`, so that data is no longer moved to the front of the buffer every time
  data is consumed off it. Data that arrives while the buffer still holds unconsumed data it has no
  room next to is staged in a second buffer, which is read once the first one has been read in
  full.
- Add `TcpBuffer.Find` and `TcpBuffer.PeekBytes`, and use them in the http and websocket parsers
  so that they no longer copy everything received on every parse attempt. `PeekBytes` reads across
  both buffers without joining them. `Find` only joins them when what it is looking for is not in
  the buffer being read, since an occurrence can straddle the boundary.

### Fixes

- Fix `size` not dropping data that is staged behind an empty read buffer, which made the buffer
  hold data that could never be consumed.

### Changes

- (BREAKING) `TcpConnection.sendBuffer` is now a `TcpSendBuffer`, whose `readView` only exposes the
  data of the buffer currently being sent, which can be less than `size`, since joining the two
  buffers would copy data that is already queued. Code that reads a send buffer has to keep reading
  until `size` reaches 0 rather than expecting `readView` to cover everything queued. `readView` of
  a `TcpBuffer` still covers all of the data it holds.
- (BREAKING) `capacity` now counts the data still to be read plus the room left for new data, so
  that `capacity` minus `size` is how many bytes `writeView` accepts. While data is staged,
  assigning `capacity` only doubles the staging buffer, so one assignment can leave `capacity`
  below what was asked for. Callers have to keep assigning until it is large enough, the way
  `while buffer.size + len(data) > buffer.capacity: buffer.capacity *= 2` already does.

## 0.1.3 (2026-02-04)

### Features

- Reduce verbosity of connection logging

## 0.1.2 (2025-06-02)

### Features

- Add support for non-blocking HTTP handlers.

## 0.1.1 (2025-03-27)

### Features

- Add support for tcp client to use SSL.

