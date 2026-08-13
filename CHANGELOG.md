# Changelog

## 0.1.4 (2026-08-13)

### Features

- Double buffer `TcpBuffer`, so that data is no longer moved to the front of the buffer every time
  data is consumed off it. Data that arrives while the buffer still holds unconsumed data it has no
  room next to is staged in a second buffer, which is read once the first one has been read in
  full.
- Add `TcpBuffer.Find` and `TcpBuffer.PeekBytes`, which read across both buffers without joining
  them, and use them in the http and websocket parsers so that they no longer copy everything
  received on every parse attempt.
- Note that `readView` of a `TcpSendBuffer` only exposes the data of the buffer currently being
  sent, which can be less than `size`, since joining the two buffers would copy data that is
  already queued. `readView` of a `TcpBuffer` still covers all of the data it holds.

## 0.1.3 (2026-02-04)

### Features

- Reduce verbosity of connection logging

## 0.1.2 (2025-06-02)

### Features

- Add support for non-blocking HTTP handlers.

## 0.1.1 (2025-03-27)

### Features

- Add support for tcp client to use SSL.

