#pragma once

// I/O backend abstraction for the server event loop.
// Supports libevent (default) and optional io_uring on Linux.

typedef enum IoBackend {
  IO_BACKEND_LIBEVENT = 0,
  IO_BACKEND_IOURING = 1,
} IoBackend;

// Detect the best available I/O backend for the platform.
// Returns IO_BACKEND_IOURING on Linux if runtime probing succeeds,
// otherwise IO_BACKEND_LIBEVENT.
IoBackend io_backend_detect(void);

// Return a human-readable name for the backend.
const char* io_backend_name(IoBackend backend);

// Parse backend from string (auto/libevent/iouring).
// Returns IO_BACKEND_LIBEVENT for unrecognized values.
IoBackend io_backend_parse(const char* value);
