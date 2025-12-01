# Installing Melian

This is a guide for installing Melian.

## Building

### Dependencies

* GCC / Clang (C99 or newer)
* `libevent` ≥ 2.1
* `libmysqlclient`
* `libjansson` (for client JSON parsing)
* `xxhash`
* POSIX environment (Linux or macOS)

### Configure & Build

```bash
# Generate the configure script (only once, or whenever autotools files change)
./bootstrap

# Configure; pass --with-* flags if headers/libs are installed outside default paths
./configure --with-mysql=/opt/homebrew/opt/mysql-client \
            --with-libevent=/opt/homebrew/opt/libevent \
            --with-jansson=/opt/homebrew/opt/jansson

# Build the server and client
make

# Optionally install into PREFIX (defaults to /usr/local)
make install
```

If `configure` cannot locate the MySQL/MariaDB client, libevent, or libjansson headers or libraries it exits with a clear error; install the client libraries or point the `--with-*` flags at their prefix paths.
