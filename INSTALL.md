# Installing Melian

This is a guide for installing Melian.

## Building

### Dependencies

* GCC / Clang (C99 or newer)
* `libevent` ≥ 2.1
* `libmysqlclient` (optional, for MySQL/MariaDB support)
* `libpq` (optional, for PostgreSQL support)
* `libjansson` (for client JSON parsing)
* `sqlite3` (optional, for SQLite support)
* `xxhash`
* POSIX environment (Linux or macOS)

### Configure & Build

```bash
# Configure; pass --with-* flags if headers/libs are installed outside default paths
# or configure to use either pkg-config or CPPFLAGS/LDFLAGS - see below
./configure --with-mysql=/opt/homebrew/opt/mysql-client \
            --with-postgresql=/opt/homebrew/opt/libpq \
            --with-sqlite3=/opt/homebrew/opt/sqlite \
            --with-libevent=/opt/homebrew/opt/libevent \
            --with-jansson=/opt/homebrew/opt/jansson

# Build the server and client
make

# Optionally install into PREFIX (defaults to /usr/local)
make install
```

If you're using Homebrew, considering adding the following to your shell rc file:

```bash
export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig:/opt/homebrew/share/pkgconfig
export CPPFLAGS="-I/opt/homebrew/include"
export LDFLAGS="-L/opt/homebrew/lib"
```
