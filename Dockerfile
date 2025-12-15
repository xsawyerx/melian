# syntax=docker/dockerfile:1.6

# Usage instructions:
# Build (server-only): docker build -t melian:latest .
# Build with bundled clients: docker build --build-arg WITH_CLIENTS=true -t melian:clients .
#
# Run with defaults (SQLite + UNIX socket):
# docker run --rm -p 42123:42123 -v /tmp:/tmp melian:latest
#
# Override env vars to point at MySQL/Postgres (running elsewhere):
# docker run --rm -p 42123:42123 -e MELIAN_DB_DRIVER=mysql -e MELIAN_DB_HOST=dbhost ... melian:latest
#
# Access the UNIX socket from the host by sharing /tmp or another path:
# docker run --rm -v /host/tmp:/tmp melian:latest
#
# Clients are under /app/clients/... inside the container; use docker exec -it <id> bash to experiment.
#
# Clients are disabled by default, to build with clients:
# docker build --build-arg WITH_CLIENTS=true -t melian:clients .


# Build/dev image
FROM debian:bookworm-slim AS build
ARG WITH_CLIENTS=false

WORKDIR /app

# For the server
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      autoconf automake build-essential pkg-config \
      libevent-dev libjansson-dev sqlite3 libsqlite3-dev

# For the clients (optional)
RUN if [ "$WITH_CLIENTS" = "true" ]; then \
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        cpanminus python3 python3-pip nodejs npm php-cli composer git; \
    fi

RUN rm -rf /var/lib/apt/lists/*

COPY . /app

# Ensure optional directories exist even if clients are skipped
RUN mkdir -p /opt/perl5 /opt/python \
    clients/nodejs/node_modules clients/php/vendor

# For the clients
RUN if [ "$WITH_CLIENTS" = "true" ]; then \
      git submodule update --init --recursive; \
    fi

RUN ./bootstrap || true
RUN ./configure --with-sqlite3=/usr --with-libevent=/usr --with-jansson=/usr
RUN make -j"$(nproc)"

# Can also run this to cut the size down... 
# && strip melian-server melian-client

# Perl/Python/Node/PHP clients
RUN if [ "$WITH_CLIENTS" = "true" ]; then \
      (cd clients/perl && cpanm -l /opt/perl5 --quiet --notest --installdeps .) && \
      python3 -m pip install --break-system-packages --prefix=/opt/python -r clients/python/requirements.txt && \
      npm --prefix clients/nodejs ci --omit=dev && \
      (cd clients/php && composer install --no-dev --no-interaction --no-progress); \
    fi

RUN mkdir -p /data && \
    gunzip -c .github/workflows/assets/melian-sqlite.sql.gz | sqlite3 /data/melian.db

# Runtime image
FROM debian:bookworm-slim
ARG WITH_CLIENTS=false

ENV MELIAN_SOCKET_HOST=0.0.0.0 \
    MELIAN_SOCKET_PORT=42123 \
    MELIAN_SOCKET_PATH=/run/melian/melian.sock

# For the server
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libevent-2.1-7 libjansson4 libsqlite3-0 sqlite3 \
      ca-certificates curl netcat-openbsd

# For the clients (optional)
RUN if [ "$WITH_CLIENTS" = "true" ]; then \
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        perl python3-minimal php-cli nodejs; \
    fi

RUN rm -rf /var/lib/apt/lists/*

# For the clients
ENV PERL5LIB=/opt/perl5/lib/perl5 \
    PYTHONPATH=/opt/python/lib/python3/dist-packages:/opt/python/lib/python3/site-packages \
    NODE_PATH=/opt/melian/js/node_modules

WORKDIR /app

COPY --from=build /app/utils/config/melian.json /etc/melian.json
COPY --from=build /app/melian-server /usr/local/bin/melian-server
COPY --from=build /data /data

# Clients - use `tar` because COPY wouldn't impact the size
RUN if [ "$WITH_CLIENTS" = "true" ]; then \
      mkdir -p /opt/melian && \
      tar -C /app/clients/perl -cf - lib | tar -C /opt/melian -xf - && \
      tar -C /app/clients/python -cf - melian | tar -C /opt/melian -xf - && \
      tar -C /app/clients/nodejs -cf - node_modules src package.json package-lock.json | tar -C /opt/melian -xf - && \
      tar -C /app/clients/php -cf - src vendor | tar -C /opt/melian -xf - && \
      install /app/melian-client /usr/local/bin/melian-client; \
    else \
      rm -f /usr/local/bin/melian-client; \
    fi


EXPOSE 42123
VOLUME ["/run/melian"]

HEALTHCHECK CMD nc -z localhost 42123 || exit 1

ENTRYPOINT ["melian-server","-c","/etc/melian.json"]
