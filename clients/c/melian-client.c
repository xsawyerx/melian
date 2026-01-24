#include <stdio.h>
#include "client.h"

static void show_usage(const char* progname) {
  fprintf(stderr, "A test client for the Melian server\n");
  fprintf(stderr, "Usage with TCP socket: %s [-UCHq] [-v] [-h host] -p port\n", progname);
  fprintf(stderr, "Usage with UNIX socket: %s [-UCHq] [-v] -u unix_path\n", progname);
  fprintf(stderr, "Options:\n");
  fprintf(stderr, "  -U    query table1 by id\n");
  fprintf(stderr, "  -C    query table2 by id\n");
  fprintf(stderr, "  -H    query table2 by hostname\n");
  fprintf(stderr, "  -q    send QUIT message at the end\n");
  fprintf(stderr, "  -v    print verbose logging\n");
}

int main(int argc, char **argv) {
  Client* client = 0;
  do {
    client = client_build();
    if (!client) break;

    if (! client_configure(client, argc, argv)) {
      show_usage(argv[0]);
      break;
    }

    client_run(client);
  } while (0);
  if (client) client_destroy(client);
  return 0;
}
