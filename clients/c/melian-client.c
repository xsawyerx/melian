#include <stdio.h>
#include "client.h"

static void show_usage(const char* progname) {
  fprintf(stderr, "A test client for the Melian server\n\n");
  fprintf(stderr, "Usage: %s [options] [subcommand]\n\n", progname);
  fprintf(stderr, "Connection options:\n");
  fprintf(stderr, "  -h host    Server host (default: 127.0.0.1)\n");
  fprintf(stderr, "  -p port    Server port (TCP mode)\n");
  fprintf(stderr, "  -u path    UNIX socket path (default: /tmp/melian.sock)\n");
  fprintf(stderr, "  -v         Verbose logging\n\n");
  fprintf(stderr, "Subcommands:\n");
  fprintf(stderr, "  fetch      Fetch a single row\n");
  fprintf(stderr, "  schema     Show server schema\n");
  fprintf(stderr, "  stats      Show server statistics\n\n");
  fprintf(stderr, "Fetch options:\n");
  fprintf(stderr, "  --table NAME       Table by name\n");
  fprintf(stderr, "  --table-id ID      Table by numeric ID\n");
  fprintf(stderr, "  --index NAME       Index by column name\n");
  fprintf(stderr, "  --index-id ID      Index by numeric ID\n");
  fprintf(stderr, "  --key VALUE        Key to look up\n\n");
  fprintf(stderr, "Benchmark mode (no subcommand):\n");
  fprintf(stderr, "  -U         Benchmark table1 by id\n");
  fprintf(stderr, "  -C         Benchmark table2 by id\n");
  fprintf(stderr, "  -H         Benchmark table2 by hostname\n");
  fprintf(stderr, "  -s         Print server statistics\n");
  fprintf(stderr, "  -q         Send QUIT to server after benchmarks\n\n");
  fprintf(stderr, "Examples:\n");
  fprintf(stderr, "  %s -u /tmp/melian.sock fetch --table table1 --index id --key 42\n", progname);
  fprintf(stderr, "  %s -u /tmp/melian.sock fetch --table-id 1 --index hostname --key host-00002\n", progname);
  fprintf(stderr, "  %s -u /tmp/melian.sock schema\n", progname);
  fprintf(stderr, "  %s -u /tmp/melian.sock stats\n", progname);
  fprintf(stderr, "  %s -u /tmp/melian.sock -UCH\n", progname);
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
