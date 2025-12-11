#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "config.h"
#include "data.h"
#include "server.h"

static void show_usage(const char* prog) {
  printf("%s -- a cache for MySQL tables\n", prog);
  printf("\n");
  printf("The program reads full tables from MySQL, stores them in memory,\n");
  printf("and serves the data over a UNIX socket based on a key value.\n");
  printf("\nOptions:\n");
  printf("  -c, --configfile <path>  Use the specified JSON config file instead of autodetecting.\n");
  printf("  -h, --help               Show this help message.\n");
  printf("\nPriority order for config files:\n");
  printf("  1. Command line -c/--configfile\n");
  printf("  2. Environment variable MELIAN_CONFIG_FILE\n");
  printf("  3. Default path %s\n", MELIAN_DEFAULT_CONFIG_FILE);

  data_show_usage();
  config_show_usage();
}

int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  const char* cli_config_path = NULL;
  int opt = 0;
  static const struct option long_opts[] = {
    {"configfile", required_argument, NULL, 'c'},
    {"help", no_argument, NULL, 'h'},
    {0, 0, 0, 0},
  };
  while ((opt = getopt_long(argc, argv, "c:h", long_opts, NULL)) != -1) {
    switch (opt) {
      case 'c':
        cli_config_path = optarg;
        break;
      case 'h':
        show_usage(argv[0]);
        return 0;
      default:
        show_usage(argv[0]);
        return 1;
    }
  }

  const char* env_config_path = getenv("MELIAN_CONFIG_FILE");
  const char* final_config_path = cli_config_path && cli_config_path[0]
                                  ? cli_config_path
                                  : (env_config_path && env_config_path[0]
                                     ? env_config_path
                                     : MELIAN_DEFAULT_CONFIG_FILE);
  ConfigFileSource source = CONFIG_FILE_SOURCE_DEFAULT;
  if (cli_config_path && cli_config_path[0]) {
    source = CONFIG_FILE_SOURCE_CLI;
  } else if (env_config_path && env_config_path[0]) {
    source = CONFIG_FILE_SOURCE_ENV;
  }
  config_set_config_file_path(final_config_path, source);

  Server* server = 0;
  do {
    server = server_build();
    if (!server) break;

    if (!server_initial_load(server)) break;
    if (!server_listen(server)) break;
    if (!server_run(server)) break;
  } while (0);
  if (server) {
    server_stop(server);
    server_destroy(server);
  }
  return 0;
}
