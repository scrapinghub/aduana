#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include "freq_scheduler.h"

int
main(int argc, char **argv) {
     FILE *output;

     if (argc < 3) {
          fprintf(stderr, "Insufficient number of arguments\n");
          goto exit_help;
     } else if (argc > 4) {
          fprintf(stderr, "Too many arguments\n");
          goto exit_help;
     }

     PageDB *page_db;
     if (page_db_new(&page_db, argv[2]) != 0) {
          fprintf(stderr, "Error opening page database: ");
          fprintf(stderr, "%s", page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     page_db_set_persist(page_db, 1);

     if (argc == 4) {
          output = fopen(argv[3], "w");
          if (!output) {
               int errno_cp = errno;
               fprintf(stderr, "Could not open output file: ");
               fprintf(stderr, "%s", strerror(errno_cp));
               fprintf(stderr, "\n");
               return -1;
          }
     } else {
          output = stdout;
     }

     FreqScheduler *sch = 0;
     if (freq_scheduler_new(&sch, page_db, argv[1]) != 0) {
          fprintf(stderr, "Error opening scheduler database: ");
          fprintf(stderr, "%s", sch? sch->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     sch->persist = 1;

     if (freq_scheduler_dump(sch, output) != 0) {
          fprintf(stderr, "Error dumping database:");
          fprintf(stderr, "%s", sch->error->message);
          fprintf(stderr, "\n");
          return -1;
     }

     freq_scheduler_delete(sch);
     page_db_delete(page_db);

     if (output != stdout)
          fclose(output);

     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_scheduler_db  path_to_page_db [path_to_output]\n", argv[0]);
     fprintf(stderr, "    path_to_output: If no path to output specified will print to stdout\n");
     return -1;
}
