#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include "page_db.h"

int
main(int argc, char **argv) {
     FILE *output;
     int links = 0;

     if (argc < 2) {
          fprintf(stderr, "Insufficient number of arguments\n");
          goto exit_help;
     } else if (argc > 4) {
          fprintf(stderr, "Too many arguments\n");
          goto exit_help;
     }

     if (strcmp(argv[1], "links") == 0)
          links = 1;
     else if (strcmp(argv[1], "info") == 0)
          links = 0;
     else {
          fprintf(stderr, "Could not understand dump mode: %s\n", argv[1]);
          fprintf(stderr, "Please specify 'links' or 'info'\n");
          goto exit_help;
     }

     if (argc == 4) {
          output = fopen(argv[3], "w");
          if (!output) {
               int errno_cp = errno;
               fprintf(stderr, "Could not open output file: ");
               fprintf(stderr, strerror(errno_cp));
               fprintf(stderr, "\n");
               return -1;
          }
     } else {
          output = stdout;
     }

     PageDB *page_db = 0;
     if (page_db_new(&page_db, argv[2]) != 0) {
          fprintf(stderr, "Error opening page database: ");
          fprintf(stderr, page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     page_db_set_persist(page_db, 1);

     int dump_error = 0;
     if (links && (page_db_links_dump(page_db, output) != 0))
          dump_error = 1;
     else if (page_db_info_dump(page_db, output) != 0)
          dump_error = 1;

     if (dump_error) {
          fprintf(stderr, "Error dumping database:");
          fprintf(stderr, page_db->error->message);
          fprintf(stderr, "\n");
          return -1;
     }

     page_db_delete(page_db);

     if (output != stdout)
          fclose(output);

     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_page_db mode [path_to_output]\n", argv[0]);
     fprintf(stderr, "    mode          : info | links\n");
     fprintf(stderr, "    path_to_output: If no path to output specified will print to stdout\n");
     return -1;
}
