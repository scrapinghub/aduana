#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include "page_db.h"

int
main(int argc, char **argv) {
     FILE *output;
     switch (argc) {
     case 2:
          output = stdout;
          break;
     case 3:
          output = fopen(argv[2], "w");
          if (!output) {
               int errno_cp = errno;
               fprintf(stderr, "Could not open output file: ");
               fprintf(stderr, strerror(errno_cp));
               fprintf(stderr, "\n");
               return -1;
          }
          break;
     default:
          goto exit_help;
     }
     
     PageDB *page_db = 0;
     if (page_db_new(&page_db, argv[1]) != 0) {
          fprintf(stderr, "Error opening page database: ");
          fprintf(stderr, page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }

     if (page_db_info_dump(page_db, output) != 0) {
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
     fprintf(stderr, "Use: %s path_to_page_db [path_to_output]\n", argv[0]);
     fprintf(stderr, "If no path to output specified will print to stdout\n");
     return -1;
}
