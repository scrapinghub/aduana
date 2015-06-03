#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>

#include "page_db.h"

int
main(int argc, char **argv) {
     if (argc != 3) {
          fprintf(stderr, "Incorrect number of arguments\n");
          goto exit_help;
     }

     PageDB *page_db = 0;
     if (page_db_new(&page_db, argv[1]) != 0) {
          fprintf(stderr, "Error opening page database: ");
          fprintf(stderr, "%s", page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     page_db_set_persist(page_db, 1);

     uint64_t hash;
     if (sscanf(argv[2], "%"PRIx64, &hash) != 1) {
          fprintf(stderr, "hash could not be parsed as hexadecimal 64 bit unsigned\n");
          return -1;
     }

     PageInfo *pi;
     while (hash != 0) {
          if (page_db_get_info(page_db, hash, &pi) != 0) {
               fprintf(stderr, "while looking for hash %016"PRIx64": ", hash);
               fprintf(stderr, "%s\n", page_db->error->message);
               return -1;
          }
          if (!pi) {
               fprintf(stderr, "backlink does not exist: %016"PRIx64": ", hash);
               return -1;
          }
          printf("%016"PRIx64" %s\n", hash, pi->url);
          hash = pi->linked_from;
     };

     page_db_delete(page_db);
     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_page_db url_hash\n", argv[0]);
     return -1;
}
