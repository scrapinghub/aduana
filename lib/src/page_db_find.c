#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <regex.h>

#include "page_db.h"

int
main(int argc, char **argv) {
     if (argc != 3) {
          fprintf(stderr, "Incorrect number of arguments\n");
          goto exit_help;
     }

     regex_t r_url;
     int ret = regcomp(&r_url, argv[2], REG_NOSUB | REG_EXTENDED);
     if (ret != 0) {
          fprintf(stderr, "Error parsing regular expression: ");
          char error_msg[10000];
          regerror(ret, &r_url, error_msg, sizeof(error_msg));
          fprintf(stderr, "%s\n", error_msg);
          return -1;
     }

     PageDB *page_db = 0;
     if (page_db_new(&page_db, argv[1]) != 0) {
          fprintf(stderr, "Error opening page database: ");
          fprintf(stderr, "%s", page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     page_db_set_persist(page_db, 1);

     HashInfoStream *st;
     if (hashinfo_stream_new(&st, page_db) != 0) {
          fprintf(stderr, "Error creating stream inside database: ");
          fprintf(stderr, "%s", page_db? page_db->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     uint64_t hash;
     PageInfo *pi;
     while (hashinfo_stream_next(st, &hash, &pi) == stream_state_next) {
          if (regexec(&r_url, pi->url, 0, 0, 0) == 0)
               printf("%016"PRIx64" %s\n", hash, pi->url);
          page_info_delete(pi);
     }
     hashinfo_stream_delete(st);

     page_db_delete(page_db);
     regfree(&r_url);
     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_page_db url_regex\n", argv[0]);
     return -1;
}
