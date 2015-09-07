#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include "bf_scheduler.h"

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

     BFScheduler *sch = 0;
     if (bf_scheduler_new(&sch, page_db, argv[2]) != 0) {
          fprintf(stderr, "Error opening BFS scheduler database: ");
          fprintf(stderr, "%s", sch? sch->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     bf_scheduler_set_persist(sch, 1);

     if (bf_scheduler_reload(sch) != 0) {
	  fprintf(stderr, "Error reloading: ");
          fprintf(stderr, "%s", sch? sch->error->message: "NULL");
          fprintf(stderr, "\n");
          return -1;
     }
     else {
	  printf("Done\n");
     }
     page_db_delete(page_db);
     bf_scheduler_delete(sch);

     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_page_db path_to_bfs_db\n", argv[0]);
     return -1;
}
