#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>

#include "page_db.h"

struct _LinkList {
     int64_t idx;
     struct _LinkList *next;
};
typedef struct _LinkList LinkList;

LinkList *
link_list_cons(LinkList *next, int64_t idx) {
     LinkList *ll = malloc(sizeof(*ll));
     if (!ll)
          return 0;

     ll->idx = idx;
     ll->next = next;

     return ll;
}

void
link_list_delete(LinkList *ll) {
     LinkList *next = ll;
     while (next) {
          LinkList *n = next->next;
          free(next);
          next = n;
     }
}

int
link_list_find(LinkList *ll, int64_t idx) {
     int i=0;
     for (LinkList *c = ll; c != 0; c = c->next) {
          if (c->idx == idx)
               return i;
          ++i;
     }
     return -1;
}

int
print_line(PageDB *page_db, uint64_t hash) {
     PageInfo *pi = 0;
     if (page_db_get_info(page_db, hash, &pi) != 0) {
          fprintf(stderr, "%s\n", page_db->error->message);
          return -1;
     }
     char *url = pi? pi->url: "UNKNOWN";
     printf("    %016"PRIx64" %s\n", hash, url);
     page_info_delete(pi);
     return 0;
}

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

     uint64_t idx;
     if (page_db_get_idx(page_db, hash, &idx) != 0) {
          fprintf(stderr, "could not find hash inside database\n");
          return -1;
     }

     PageDBLinkStream *lst = 0;
     if (page_db_link_stream_new(&lst, page_db) != 0) {
          fprintf(stderr, "creating link stream: ");
          fprintf(stderr, "%s\n", page_db->error->message);
          return -1;
     }
     lst->only_diff_domain = 0;

     Link link;
     LinkList *blinks = 0;
     LinkList *flinks = 0;
     int blinks_len = 0;
     int flinks_len = 0;
     while (page_db_link_stream_next(lst, &link) == stream_state_next) {
          if ((uint64_t)link.from == idx) {
               flinks = link_list_cons(flinks, link.to);
               ++flinks_len;
          }
          if ((uint64_t)link.to == idx) {
               blinks = link_list_cons(blinks, link.from);
               ++blinks_len;
          }
     }
     page_db_link_stream_delete(lst);

     uint64_t *hash_blinks = calloc(blinks_len, sizeof(*hash_blinks));
     uint64_t *hash_flinks = calloc(flinks_len, sizeof(*hash_flinks));
     HashIdxStream *hist;
     if (hashidx_stream_new(&hist, page_db) != 0) {
          fprintf(stderr, "creating hash->idx stream: ");
          fprintf(stderr, "%s\n", page_db->error->message);
          return -1;
     }
     uint64_t hash2;
     size_t idx2;
     while (hashidx_stream_next(hist, &hash2, &idx2) == stream_state_next) {
          int i = link_list_find(blinks, idx2);
          if (i != -1)
               hash_blinks[i] = hash2;

          i = link_list_find(flinks, idx2);
          if (i != -1)
               hash_flinks[i] = hash2;
     }
     hashidx_stream_delete(hist);

     printf("->%016"PRIx64"\n", hash);
     for (int i=0; i<blinks_len; ++i)
          if (print_line(page_db, hash_blinks[i]) != 0)
               return -1;

     printf("%016"PRIx64"->\n", hash);
     for (int i=0; i<flinks_len; ++i)
          if (print_line(page_db, hash_flinks[i]) != 0)
               return -1;

     page_db_delete(page_db);

     return 0;

exit_help:
     fprintf(stderr, "Use: %s path_to_page_db url_hash\n", argv[0]);
     return -1;
}
