#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "lmdb.h"

#include "page_db.h"
#include "scheduler.h"

PageRequest*
page_request_new(size_t n_urls) {
     PageRequest *req = malloc(sizeof(*req));
     if (!req)
          return 0;
     req->urls = calloc(n_urls, sizeof(*req->urls));
     req->n_urls = 0;

     return req;
}

void
page_request_delete(PageRequest *req) {
     if (req) {
          for (size_t i=0; i<req->n_urls; ++i)
               free(req->urls[i]);
          free(req->urls);
          free(req);
     }
}

int
page_request_add_url(PageRequest *req, const char *url) {
     if (!(req->urls[req->n_urls] = strdup(url)))
          return -1;
     req->n_urls++;
     return 0;
}
