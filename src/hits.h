#ifndef _HITS_H
#define _HITS_H

#include "mmap_array.h"
#include "pagedb.h"

#define HITS_MAX_ERROR_LENGTH 10000

typedef enum {
     hits_error_ok = 0,
     hits_error_memory,
     hits_error_internal
} HitsError;

typedef struct {
     MMapArray *h1;
     MMapArray *h2;
     MMapArray *a1;
     MMapArray *a2;

     size_t n_pages;   

     HitsError error; /**< Last error */
     /** A message with a meaningful description of the error */
     char error_msg[HITS_MAX_ERROR_LENGTH+1];
} Hits;

HitsError
hits_new(Hits **hits, const char *path, size_t max_vertices);

HitsError
hits_delete(Hits *hits);

HitsError
hits_set_n_pages(Hits *hits, size_t n_pages);

HitsError
hits_compute(Hits *hits, 
	     void *link_stream_state,
	     LinkStreamNextFunc *link_stream_next,
	     LinkStreamResetFunc *link_stream_reset);

#endif // _HITS_H
