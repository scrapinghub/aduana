#ifndef _PAGE_RANK_H
#define _PAGE_RANK_H

#include "mmap_array.h"
#include "pagedb.h"


#define PAGE_RANK_MAX_ERROR_LENGTH 10000

typedef enum {
     page_rank_error_ok = 0,
     page_rank_error_memory,
     page_rank_error_internal
} PageRankError;

typedef struct {
     MMapArray *out_degree;
     MMapArray *value1;
     MMapArray *value2;

     size_t n_pages;   
     float damping;

     PageRankError error; /**< Last error */
     /** A message with a meaningful description of the error */
     char error_msg[PAGE_RANK_MAX_ERROR_LENGTH+1];

     /** This flag allow us to execute certain operations just the
	 first time Page Rank is computed */
     int initialized;
} PageRank;

PageRankError
page_rank_new(PageRank **pr, const char *path, size_t max_vertices, float damping);

PageRankError
page_rank_delete(PageRank *pr);

PageRankError
page_rank_set_n_pages(PageRank *pr, size_t n_pages);

PageRankError
page_rank_compute(PageRank *pr,
		  void *link_stream_state,
		  LinkStreamNextFunc *link_stream_next,
		  LinkStreamResetFunc *link_stream_reset);

#endif // _PAGE_RANK_H
