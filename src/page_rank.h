#ifndef _PAGE_RANK_H
#define _PAGE_RANK_H

#include "mmap_array.h"
#include "link_stream.h"

/// @addtogroup PageRank
/// @{

typedef enum {
     page_rank_error_ok = 0,
     page_rank_error_memory,
     page_rank_error_internal,
     page_rank_error_precision
} PageRankError;

#define PAGE_RANK_DEFAULT_DAMPING 0.85    /**< Default @ref PageRank.damping */
#define PAGE_RANK_DEFAULT_MAX_LOOPS 100   /**< Default @ref PageRank.max_loops */
#define PAGE_RANK_DEFAULT_PRECISION 1e-4  /**< Default @ref PageRank.precision */
#define PAGE_RANK_DEFAULT_PERSIST 0       /**< Default @ref PageRank.persist */

typedef struct {
     MMapArray *out_degree;
     MMapArray *value1;
     MMapArray *value2;

     size_t n_pages;

     char *path_out_degree;
     char *path_pr;

     Error *error;

// Options
// -----------------------------------------------------------------------------
     /** Probability of making a random page jump: 1.0 - damping */
     float damping;
     /** External computed scores associated with the pages */
     MMapArray *scores;
     /** If greater than 0 stop computation even if precision was not achieved */
     size_t max_loops;
     /** Stop iteration when the the largest change in any page score is below
         this threshold */
     float precision;
     /** If true, do not delete files after deleting */
     int persist;

} PageRank;

PageRankError
page_rank_new(PageRank **pr, const char *path, size_t max_vertices);

PageRankError
page_rank_delete(PageRank *pr);

PageRankError
page_rank_set_n_pages(PageRank *pr, size_t n_pages);

PageRankError
page_rank_compute(PageRank *pr,
                  void *link_stream_state,
                  LinkStreamNextFunc *link_stream_next,
                  LinkStreamResetFunc *link_stream_reset);

PageRankError
page_rank_get(const PageRank *pr, size_t idx, float *score_old, float *score_new);

void
page_rank_set_persist(PageRank *pr, int value);

/// @}

#endif // _PAGE_RANK_H
