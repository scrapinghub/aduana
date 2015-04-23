#ifndef _HITS_H
#define _HITS_H

#include "mmap_array.h"
#include "link_stream.h"

#define HITS_MAX_ERROR_LENGTH 10000

typedef enum {
     hits_error_ok = 0,
     hits_error_memory,
     hits_error_internal,
     hits_error_precision
} HitsError;

#define HITS_DEFAULT_MAX_LOOPS 100   /**< Default @ref Hits.max_loops */
#define HITS_DEFAULT_PRECISION 1e-4  /**< Default @ref Hits.precision */
#define HITS_DEFAULT_PERSIST 0       /**< Default @ref Hits.persist */

typedef struct {
     MMapArray *h1;
     MMapArray *h2;
     MMapArray *a1;
     MMapArray *a2;

     char *path_h1;
     char *path_h2;

     size_t n_pages;

     Error error;

// Options
// -----------------------------------------------------------------------------
     /** If greater than 0 stop computation even if precision was not achieved */
     size_t max_loops;
     /** Stop iteration when the the largest change in any page score is below
         this threshold */
     float precision;
     /** If true, do not delete files after deleting object*/
     int persist;
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

HitsError
hits_get_hub(const Hits *pr,
             size_t idx,
             float *score_old,
             float *score_new);

HitsError
hits_get_authority(const Hits *pr,
                   size_t idx,
                   float *score_old,
                   float *score_new);

#endif // _HITS_H
