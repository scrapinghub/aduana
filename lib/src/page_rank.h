#ifndef _PAGE_RANK_H
#define _PAGE_RANK_H

#include "mmap_array.h"
#include "link_stream.h"

/** @addtogroup PageRank
 * @{
 */

typedef enum {
     page_rank_error_ok = 0,   /**< No error */
     page_rank_error_memory,   /**< Error allocating memory */
     page_rank_error_internal, /**< Unexpected error */
     page_rank_error_precision /**< Could not achieve precision in maximum number of loops */
} PageRankError;

#define PAGE_RANK_DEFAULT_DAMPING 0.85    /**< Default @ref PageRank::damping */
#define PAGE_RANK_DEFAULT_MAX_LOOPS 100   /**< Default @ref PageRank::max_loops */
#define PAGE_RANK_DEFAULT_PRECISION 1e-4  /**< Default @ref PageRank::precision */
#define PAGE_RANK_DEFAULT_PERSIST 0       /**< Default @ref PageRank::persist */

/** Implementation of the PageRank algorithm.
 *
 * See for example [Wikipedia](http://en.wikipedia.org/wiki/PageRank).
 *
 * Additionally, it allows to merge the pure link based original algorithm with
 * page content scores.
 */
typedef struct {
     /** Number of outgoing links.
      *
      * If page content scores are used then this array is actually the
      * aggregated scores of all the outgoing links.
      **/
     MMapArray *out_degree;
     /** PageRank value, old iteration */
     MMapArray *value1;
     /** PageRank value, new iteration */
     MMapArray *value2;

     /** Number of pages */
     size_t n_pages;

     /** Path to the out degree mmap array file */
     char *path_out_degree;
     /** Path to page rank mmap array file */
     char *path_pr;

     /** Error status */
     Error *error;

// Options
// -----------------------------------------------------------------------------
     /** Probability of making a random page jump: 1.0 - damping */
     float damping;
     /** External computed scores associated with the pages */
     MMapArray *scores;
     /** Total score */
     float total_score;
     /** If greater than 0 stop computation even if precision was not achieved */
     size_t max_loops;
     /** Stop iteration when the the largest change in any page score is below
         this threshold */
     float precision;
     /** If true, do not delete files after deleting */
     int persist;

} PageRank;

/** Create a new structure.
 *
 * @param pr The new structure is returned here. NULL if memory error.
 * @param path Directory where all files will be stored.
 * @param max_vertices Initial hint of the number of pages.
 *
 * @return 0 if success, otherwise an error code.
 */
PageRankError
page_rank_new(PageRank **pr, const char *path, size_t max_vertices);

/** Free memory and close associated resources.
 *
 * Files will be deleted or not depending on the value of PageRank::persist.
 **/
PageRankError
page_rank_delete(PageRank *pr);

/** Reserve memory for the specified number of pages */
PageRankError
page_rank_set_n_pages(PageRank *pr, size_t n_pages);

/** Compute PageRank score for all pages.
 *
 * The algorithm makes random access of pages scores and sequential access of
 * the links.
 *
 * @param pr
 * @param link_stream_state For example @ref PageDBLinkStream
 * @param link_stream_next For example @ref page_db_link_stream_next
 * @param link_stream_reset For example @ref page_db_link_stream_reset
 *
 * @return 0 if success, otherwise an error code.
 **/
PageRankError
page_rank_compute(PageRank *pr,
                  void *link_stream_state,
                  LinkStreamNextFunc *link_stream_next,
                  LinkStreamResetFunc *link_stream_reset);

/** Get PageRank score associated to a given page.
 *
 * @param pr
 * @param idx Page index.
 * @param score_old Score on the previous call to @ref page_rank_compute.
 * @param score_new Score on the last call to @ref page_rank_compute.
 *
 * @return 0 if success, otherwise an error code.
 **/
PageRankError
page_rank_get(const PageRank *pr, size_t idx, float *score_old, float *score_new);

/** Set value of @ref PageRank::persist */
void
page_rank_set_persist(PageRank *pr, int value);

/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_page_rank_suite(void);
#endif
#endif // _PAGE_RANK_H
