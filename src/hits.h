#ifndef _HITS_H
#define _HITS_H

#include "mmap_array.h"
#include "link_stream.h"

/** @addtogroup Hits
 * @{
 */

typedef enum {
     hits_error_ok = 0,   /**< No error */
     hits_error_memory,   /**< Error allocating memory */
     hits_error_internal, /**< Unexpected error */
     hits_error_precision /**< Could not achieve precision in maximum number of loops */
} HitsError;

#define HITS_DEFAULT_MAX_LOOPS 100   /**< Default @ref Hits::max_loops */
#define HITS_DEFAULT_PRECISION 1e-4  /**< Default @ref Hits::precision */
#define HITS_DEFAULT_PERSIST 0       /**< Default @ref Hits::persist */

/** Implementation of the HITS algorithm.
 *
 * See for example [Wikipedia](http://en.wikipedia.org/wiki/HITS_algorithm).
 *
 * Additionally, it allows to merge the pure link based original algorithm with
 * page content scores. The idea is that the authority scores are distributed
 * back to the hub according to the content score. For example imagine that page
 * A links to B, C and D and the content/authority scores are:
 *
 * -B: 0.5 / 0.1
 * -C: 0.1 / 1.0
 * -D: 0.9 / 0.5
 *
 * Then the hub score of A would be computed as:
 *
 * Hub(A) = 0.5*0.1 + 0.1*1.0 + 0.9*0.5
 */
typedef struct {
     /** Hub score, previous iteration */
     MMapArray *h1;
     /** Hub score, current iteration */
     MMapArray *h2;
     /** Authority score, previous iteration */
     MMapArray *a1;
     /** Authority score, current iteration */
     MMapArray *a2;

     /** Path to mmap file of @ref Hits::h1 */
     char *path_h1;
     /** Path to mmap file of @ref Hits::h2 */
     char *path_h2;

     /** Number of pages */
     size_t n_pages;

     /** Error status */
     Error *error;

// Options
// -----------------------------------------------------------------------------
     /** External computed scores associated with the pages */
     MMapArray *scores;
     /** If greater than 0 stop computation even if precision was not achieved */
     size_t max_loops;
     /** Stop iteration when the the largest change in any page score is below
         this threshold */
     float precision;
     /** If true, do not delete files after deleting object*/
     int persist;
} Hits;

/** Create a new structure.
 *
 * @param pr The new structure is returned here. NULL if memory error.
 * @param path Directory where all files will be stored.
 * @param max_vertices Initial hint of the number of pages.
 *
 * @return 0 if success, otherwise an error code.
 */
HitsError
hits_new(Hits **hits, const char *path, size_t max_vertices);

/** Free memory and close associated resources.
 *
 * Files will be deleted or not depending on the value of Hits::persist.
 **/
HitsError
hits_delete(Hits *hits);

/** Reserve memory for the specified number of pages */
HitsError
hits_set_n_pages(Hits *hits, size_t n_pages);

/** Compute HITS score for all pages.
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
HitsError
hits_compute(Hits *hits,
             void *link_stream_state,
             LinkStreamNextFunc *link_stream_next,
             LinkStreamResetFunc *link_stream_reset);

/** Get hub score associated to a given page.
 *
 * @param pr
 * @param idx Page index.
 * @param score_old Score on the previous call to @ref hits_compute.
 * @param score_new Score on the last call to @ref hits_compute.
 *
 * @return 0 if success, otherwise an error code.
 **/
HitsError
hits_get_hub(const Hits *pr,
             size_t idx,
             float *score_old,
             float *score_new);

/** Get authority score associated to a given page.
 *
 * @param pr
 * @param idx Page index.
 * @param score_old Score on the previous call to @ref hits_compute.
 * @param score_new Score on the last call to @ref hits_compute.
 *
 * @return 0 if success, otherwise an error code.
 **/
HitsError
hits_get_authority(const Hits *pr,
                   size_t idx,
                   float *score_old,
                   float *score_new);

/** Set value of @ref Hits::persist */
void
hits_set_persist(Hits *hits, int value);
/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_hits_suite(void);
#endif

#endif // _HITS_H
