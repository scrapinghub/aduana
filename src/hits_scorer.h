#ifndef __HITS_SCORER_H__
#define __HITS_SCORER_H__

#include "hits.h"
#include "page_db.h"
#include "scorer.h"
#include "util.h"

/** @addtogroup HitsScorer
 *
 * Merge of @ref Hits and @ref PageDB, for use inside an scheduler
 * (for example @ref BFScheduler).
 * @{
 */

typedef enum {
     hits_scorer_error_ok = 0,   /**< No error */
     hits_scorer_error_memory,   /**< Error allocating memory */
     hits_scorer_error_internal, /**< Unexpected error */
     hits_scorer_error_precision /**< Could not achieve precision in maximum number of loops */
} HitsScorerError;

/** Default value for @ref HitsScorer::use_content_scores */
#define HITS_SCORER_USE_CONTENT_SCORES 0
/** Default value for @ref HitsScorer::persist */
#define HITS_SCORER_PERSIST 0

typedef struct {
     /** Implementation of the HITS algorithm */
     Hits *hits;
     /** Database with crawl information */
     PageDB *page_db;

     /** Error status */
     Error *error;
// Options
// -----------------------------------------------------------------------------
     /** If true files will not be removed by @ref page_rank_scorer_delete */
     int persist;
     /** If true use content scores inside @ref PageRank algorithm */
     int use_content_scores;
} HitsScorer;

/** Create new scorer */
HitsScorerError
hits_scorer_new(HitsScorer **hs, PageDB *db);

/** Add new page to scorer.
 *
 * Function signature complies with @ref Scorer::add
 */
int
hits_scorer_add(void *state, const PageInfo *page_info, float *score);

/** Access HITS scorer as with @ref hits_get_authority.
 *
 * Function signature complies with @ref Scorer::get
 */
int
hits_scorer_get(void *state, size_t idx, float *score_old, float *score_new);

/** Update scores.
 *
 * Function signature complies with @ref Scorer::update
 */
int
hits_scorer_update(void *state);

/** Given a @ref Scorer fill its fields with the necessary info */
void
hits_scorer_setup(HitsScorer *hs, Scorer *scorer);

/** Delete scorer.
 *
 * Files will be deleted unles @ref HitsScorer::persist is true
 */
HitsScorerError
hits_scorer_delete(HitsScorer *hs);

/** Sets @ref HitsScorer::persist */
void
hits_scorer_set_persist(HitsScorer *hs, int value);

/** Sets @ref HitsScorer::use_content_scores */
void
hits_scorer_set_use_content_scores(HitsScorer *hs, int value);
/// @}

#endif // __HITS_SCORER_H__
