#ifndef __PAGE_RANK_SCORER_H__
#define __PAGE_RANK_SCORER_H__

#include "page_rank.h"
#include "page_db.h"
#include "scorer.h"
#include "util.h"

/** @addtogroup PageRankScorer
 *
 * Merge of @ref PageRank and @ref PageDB, for use inside an scheduler
 * (for example @ref BFScheduler).
 *
 * @{
 */

typedef enum {
     page_rank_scorer_error_ok = 0,    /**< No error */
     page_rank_scorer_error_memory,    /**< Error allocating memory */
     page_rank_scorer_error_internal,  /**< Unexpected error */
     page_rank_scorer_error_precision  /**< Could not achieve precision in maximum number of loops */
} PageRankScorerError;

/** Default value for @ref PageRankScorer::use_content_scores */
#define PAGE_RANK_SCORER_USE_CONTENT_SCORES 0

/** Default value for @ref PageRankScorer::persist */
#define PAGE_RANK_SCORER_PERSIST 0

typedef struct {
     /** Implementation of the PageRank algorithm */
     PageRank *page_rank;
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
} PageRankScorer;

/** Create new scorer */
PageRankScorerError
page_rank_scorer_new(PageRankScorer **prs, PageDB *db);

/** Add new page to scorer.
 *
 * Function signature complies with @ref Scorer::add
 */
int
page_rank_scorer_add(void *state, const PageInfo *page_info, float *score);

/** Access PageRank scorer as with @ref page_rank_get.
 *
 * Function signature complies with @ref Scorer::get
 */
int
page_rank_scorer_get(void *state, size_t idx, float *score_old, float *score_new);

/** Update scores.
 *
 * Function signature complies with @ref Scorer::update
 */
int
page_rank_scorer_update(void *state);

/** Given a @ref Scorer fill its fields with the necessary info */
void
page_rank_scorer_setup(PageRankScorer *prs, Scorer *scorer);

/** Delete scorer.
 *
 * Files will be deleted unles @ref PageRankScorer::persist is true
 */
PageRankScorerError
page_rank_scorer_delete(PageRankScorer *prs);

/** Sets @ref PageRankScorer::persist */
void
page_rank_scorer_set_persist(PageRankScorer *prs, int value);

/** Sets @ref PageRankScorer::use_content_scores */
void
page_rank_scorer_set_use_content_scores(PageRankScorer *prs, int value);

/** Sets @ref PageRankScorer::page_rank::damping */
void
page_rank_scorer_set_damping(PageRankScorer *prs, float value);
/// @}

#endif // __PAGE_RANK_SCORER_H__
