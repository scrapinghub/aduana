#ifndef __PAGE_RANK_SCORER_H__
#define __PAGE_RANK_SCORER_H__

#include "page_rank.h"
#include "page_db.h"
#include "scorer.h"
#include "util.h"

/// @addtogroup PageRankScorer
/// @{

typedef enum {
     page_rank_scorer_error_ok = 0,
     page_rank_scorer_error_memory,
     page_rank_scorer_error_internal,
     page_rank_scorer_error_precision
} PageRankScorerError;

#define PAGE_RANK_SCORER_USE_CONTENT_SCORES 0
#define PAGE_RANK_SCORER_PERSIST 0

typedef struct {
     PageRank *page_rank;
     PageDB *page_db;

     Error *error;
// Options
// -----------------------------------------------------------------------------
     int persist;
     int use_content_scores;
} PageRankScorer;

PageRankScorerError
page_rank_scorer_new(PageRankScorer **prs, PageDB *db);

int
page_rank_scorer_get(void *state, size_t idx, float *score_old, float *score_new);

int
page_rank_scorer_update(void *state);

PageRankScorerError
page_rank_scorer_delete(PageRankScorer *prs);

void
page_rank_scorer_setup(PageRankScorer *prs, Scorer *scorer);

void
page_rank_scorer_set_persist(PageRankScorer *prs, int value);

void
page_rank_scorer_set_use_content_scores(PageRankScorer *prs, int value);
/// @}

#endif // __PAGE_RANK_SCORER_H__
