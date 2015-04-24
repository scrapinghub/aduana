#ifndef __PAGE_RANK_SCORER_H__
#define __PAGE_RANK_SCORER_H__

#include "page_rank.h"
#include "page_db.h"
#include "scorer.h"
#include "util.h"

typedef enum {
     page_rank_scorer_error_ok = 0,
     page_rank_scorer_error_memory,
     page_rank_scorer_error_internal,
     page_rank_scorer_error_precision
} PageRankScorerError;

typedef struct {
     PageRank *page_rank;
     PageDB *page_db;

     Error *error;
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

#endif // __PAGE_RANK_SCORER_H__
