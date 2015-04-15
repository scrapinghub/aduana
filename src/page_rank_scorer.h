#ifndef __PAGE_RANK_SCORER_H__
#define __PAGE_RANK_SCORER_H__

#include "page_rank.h"
#include "page_db.h"
#include "util.h"

#define PAGE_RANK_SCORER_MAX_ERROR_LENGTH 10000
typedef enum {
     page_rank_scorer_error_ok = 0,
     page_rank_scorer_error_memory,
     page_rank_scorer_error_internal,
     page_rank_scorer_error_precision
} PageRankScorerError;

typedef struct {
     PageRank *page_rank;
     PageDB *page_db;

     Error error;
} PageRankScorer;

PageRankScorerError
page_rank_scorer_new(PageRankScorer **prs, PageDB *db);

PageRankScorerError
page_rank_update(PageRankScorer *prs);

PageRankScorerError
page_rank_scorer_delete(PageRankScorer *prs);

#endif // __PAGE_RANK_SCORER_H__
