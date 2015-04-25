#ifndef __HITS_SCORER_H__
#define __HITS_SCORER_H__

#include "hits.h"
#include "page_db.h"
#include "scorer.h"
#include "util.h"

/// @addtogroup HitsScorer
/// @{

typedef enum {
     hits_scorer_error_ok = 0,
     hits_scorer_error_memory,
     hits_scorer_error_internal,
     hits_scorer_error_precision
} HitsScorerError;

typedef struct {
     Hits *hits;
     PageDB *page_db;

     Error *error;
} HitsScorer;

HitsScorerError
hits_scorer_new(HitsScorer **hs, PageDB *db);

int
hits_scorer_get(void *state, size_t idx, float *score_old, float *score_new);

int
hits_scorer_update(void *state);

HitsScorerError
hits_scorer_delete(HitsScorer *hs);

void
hits_scorer_setup(HitsScorer *hs, Scorer *scorer);

/// @}

#endif // __HITS_SCORER_H__
