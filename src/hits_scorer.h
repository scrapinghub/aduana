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

#define HITS_SCORER_USE_CONTENT_SCORES 0
#define HITS_SCORER_PERSIST 0

typedef struct {
     Hits *hits;
     PageDB *page_db;

     Error *error;
// Options
// -----------------------------------------------------------------------------
     int persist;
     int use_content_scores;
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

void
hits_scorer_set_persist(HitsScorer *hs, int value);

void
hits_scorer_set_use_content_scores(HitsScorer *hs, int value);
/// @}

#endif // __HITS_SCORER_H__
