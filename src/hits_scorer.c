#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <string.h>

#include "hits.h"
#include "hits_scorer.h"
#include "page_db.h"
#include "util.h"

static void
hits_scorer_set_error(HitsScorer *hs, int code, const char *message) {
     error_set(&hs->error, code, message);
}

static void
hits_scorer_add_error(HitsScorer *hs, const char *message) {
     error_add(&hs->error, message);
}

HitsScorerError
hits_scorer_new(HitsScorer **hs, PageDB *db) {
     HitsScorer *p = *hs = malloc(sizeof(*p));
     if (!p)
          return hits_scorer_error_memory;

     error_init(&p->error);

     p->page_db = db;
     if (hits_new(&p->hits, db->path, 1000) != 0) {
          hits_scorer_set_error(p, hits_scorer_error_internal, __func__);
          hits_scorer_add_error(p, "initializing HITS");
          hits_scorer_add_error(p, p? p->error.message: "NULL");
          return p->error.code;
     }

     return 0;
}

int
hits_scorer_update(void *state) {
     HitsScorer *hs = (HitsScorer*)state;

     char *error1 = 0;
     char *error2 = 0;

     PageDBLinkStream *st = 0;
     if (page_db_link_stream_new(&st, hs->page_db) != 0) {
          error1 = "creating link stream";
          error2 = st? "unknown": "NULL";
          goto on_error;
     }

     if (hits_compute(hs->hits,
                           st,
                           page_db_link_stream_next,
                           page_db_link_stream_reset) != 0) {
          error1 = "computing HITS";
          error2 = hs->hits->error.message;
          goto on_error;
     }
     page_db_link_stream_delete(st);
     return 0;
on_error:
     page_db_link_stream_delete(st);

     hits_scorer_set_error(hs,  hits_scorer_error_internal, __func__);
     hits_scorer_add_error(hs, error1);
     hits_scorer_add_error(hs, error2);
     return hs->error.code;
}

int
hits_scorer_add(void *state, const PageInfo *page_info, float *score) {
     *score = 0.0;
     return 0;
}

int
hits_scorer_get(void *state, size_t idx, float *score_old, float *score_new) {
     HitsScorer *hs = (HitsScorer*)state;
     return hits_get_authority(hs->hits, idx, score_old, score_new);
}

HitsScorerError
hits_scorer_delete(HitsScorer *hs) {
     if (hits_delete(hs->hits) != 0) {
          hits_scorer_set_error(hs,  hits_scorer_error_internal, __func__);
          hits_scorer_add_error(hs, "deleting HITS");
          hits_scorer_add_error(hs,
                                hs->hits? hs->hits->error.message
                                : "unknown error");
          return hs->error.code;
     }
     error_destroy(&hs->error);
     free(hs);
     return 0;
}

void
hits_scorer_setup(HitsScorer *hs, Scorer *scorer) {
     scorer->state = (void*)hs;
     scorer->add = hits_scorer_add;
     scorer->get = hits_scorer_get;
     scorer->update = hits_scorer_update;
}

#if (defined TEST) && TEST
#include "CuTest.h"

#endif // TEST
