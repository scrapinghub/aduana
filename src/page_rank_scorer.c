#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <string.h>

#include "page_rank.h"
#include "page_rank_scorer.h"
#include "page_db.h"
#include "util.h"

static void
page_rank_scorer_set_error(PageRankScorer *prs, int code, const char *message) {
     error_set(&prs->error, code, message);
     
}

static void
page_rank_scorer_add_error(PageRankScorer *prs, const char *message) {
     error_add(&prs->error, message);
}

PageRankScorerError
page_rank_scorer_new(PageRankScorer **prs, PageDB *db) {
     PageRankScorer *p = *prs = malloc(sizeof(*p));
     if (!p)
          return page_rank_scorer_error_memory;
     page_rank_scorer_set_error(p, page_rank_scorer_error_ok, "NO ERROR");

     p->page_db = db;
     if (page_rank_new(&p->page_rank, db->path, 1000) != 0) {
          page_rank_scorer_set_error(p, page_rank_scorer_error_internal, __func__);
          page_rank_scorer_add_error(p, "initializing PageRank");
          page_rank_scorer_add_error(p, p? p->error.message: "NULL");
          return p->error.code;
     }

     return 0;
}

int
page_rank_scorer_update(void *state) {
     PageRankScorer *prs = (PageRankScorer*)state;

     char *error1 = 0;
     char *error2 = 0;

     PageDBLinkStream *st = 0;
     if (page_db_link_stream_new(&st, prs->page_db) != 0) {
          error1 = "creating link stream";
          error2 = st? "unknown": "NULL";
          goto on_error;
     }
 
     if (page_rank_compute(prs->page_rank, 
                           st, 
                           page_db_link_stream_next, 
                           page_db_link_stream_reset) != 0) {
          error1 = "computing PageRank";
          error2 = prs->page_rank->error.message;
          goto on_error;
     }

     return 0;
on_error:
     page_db_link_stream_delete(st);

     page_rank_scorer_set_error(prs,  page_rank_scorer_error_internal, __func__);
     page_rank_scorer_add_error(prs, error1);
     page_rank_scorer_add_error(prs, error2);

     return prs->error.code;
}

int
page_rank_scorer_add(void *state, const PageInfo *page_info, float *score) {
     *score = 0.0;
     return 0;
}

int
page_rank_scorer_get(void *state, size_t idx, float *score_old, float *score_new) {
     PageRankScorer *prs = (PageRankScorer*)state;
     return page_rank_get(prs->page_rank, idx, score_old, score_new);
}

PageRankScorerError
page_rank_scorer_delete(PageRankScorer *prs) {
     if (page_rank_delete(prs->page_rank) != 0) {
          page_rank_scorer_set_error(prs,  page_rank_scorer_error_internal, __func__);
          page_rank_scorer_add_error(prs, "deleting PageRank");
          page_rank_scorer_add_error(prs, 
                                     prs->page_rank? 
                                     prs->page_rank->error.message
                                     : "unknown error");
     }
     return prs->error.code;
}

void
page_rank_scorer_setup(PageRankScorer *prs, Scorer *scorer) {
     scorer->state = (void*)prs;
     scorer->add = page_rank_scorer_add;
     scorer->get = page_rank_scorer_get;
     scorer->update = page_rank_scorer_update;
}

#if (defined TEST) && TEST
#include "CuTest.h"

#endif // TEST
