#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <assert.h>
#include <errno.h>
#include <malloc.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "lmdb.h"
#include "xxhash.h"

#include "page_db.h"
#include "util.h"
#include "scheduler.h"
#include "bf_scheduler.h"
#include "scorer.h"
#include "page_rank_scorer.h"

/** Structure used as key inside the schedule database.
 *
 * In the schedule we want to keep an ordered heap of hashes, where we can pop
 * the highest scores whenever we want. In this sense it would have been more
 * natural to use as keys the scores (a single float) and the values the hashes.
 * However, the scores may change as the page database changes and we want a
 * fast method to change the score associated to a hash. In particular we want
 * to make the change:
 *
 *   (score_old, hash) --> (score_new, hash)
 *
 * Since there can be lots of pages with the same score we move the hash into
 * the key, so that finding the previous (score_old, hash) can be done very fast */
typedef struct {
     float score;
     uint64_t hash;
} ScheduleKey;

/** Order keys from higher to lower
 *
 * First by score (descending) and then by hash.
 * */
static int
schedule_entry_mdb_cmp(const MDB_val *a, const MDB_val *b) {
     ScheduleKey *se_a = (ScheduleKey*)a->mv_data;
     ScheduleKey *se_b = (ScheduleKey*)b->mv_data;
     return
          se_a->score < se_b->score? -1:
          se_a->score > se_b->score? +1:
          // equal scores, order by hash
          se_a->hash  < se_b->hash? -1:
          se_a->hash  > se_b->hash? +1: 0;
}


static void
bf_scheduler_set_error(BFScheduler *sch, int code, const char *message) {
     error_set(&sch->error, code, message);
}

static void
bf_scheduler_clean_error(BFScheduler *sch) {
     error_clean(&sch->error);
}

static void
bf_scheduler_add_error(BFScheduler *sch, const char *message) {
     error_add(&sch->error, message);
}

/** Doubles database size.
 *
 * This function is automatically called when an operation cannot proceed because
 * of insufficient allocated mmap memory.
 */
static BFSchedulerError
bf_scheduler_grow(BFScheduler *sch) {
     MDB_envinfo info;
     int mdb_rc =
          mdb_env_info(sch->env, &info) ||
          mdb_env_set_mapsize(sch->env, info.me_mapsize*2);
     if (mdb_rc != 0) {
          bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
          bf_scheduler_add_error(sch, mdb_strerror(mdb_rc));
     }
     return sch->error.code;
}

BFSchedulerError
bf_scheduler_new(BFScheduler **sch, PageDB *db) {
     BFScheduler *p = *sch = malloc(sizeof(*p));
     if (!p)
          return bf_scheduler_error_memory;
     bf_scheduler_set_error(p, bf_scheduler_error_ok, "NO ERROR");

     p->page_db = db;
     p->persist = BF_SCHEDULER_DEFAULT_PERSIST;

     char *error = 0;
     if (!(p->path = concat(db->path, "bfs", '_')))
          error = "building scheduler path";
     else
          error = make_dir(p->path);

     if (error != 0) {
          bf_scheduler_set_error(p, bf_scheduler_error_invalid_path, __func__);
          bf_scheduler_add_error(p, error);
          return p->error.code;
     }

     // initialize LMDB on the directory
     int mdb_rc = 0;
     if ((mdb_rc = mdb_env_create(&p->env) != 0))
          error = "creating environment";
     else if ((mdb_rc = mdb_env_set_mapsize(p->env, BF_SCHEDULER_DEFAULT_SIZE)) != 0)
          error = "setting map size";
     else if ((mdb_rc = mdb_env_set_maxdbs(p->env, 1)) != 0)
          error = "setting number of databases";
     else if ((mdb_rc = mdb_env_open(
                    p->env,
                    p->path,
                    MDB_NOTLS | MDB_WRITEMAP | MDB_MAPASYNC, 0664) != 0))
          error = "opening environment";

     if (error != 0) {
          bf_scheduler_set_error(p, bf_scheduler_error_internal, __func__);
          bf_scheduler_add_error(p, error);
          bf_scheduler_add_error(p, mdb_strerror(mdb_rc));
          return p->error.code;
     }

     return 0;
}

static int
bf_scheduler_open_cursor(MDB_txn *txn, MDB_cursor **cursor) {
     MDB_dbi dbi;
     int mdb_rc =
          mdb_dbi_open(txn, "schedule", MDB_CREATE, &dbi) ||
          mdb_set_compare(txn, dbi, schedule_entry_mdb_cmp) ||
          mdb_cursor_open(txn, dbi, cursor);

     if (mdb_rc != 0)
          *cursor = 0;

     return mdb_rc;
}

BFSchedulerError
bf_scheduler_add(BFScheduler *sch, const CrawledPage *page) {
     char *error1 = 0;
     char *error2 = 0;

     PageInfoList *pil;
     if (page_db_add(sch->page_db, page, &pil) != 0) {
          error1 = "adding crawled page";
          error2 = sch->page_db->error.message;
          goto on_error;
     }

     int failed = 0;
     MDB_txn *txn;
     MDB_cursor *cur;

txn_start:
     txn = 0;
     cur = 0;
     int mdb_rc =
          mdb_txn_begin(sch->env, 0, 0, &txn) ||
          bf_scheduler_open_cursor(txn, &cur);

     if (mdb_rc != 0) {
          error1 = "starting transaction/cursor";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }

     for (PageInfoList *node = pil; node != 0; node=node->next) {
          PageInfo *pi = node->page_info;
          if (pi->n_crawls == 0) {
               ScheduleKey se = {
                    .score = 0.0,
                    .hash = node->hash
               };
               if (sch->scorer.state)
                    sch->scorer.add(sch->scorer.state, pi, &se.score);
               else
                    se.score = -pi->score;
               MDB_val key = {
                    .mv_size = sizeof(se),
                    .mv_data = &se
               };
               MDB_val val = {
                    .mv_size = 0,
                    .mv_data = 0
               };
               if ((mdb_rc = mdb_cursor_put(cur, &key, &val, 0)) != 0) {
                    error1 = "adding page to schedule";
                    error2 = mdb_strerror(mdb_rc);
                    goto on_error;
               }
          }
     }
     page_info_list_delete(pil);

     mdb_cursor_close(cur);
     cur = 0;

     if ((mdb_rc = mdb_txn_commit(txn)) != 0) {
          error1 = "commiting schedule transaction";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }

     return 0;

on_error:
     if (cur != 0)
          mdb_cursor_close(cur);
     if (txn != 0)
          mdb_txn_abort(txn);
     page_info_list_delete(pil);

     if (!failed) {
          failed = 1;
          bf_scheduler_clean_error(sch);
          if (bf_scheduler_grow(sch) == 0)
               goto txn_start;
     }

     bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
     bf_scheduler_add_error(sch, error1);
     bf_scheduler_add_error(sch, error2);
     return sch->error.code;
}

static BFSchedulerError
bf_scheduler_change_score(BFScheduler *sch,
                          MDB_cursor *cur,
                          uint64_t hash,
                          float score_old,
                          float score_new) {
     char *error1 = 0;
     char *error2 = 0;
     // Delete old key
     ScheduleKey se = { .score = score_old, .hash = hash };
     MDB_val key = {.mv_size = sizeof(se), .mv_data = &se};
     MDB_val val = {0, 0};
     int mdb_rc;
     switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_SET)) {
     case 0:
          // Delete old key
          if ((mdb_rc = mdb_cursor_del(cur, 0)) != 0) {
               error1 = "deleting Hash/Idx item";
               error2 = mdb_strerror(mdb_rc);
               goto on_error;
          }
          break;
     case MDB_NOTFOUND:
          // OK, do nothing
          break;
     default:
          error1 = "trying to retrieve Hash/Index item";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
          break;
     }
     // Add new key
     se.score = score_new;
     if ((mdb_rc = mdb_cursor_put(cur, &key, &val, 0)) != 0) {
          error1 = "adding updated Hash/Index item";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }
     return 0;

on_error:
     bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
     bf_scheduler_add_error(sch, error1);
     bf_scheduler_add_error(sch, error2);
     return sch->error.code;
}


static BFSchedulerError
bf_scheduler_update_batch(BFScheduler *sch) {
     assert(sch->scorer.state != 0);

     int retry = 1;

     char *error1;
     char *error2;
     MDB_txn *txn;
     MDB_cursor *cur;

txn_start:
     error1 = error2 = 0;
     txn = 0;
     cur = 0;

     // if no Hash/Idx stream active create a new one. It will be deleted if:
     //     1. We reach stream_state_end or
     //     2. An error is produced
     if (!sch->stream &&
         (hashidx_stream_new(&sch->stream, sch->page_db) != 0)) {
               error1 = "creating Hash/Index stream";
               error2 = sch->page_db->error.message;
               goto on_error;
     }

     // create new read/write transaction and cursor inside the schedule
     int mdb_rc = mdb_txn_begin(sch->env, 0, 0, &txn) ||
          bf_scheduler_open_cursor(txn, &cur);

     // we make the update in batches because there can be only one simultaneous
     // write transaction, and we need another write transaction to get requests, which
     // actually has higher priority
     for (size_t i=0; i<BF_SCHEDULER_UPDATE_BATCH_SIZE && sch->stream; ++i) {

          uint64_t hash;
          size_t idx;
          float score_old;
          float score_new;
          switch (hashidx_stream_next(sch->stream, &hash, &idx)) {
          case stream_state_next:
               sch->scorer.get(sch->scorer.state, idx, &score_old, &score_new);
               // to gain some performance we don't bother to change the schedule unless
               // there is some significant score change
               if (fabs(score_old - score_new) >= 0.1*fabs(score_old) &&
                   (bf_scheduler_change_score(sch, cur, hash, score_old, score_new) != 0)) {
                    // free resources
                    hashidx_stream_delete(sch->stream);
                    mdb_cursor_close(cur);
                    mdb_txn_abort(txn);
                    // and mark them as free
                    sch->stream = 0;
                    cur = 0;
                    txn = 0;
                    if (retry) {
                         // it could be an error due to insufficient space, expand database
                         // and retry just once more
                         bf_scheduler_clean_error(sch);
                         if ((bf_scheduler_grow(sch)) != 0)
                              return sch->error.code;
                         else {
                              retry = 0;
                              goto txn_start;
                         }
                    } else {
                         return sch->error.code;
                    }
               }
               break;
          case stream_state_end:
               hashidx_stream_delete(sch->stream);
               sch->stream = 0;
               break;
          case stream_state_init: // not possible really
          case stream_state_error:
               error1 = "processing the Hash/Idx stream";
               break;
          }
     }
     mdb_cursor_close(cur);
     cur = 0;

     if ((mdb_rc = mdb_txn_commit(txn)) != 0) {
          error1 = "commiting transaction";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }
     txn = 0;

     return 0;
on_error:
     if (sch->stream)
          hashidx_stream_delete(sch->stream);
     if (cur)
          mdb_cursor_close(cur);
     if (txn)
          mdb_txn_abort(txn);

     bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
     bf_scheduler_add_error(sch, error1);
     bf_scheduler_add_error(sch, error2);
     return sch->error.code;
}

static BFSchedulerError
bf_scheduler_update(BFScheduler *sch) {
     if (sch->scorer.update(sch->scorer.state) != 0) {
          bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
          bf_scheduler_add_error(sch, "updating scorer");
          return sch->error.code;
     }
     do {
          if (bf_scheduler_update_batch(sch) != 0)
               return sch->error.code;
     } while (sch->stream);

     return 0;
}


BFSchedulerError
bf_scheduler_request(BFScheduler *sch, size_t n_pages, PageRequest **request) {
     char *error1 = 0;
     char *error2 = 0;

     MDB_txn *txn = 0;
     MDB_cursor *cur = 0;
     int mdb_rc =
          mdb_txn_begin(sch->env, 0, 0, &txn) ||
          bf_scheduler_open_cursor(txn, &cur);

     PageRequest *req = *request = page_request_new(n_pages);
     if (!req) {
          error1 = "allocating memory";
          goto on_error;
     }

     while (req->n_urls < n_pages) {
          MDB_val key;
          MDB_val val;
          PageInfo *pi;
          ScheduleKey *se;

          switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_FIRST)) {
          case 0:
               se = key.mv_data;
               switch (page_db_get_info(sch->page_db, se->hash, &pi)) {
               case 0:
                    if (pi->n_crawls == 0)
                         if (page_request_add_url(req, pi->url) != 0) {
                              error1 = "adding url to request";
                              goto on_error;
                         }
                    page_info_delete(pi);
                    break;
               default:
                    error1 = "retrieving PageInfo from PageDB";
                    error2 = sch->page_db->error.message;
                    goto on_error;
                    break;
               }
               break;

          case MDB_NOTFOUND: // no more pages left
               goto all_pages_retrieved;
          default:
               error1 = "getting head of schedule";
               error2 = mdb_strerror(mdb_rc);
               goto on_error;
          }
          if ((mdb_rc = mdb_cursor_del(cur, 0)) != 0) {
               error1 = "deleting head of schedule";
               error2 = mdb_strerror(mdb_rc);
               goto on_error;
          }
     }

all_pages_retrieved:
     if ((mdb_rc = mdb_txn_commit(txn)) != 0) {
          error1 = "commiting scheduler transaction";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }
     return 0;

on_error:
     bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
     bf_scheduler_add_error(sch, error1);
     bf_scheduler_add_error(sch, error2);

     return sch->error.code;
}

/** Close scheduler */
void
bf_scheduler_delete(BFScheduler *sch) {
     mdb_env_close(sch->env);
     if (!sch->persist) {
          char *data = build_path(sch->path, "data.mdb");
          char *lock = build_path(sch->path, "lock.mdb");
          remove(data);
          remove(lock);
          free(data);
          free(lock);

          remove(sch->path);
     }
     free(sch->path);
     free(sch);
}

#if (defined TEST) && TEST
#include "CuTest.h"

void
test_bf_scheduler_requests(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error.message: "NULL",
              page_db_new(&db, test_dir_db) == 0);
     db->persist = 0;

     BFScheduler *sch;
     CuAssert(tc,
              sch != 0? sch->error.message: "NULL",
              bf_scheduler_new(&sch, db) == 0);
     sch->persist = 0;

     /* Make the link structure
      *
      *      0.0    1.0    0.1    0.5    0.4
      *   1 ---> 2 ---->4----->5------>8----->9
      *   |             |      |       |
      *   |      +------+   +--+--+    |0.2
      *   |      | 0.2   0.0|  0.5|    |
      *   | 0.1  v          v     v    |
      *   +----> 3          6     7<---+
      *
      */
     const size_t n_pages = 6;
     CrawledPage *cp;
     CrawledPage **crawl = calloc(n_pages, sizeof(*crawl));
     cp = crawl[0] = crawled_page_new("1");
     crawled_page_add_link(cp, "2", 0.0);
     crawled_page_add_link(cp, "3", 0.1);

     cp = crawl[1] = crawled_page_new("2");
     crawled_page_add_link(cp, "4", 1.0);

     cp = crawl[2] = crawled_page_new("4");
     crawled_page_add_link(cp, "3", 0.2);
     crawled_page_add_link(cp, "5", 0.1);

     cp = crawl[3] = crawled_page_new("5");
     crawled_page_add_link(cp, "6", 0.0);
     crawled_page_add_link(cp, "7", 0.5);
     crawled_page_add_link(cp, "8", 0.5);

     cp = crawl[4] = crawled_page_new("8");
     crawled_page_add_link(cp, "7", 0.2);
     crawled_page_add_link(cp, "9", 0.4);

     cp = crawl[5] = crawled_page_new("7");


     /* We have the following schedule after crawling pages 1, 2, 4, 5, 8 and 7
      * Note:
      *    - x: the page has been crawled
      *    - n: the link has not been added because it was already present
      *
      *     1           2           4           5           8           7
      * page score  page score  page score  page score  page score  page score
      * ---- -----  ---- -----  ---- -----  ---- -----  ---- -----  ---- -----
      * 3    0.1    4    1.0    4    1.0 x  4    1.0 x  4    1.0 x  4    1.0 x
      * 2    0.0    3    0.1    3    0.2 n  7    0.5    7    0.5    7    0.5 x
      *             2    0.0 x  3    0.1    8    0.5    8    0.5 x  8    0.5 x
      *                         5    0.1    3    0.2 n  9    0.4    9    0.4
      *                         2    0.0 x  3    0.1    3    0.2 n  3    0.2 n
      *                                     5    0.1 x  7    0.2 n  7    0.2 n
      *                                     6    0.0    3    0.1    3    0.1
                                            2    0.0 x  5    0.1 x  5    0.1 x
      *                                                 6    0.0    6    0.0
      *                                                 2    0.0 x  2    0.0 x
      *
      */
     for (size_t i=0; i<n_pages; ++i) {
          CuAssert(tc,
                   sch->error.message,
                   bf_scheduler_add(sch, crawl[i]) == 0);
          crawled_page_delete(crawl[i]);
     }


     /* Requests should return:
      *
      * page score
      * ---- -----
      * 9    0.4
      * 3    0.1
      * 6    0.0
      */
     PageRequest *req;
     CuAssert(tc,
              sch->error.message,
              bf_scheduler_request(sch, 2, &req) == 0);

     CuAssertStrEquals(tc, "9", req->urls[0]);
     CuAssertStrEquals(tc, "3", req->urls[1]);
     page_request_delete(req);

     CuAssert(tc,
              sch->error.message,
              bf_scheduler_request(sch, 4, &req) == 0);

     CuAssertStrEquals(tc, "6", req->urls[0]);
     CuAssert(tc, "too many requests returned", req->n_urls == 1);
     page_request_delete(req);

     bf_scheduler_delete(sch);
     page_db_delete(db);
}

/* Tests the typical database operations on a moderate crawl of 10M pages */
void
test_bf_scheduler_large(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error.message: "NULL",
              page_db_new(&db, test_dir_db) == 0);
     db->persist = 0;

     BFScheduler *sch;
     CuAssert(tc,
              sch != 0? sch->error.message: "NULL",
              bf_scheduler_new(&sch, db) == 0);
     sch->persist = 0;

     PageRankScorer *prs;
     CuAssert(tc,
              prs != 0? prs->error.message: "NULL",
              page_rank_scorer_new(&prs, db) == 0);
     page_rank_scorer_setup(prs, &sch->scorer);

     const size_t n_links = 10;
     const size_t n_pages = 10000000;

     LinkInfo links[n_links + 1];
     for (size_t j=0; j<=n_links; ++j) {
          sprintf(links[j].url = malloc(50), "test_url_%zu", j);
          links[j].score = j;
     }

     for (size_t i=0; i<n_pages; ++i) {
          for (size_t j=0; j<n_links; ++j)
               links[j] = links[j+1];
          sprintf(links[n_links].url, "test_url_%zu", i + n_links);
          links[n_links].score = i;

          CrawledPage *cp = crawled_page_new(links[0].url);
          for (size_t j=1; j<=n_links; ++j)
               crawled_page_add_link(cp, links[j].url, 0.5);

          CuAssert(tc,
                   sch->error.message,
                   bf_scheduler_add(sch, cp) == 0);
     }

     CuAssert(tc,
              sch->error.message,
              bf_scheduler_update(sch) == 0);

     page_rank_scorer_delete(prs);
     bf_scheduler_delete(sch);
     page_db_delete(db);
}

CuSuite *
test_bf_scheduler_suite(void) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_bf_scheduler_requests);
     SUITE_ADD_TEST(suite, test_bf_scheduler_large);

     return suite;
}

#endif // TEST
