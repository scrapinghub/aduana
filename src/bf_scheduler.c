#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <malloc.h>
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


static void
bf_scheduler_set_error(BFScheduler *sch, int code, const char *message) {
     error_set(&sch->error, code, message);
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


/** Order floats from lower to greater */
static int
mdb_cmp_float(const MDB_val *a, const MDB_val *b) {
     float x = *(float*)(a->mv_data);
     float y = *(float*)(b->mv_data);
     return x < y? -1: x > y? +1: 0;
}

BFSchedulerError
bf_scheduler_new(BFScheduler **sch, PageDB *db) {
     BFScheduler *p = *sch = malloc(sizeof(*p));
     if (!p)
          return bf_scheduler_error_memory;
     bf_scheduler_set_error(p, bf_scheduler_error_ok, "NO ERROR");

     p->page_db = db;

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
          mdb_dbi_open(txn, "schedule", MDB_CREATE | MDB_DUPSORT, &dbi) ||
          mdb_set_compare(txn, dbi, mdb_cmp_float) ||
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
               float score = -pi->score;
               MDB_val key = {
                    .mv_size = sizeof(score),
                    .mv_data = &score
               };
               MDB_val val = {
                    .mv_size = sizeof(node->hash),
                    .mv_data = &node->hash
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
          if (bf_scheduler_grow(sch) == 0)
               goto txn_start;
     }

     bf_scheduler_set_error(sch, bf_scheduler_error_internal, __func__);
     bf_scheduler_add_error(sch, error1);
     bf_scheduler_add_error(sch, error2);
     return sch->error.code;
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

          switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_FIRST)) {
          case 0:
               switch (page_db_get_info_from_hash(sch->page_db, *(uint64_t*)val.mv_data, &pi)) {
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
     free(sch->path);
     free(sch);
}


#if (defined TEST) && TEST
#include "CuTest.h"

void
test_bf_scheduler_requests(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);
     char *data_db = build_path(test_dir_db, "data.mdb");
     char *lock_db = build_path(test_dir_db, "lock.mdb");

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error.message: "NULL",
              page_db_new(&db, test_dir_db) == 0);

     BFScheduler *sch;
     CuAssert(tc,
              sch != 0? sch->error.message: "NULL",
              bf_scheduler_new(&sch, db) == 0);

     char *test_dir_sch = sch->path;
     char *data_sch = build_path(test_dir_sch, "data.mdb");
     char *lock_sch = build_path(test_dir_sch, "lock.mdb");

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
     remove(data_sch);
     remove(lock_sch);
     remove(test_dir_sch);

     page_db_delete(db);
     remove(data_db);
     remove(lock_db);
     remove(test_dir_db);
}

CuSuite *
test_bf_scheduler_suite(void) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_bf_scheduler_requests);
     return suite;
}

#endif // TEST
