#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "freq_scheduler.h"
#include "txn_manager.h"
#include "util.h"
#include "scheduler.h"

static void
freq_scheduler_set_error(FreqScheduler *sch, int code, const char *message) {
     error_set(sch->error, code, message);
}

static void
freq_scheduler_add_error(FreqScheduler *sch, const char *message) {
     error_add(sch->error, message);
}

FreqSchedulerError
freq_scheduler_new(FreqScheduler **sch, PageDB *db) {
     FreqScheduler *p = *sch = malloc(sizeof(*p));
     if (p == 0)
          return freq_scheduler_error_memory;

     p->error = error_new();
     if (p->error == 0) {
          free(p);
          return freq_scheduler_error_memory;
     }

     p->page_db = db;
     p->persist = 0;
     p->margin = -1.0; // disabled
     p->max_n_crawls = 0;

     // create directory if not present yet
     char *error = 0;
     if (!(p->path = concat(db->path, "freqs", '_')))
          error = "building scheduler path";
     else
          error = make_dir(p->path);

     if (error != 0) {
          freq_scheduler_set_error(p, page_db_error_invalid_path, __func__);
          freq_scheduler_add_error(p, error);
          return p->error->code;
     }

     if (txn_manager_new(&p->txn_manager, 0) != 0) {
          freq_scheduler_set_error(p, freq_scheduler_error_internal, __func__);
          freq_scheduler_add_error(p, p->txn_manager?
                                   p->txn_manager->error->message:
                                   "NULL");
          return p->error->code;
     }

     int rc;
     // initialize LMDB on the directory
     if ((rc = mdb_env_create(&p->txn_manager->env) != 0))
          error = "creating environment";
     else if ((rc = mdb_env_set_mapsize(p->txn_manager->env,
                                        FREQ_SCHEDULER_DEFAULT_SIZE)) != 0)
          error = "setting map size";
     else if ((rc = mdb_env_set_maxdbs(p->txn_manager->env, 1)) != 0)
          error = "setting number of databases";
     else if ((rc = mdb_env_open(
                    p->txn_manager->env,
                    p->path,
                    MDB_NOTLS | MDB_NOSYNC, 0664) != 0))
          error = "opening environment";

     if (error != 0) {
          freq_scheduler_set_error(p, freq_scheduler_error_internal, __func__);
          freq_scheduler_add_error(p, error);
          freq_scheduler_add_error(p, mdb_strerror(rc));
          return p->error->code;
     }

     return p->error->code;
}

static int
freq_scheduler_open_cursor(MDB_txn *txn, MDB_cursor **cursor) {
     MDB_dbi dbi;
     int mdb_rc =
          mdb_dbi_open(txn, "schedule", MDB_CREATE, &dbi) ||
          mdb_set_compare(txn, dbi, schedule_entry_mdb_cmp_asc) ||
          mdb_cursor_open(txn, dbi, cursor);

     if (mdb_rc != 0)
          *cursor = 0;

     return mdb_rc;
}

FreqSchedulerError
freq_scheduler_load_simple(FreqScheduler *sch,
                           float freq_default,
                           float freq_scale) {
     char *error1 = 0;
     char *error2 = 0;

     HashInfoStream *st;
     if (hashinfo_stream_new(&st, sch->page_db) != 0) {
          error1 = "creating stream";
          error2 = st? sch->page_db->error->message: "NULL";
          goto on_error;
     }

     MDB_txn *txn = 0;
     if (txn_manager_begin(sch->txn_manager, 0, &txn) != 0) {
          error1 = "starting transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }

     MDB_cursor *cur = 0;
     int mdb_rc = freq_scheduler_open_cursor(txn, &cur);
     if (mdb_rc != 0) {
          error1 = "opening cursor";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }

     StreamState ss;
     uint64_t hash;
     PageInfo *pi;

     while ((ss = hashinfo_stream_next(st, &hash, &pi)) == stream_state_next) {
          if ((pi->n_crawls > 0) &&
	      ((sch->max_n_crawls == 0) || (pi->n_crawls < sch->max_n_crawls)) &&
	      !page_info_is_seed(pi)){

               ScheduleKey sk = {
                    .score = 0,
                    .hash  = hash
               };
               MDB_val key = {
                    .mv_size = sizeof(sk),
                    .mv_data = &sk,
               };
               float freq = freq_default;
               if (freq_scale > 0) {
                    float rate = page_info_rate(pi);
                    if (rate > 0) {
                         freq = freq_scale * rate;
                    }
               }
	       if (freq > 0) {
		    MDB_val val = {
			 .mv_size = sizeof(float),
			 .mv_data = &freq,
		    };
		    if ((mdb_rc = mdb_cursor_put(cur, &key, &val, 0)) != 0) {
			 error1 = "adding page to schedule";
			 error2 = mdb_strerror(mdb_rc);
			 goto on_error;
		    }
	       }
          }
          page_info_delete(pi);
     }
     if (ss != stream_state_end) {
          error1 = "incorrect stream state";
          error2 = 0;
          hashinfo_stream_delete(st);
          goto on_error;
     }
     hashinfo_stream_delete(st);

     if (txn_manager_commit(sch->txn_manager, txn) != 0) {
          error1 = "commiting schedule transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }

     return sch->error->code;

on_error:
     if (txn != 0)
          txn_manager_abort(sch->txn_manager, txn);
     freq_scheduler_set_error(sch, freq_scheduler_error_internal, __func__);
     freq_scheduler_add_error(sch, error1);
     freq_scheduler_add_error(sch, error2);
     return sch->error->code;
}

FreqSchedulerError
freq_scheduler_load_mmap(FreqScheduler *sch, MMapArray *freqs) {
     char *error1 = 0;
     char *error2 = 0;
     MDB_txn *txn = 0;
     MDB_cursor *cur = 0;

     if (txn_manager_expand(
              sch->txn_manager,
              2*freqs->n_elements*freqs->element_size) != 0) {
          error1 = "resizing database";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }

     if (txn_manager_begin(sch->txn_manager, 0, &txn) != 0) {
          error1 = "starting transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }
     int mdb_rc = freq_scheduler_open_cursor(txn, &cur);
     if (mdb_rc != 0) {
          error1 = "opening cursor";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }
     for (size_t i=0; i<freqs->n_elements; ++i) {
          PageFreq *f = mmap_array_idx(freqs, i);
          ScheduleKey sk = {
               .score = 1.0/f->freq,
               .hash = f->hash
          };
          MDB_val key = {
               .mv_size = sizeof(sk),
               .mv_data = &sk,
          };
          MDB_val val = {
               .mv_size = sizeof(float),
               .mv_data = &f->freq,
          };
          if ((mdb_rc = mdb_cursor_put(cur, &key, &val, 0)) != 0) {
               error1 = "adding page to schedule";
               error2 = mdb_strerror(mdb_rc);
               goto on_error;
          }
     }
     if (txn_manager_commit(sch->txn_manager, txn) != 0) {
          error1 = "commiting schedule transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }

     return sch->error->code;

on_error:
     if (txn != 0)
          txn_manager_abort(sch->txn_manager, txn);
     freq_scheduler_set_error(sch, freq_scheduler_error_internal, __func__);
     freq_scheduler_add_error(sch, error1);
     freq_scheduler_add_error(sch, error2);
     return sch->error->code;
}

FreqSchedulerError
freq_scheduler_request(FreqScheduler *sch,
                       size_t max_requests,
                       PageRequest **request) {
     char *error1 = 0;
     char *error2 = 0;

     MDB_txn *txn = 0;
     MDB_cursor *cur = 0;

     if (txn_manager_begin(sch->txn_manager, 0, &txn) != 0) {
          error1 = "starting transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }

     int mdb_rc = freq_scheduler_open_cursor(txn, &cur);
     if (mdb_rc != 0) {
          error1 = "opening cursor";
          error2 = mdb_strerror(mdb_rc);
          goto on_error;
     }

     PageRequest *req = *request = page_request_new(max_requests);
     if (!req) {
          error1 = "allocating memory";
          goto on_error;
     }

     int interrupt_requests = 0;
     while ((req->n_urls < max_requests) && !interrupt_requests) {
          MDB_val key;
          MDB_val val;
          ScheduleKey sk;
          float freq;

	  int crawl = 0;
          switch (mdb_rc = mdb_cursor_get(cur, &key, &val, MDB_FIRST)) {
          case 0:
	       // copy data before deleting cursor
               sk = *(ScheduleKey*)key.mv_data;
               freq = *(float*)val.mv_data;


               PageInfo *pi = 0;
               if (page_db_get_info(sch->page_db, sk.hash, &pi) != 0) {
                    error1 = "retrieving PageInfo from PageDB";
                    error2 = sch->page_db->error->message;
                    goto on_error;
               }

               if (pi) {
                    if (sch->margin >= 0) {
                         double elapsed = difftime(time(0), 0) - pi->last_crawl;
                         if (elapsed < 1.0/(freq*(1.0 + sch->margin)))
                              interrupt_requests = 1;
                    }
		    crawl = (sch->max_n_crawls == 0) || (pi->n_crawls < sch->max_n_crawls);
	       }
	       if (!interrupt_requests) {
		    if ((mdb_rc = mdb_cursor_del(cur, 0)) != 0) {
			 error1 = "deleting head of schedule";
			 error2 = mdb_strerror(mdb_rc);
			 goto on_error;
		    }
		    if (crawl) {
			 if (page_request_add_url(req, pi->url) != 0) {
			      error1 = "adding url to request";
			      goto on_error;
			 }

			 sk.score += 1.0/freq;

			 val.mv_data = &freq;
			 key.mv_data = &sk;
			 if ((mdb_rc = mdb_cursor_put(cur, &key, &val, 0)) != 0) {
			      error1 = "moving element inside schedule";
			      error2 = mdb_strerror(mdb_rc);
			      goto on_error;
			 }
		    }
	       }
	       page_info_delete(pi);

               break;

          case MDB_NOTFOUND: // no more pages left
               interrupt_requests = 1;
               break;
          default:
               error1 = "getting head of schedule";
               error2 = mdb_strerror(mdb_rc);
               goto on_error;
          }
     }
     if (txn_manager_commit(sch->txn_manager, txn) != 0) {
          error1 = "commiting schedule transaction";
          error2 = sch->txn_manager->error->message;
          goto on_error;
     }
     return 0;
on_error:
     freq_scheduler_set_error(sch, freq_scheduler_error_internal, __func__);
     freq_scheduler_add_error(sch, error1);
     freq_scheduler_add_error(sch, error2);

     return sch->error->code;
}

FreqSchedulerError
freq_scheduler_add(FreqScheduler *sch, const CrawledPage *page) {
     if (page_db_add(sch->page_db, page, 0) != 0) {
          freq_scheduler_set_error(sch, freq_scheduler_error_internal, __func__);
          freq_scheduler_add_error(sch, "adding crawled page");
          freq_scheduler_add_error(sch, sch->page_db->error->message);
     }
     return sch->error->code;
}

void
freq_scheduler_delete(FreqScheduler *sch) {
     mdb_env_close(sch->txn_manager->env);
     (void)txn_manager_delete(sch->txn_manager);
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
     error_delete(sch->error);
     free(sch);
}

#if (defined TEST) && TEST
#include "test_freq_scheduler.c"
#endif // TEST
