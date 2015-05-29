#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <assert.h>
#include <errno.h>
#include <lmdb.h>
#ifdef __APPLE__
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>

#include "txn_manager.h"
#include "util.h"

int
inv_semaphore_init(InvSemaphore *is) {
     int rc = 0;

     if ((rc = pthread_cond_init(&is->cond, 0)) != 0)
          return rc;
     if ((rc = pthread_mutex_init(&is->mtx_inc_dec, 0)) != 0)
          return rc;
     if ((rc = pthread_mutex_init(&is->mtx_inc_pos, 0)) != 0)
          return rc;
     is->value = 0;

     return rc;
}

int
inv_semaphore_inc(InvSemaphore *is) {
     int rc = 0;

     if ((rc = pthread_mutex_lock(&is->mtx_inc_pos)) != 0)
          return rc;
     if ((rc = pthread_mutex_lock(&is->mtx_inc_dec)) != 0)
          return rc;
     is->value++;
     if ((rc = pthread_mutex_unlock(&is->mtx_inc_dec)) != 0)
          return rc;
     rc = pthread_mutex_unlock(&is->mtx_inc_pos);
     return rc;
}

int
inv_semaphore_dec(InvSemaphore *is) {
     int rc = 0;

     if ((rc = pthread_mutex_lock(&is->mtx_inc_dec)) != 0)
          return rc;
     is->value--;
     if ((rc = pthread_cond_broadcast(&is->cond)) != 0)
          return rc;
     rc = pthread_mutex_unlock(&is->mtx_inc_dec);
     return rc;
}

int
inv_semaphore_count(InvSemaphore *is) {
     return is->value;
}

int
inv_semaphore_block(InvSemaphore *is) {
     int rc = 0;
     if ((rc = pthread_mutex_lock(&is->mtx_inc_pos)) != 0)
          return rc;
     if ((rc = pthread_mutex_lock(&is->mtx_inc_dec)) != 0)
          return rc;
     // 1. keep inc_pos to block an increment
     // 2. release inc_dec and wait from a signal from a decrement
     while (is->value > 0)
          if ((rc = pthread_cond_wait(&is->cond, &is->mtx_inc_dec)) != 0)
               return rc;
     return rc;
}

int
inv_semaphore_release(InvSemaphore *is) {
     int rc = pthread_mutex_unlock(&is->mtx_inc_dec);
     if (rc != 0)
          return rc;
     return pthread_mutex_unlock(&is->mtx_inc_pos);
}

int
inv_semaphore_destroy(InvSemaphore *is) {
     int rc = 0;

     if ((rc = pthread_cond_destroy(&is->cond)) != 0)
          return rc;
     if ((rc = pthread_mutex_destroy(&is->mtx_inc_dec)) != 0)
          return rc;
     if ((rc = pthread_mutex_destroy(&is->mtx_inc_pos)) != 0)
          return rc;

     return rc;
}

TxnManagerError
txn_manager_new(TxnManager **tm, MDB_env *env) {
     TxnManager *p = *tm = malloc(sizeof(*p));
     if (!p)
          return txn_manager_error_memory;

     if (!(p->error = error_new())) {
          free(p);
          return txn_manager_error_memory;
     }

     p->env = env;
     if (inv_semaphore_init(&p->txn_counter_read) != 0)
          error_set(p->error, txn_manager_error_thread, "creating read txn counter");
     else if (inv_semaphore_init(&p->txn_counter_write) != 0)
          error_set(p->error, txn_manager_error_thread, "creating write txn counter");

     return p->error->code;
}

TxnManagerError
txn_manager_begin(TxnManager *tm, int flags, MDB_txn **txn) {
     InvSemaphore *counter =
          flags & MDB_RDONLY? &tm->txn_counter_read: &tm->txn_counter_write;

     if (inv_semaphore_inc(counter) != 0) {
          error_set(tm->error, txn_manager_error_thread, __func__);
          error_add(tm->error, "incrementing txn counter");
          return tm->error->code;
     }
     int mdb_rc = mdb_txn_begin(tm->env, 0, flags, txn);
     // other process has changed database size. Try to adapt to new size
     if (mdb_rc == MDB_MAP_RESIZED)
          mdb_rc =
               mdb_env_set_mapsize(tm->env, 0) ||
               mdb_txn_begin(tm->env, 0, flags, txn);

     if (mdb_rc != 0) {
          error_set(tm->error, txn_manager_error_mdb, __func__);
          error_add(tm->error, "beginning new transaction");
          error_add(tm->error, mdb_strerror(mdb_rc));
     }
     return tm->error->code;
}

TxnManagerError
txn_manager_commit(TxnManager *tm, MDB_txn *txn) {
     InvSemaphore *counter =
          mdb_txn_rdonly(txn)?
          &tm->txn_counter_read: &tm->txn_counter_write;

     int mdb_rc = mdb_txn_commit(txn);
     if (mdb_rc != 0) {
          error_set(tm->error, txn_manager_error_mdb, __func__);
          error_add(tm->error, "commiting new transaction");
          error_add(tm->error, mdb_strerror(mdb_rc));

          txn_manager_abort(tm, txn);
     } else if (inv_semaphore_dec(counter) != 0) {
          error_set(tm->error, txn_manager_error_thread, __func__);
          error_add(tm->error, "decrementing txn counter");
     }
     return tm->error->code;
}

TxnManagerError
txn_manager_abort(TxnManager *tm, MDB_txn *txn) {
    InvSemaphore *counter =
          mdb_txn_rdonly(txn)?
          &tm->txn_counter_read: &tm->txn_counter_write;

     mdb_txn_abort(txn);
     if (inv_semaphore_dec(counter) != 0) {
          error_set(tm->error, txn_manager_error_thread, __func__);
          error_add(tm->error, "decrementing txn counter");
     }
     return tm->error->code;
}

TxnManagerError
txn_manager_delete(TxnManager *tm) {
     if (inv_semaphore_count(&tm->txn_counter_read) != 0) {
          error_set(tm->error, txn_manager_error_internal, __func__);
          error_add(tm->error, "read transactions still active");
     } else if (inv_semaphore_count(&tm->txn_counter_write) != 0) {
          error_set(tm->error, txn_manager_error_internal, __func__);
          error_add(tm->error, "write transactions still active");
     } else if (inv_semaphore_destroy(&tm->txn_counter_read) != 0) {
          error_set(tm->error, txn_manager_error_thread, __func__);
          error_add(tm->error, "destroying read txn counter");
     } else if (inv_semaphore_destroy(&tm->txn_counter_write) != 0) {
          error_set(tm->error, txn_manager_error_thread, __func__);
          error_add(tm->error, "destroying write txn counter");
     } else {
          error_delete(tm->error);
          free(tm);
          return 0;
     }
     return tm->error->code;
}


TxnManagerError
txn_manager_expand(TxnManager *tm) {
     int rc = 0;
     char *error = 0;

#define ERROR(msg, label) do{\
          error = msg;\
          goto label;\
     } while(0)


     if ((rc = inv_semaphore_block(&tm->txn_counter_write)) != 0)
          ERROR("blocking write counter", error_thread);

     MDB_envinfo info;
     if ((rc = mdb_env_info(tm->env, &info)) != 0)
          ERROR("getting environment info", error_mdb);

     MDB_stat stat;
     if ((rc = mdb_env_stat(tm->env, &stat)) != 0)
          ERROR("getting environment stats", error_mdb);

     size_t max_pgno = info.me_mapsize/stat.ms_psize;
     if (max_pgno < info.me_last_pgno + MDB_MINIMUM_FREE_PAGES) {
          // we disallow creating new transactions, but allow aborting/commiting
          // until the txn_counter reaches 0
          if ((rc = inv_semaphore_block(&tm->txn_counter_read)) != 0)
               ERROR("blocking read counter", error_thread);

          // at this point no transactions are active
          if ((rc = mdb_env_set_mapsize(tm->env, info.me_mapsize*2)) != 0)
               ERROR("increasing mapsize", error_mdb);

          // allow transactions again
          if ((rc = inv_semaphore_release(&tm->txn_counter_read)) != 0)
               ERROR("releasing read counter", error_thread);
     }
     if ((rc = inv_semaphore_release(&tm->txn_counter_write)) != 0)
          ERROR("releasing write counter", error_thread);

     return tm->error->code;
error_thread:
     error_set(tm->error, txn_manager_error_thread, __func__);
     error_add(tm->error, error);
     error_add(tm->error, strerror(rc));
error_mdb:
     error_set(tm->error, txn_manager_error_mdb, __func__);
     error_add(tm->error, error);
     error_add(tm->error, mdb_strerror(rc));
     return tm->error->code;
}
