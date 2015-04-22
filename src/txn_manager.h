#ifndef __TXN_MANAGER_H__
#define __TXN_MANAGER_H__

#include <pthread.h>
#include "lmdb.h"

#include "util.h"

typedef struct {
     pthread_cond_t cond;
     pthread_mutex_t mtx_inc_dec;
     pthread_mutex_t mtx_inc_pos;
     int value;
} InvSemaphore;

int
inv_semaphore_init(InvSemaphore *is);

int
inv_semaphore_inc(InvSemaphore *is);

int
inv_semaphore_dec(InvSemaphore *is);

int
inv_semaphore_count(InvSemaphore *is);

int
inv_semaphore_block(InvSemaphore *is);

int
inv_semaphore_release(InvSemaphore *is);

int
inv_semaphore_destroy(InvSemaphore *is);

typedef enum {
     txn_manager_error_ok = 0,
     txn_manager_error_internal,
     txn_manager_error_memory,
     txn_manager_error_thread,
     txn_manager_error_mdb
} TxnManagerError;

typedef struct {
     MDB_env *env;     
     InvSemaphore txn_counter_read;
     InvSemaphore txn_counter_write;
     Error error;
} TxnManager;

TxnManagerError
txn_manager_new(TxnManager **tm, MDB_env *env);

TxnManagerError
txn_manager_begin(TxnManager *tm, int flags, MDB_txn **txn);

TxnManagerError
txn_manager_commit(TxnManager *tm, MDB_txn *txn);

TxnManagerError
txn_manager_abort(TxnManager *tm, MDB_txn *txn);

TxnManagerError
txn_manager_delete(TxnManager *tm);

#define MDB_MINIMUM_FREE_PAGES 10000

TxnManagerError
txn_manager_expand(TxnManager *tm);

#endif // __TXN_MANAGER_H__
