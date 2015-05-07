#ifndef __TXN_MANAGER_H__
#define __TXN_MANAGER_H__

#include <pthread.h>
#include "lmdb.h"

#include "util.h"

/** @addtogroup InvSemaphore
 *
 * Inverse semaphore: it blocks when count is greater than zero.
 *
 * The main use is inside @ref TxnManager where it is used to track the number
 * of open read and write transactions and block certain operations accordingly.
 * @{
 */

/** Inverse Semaphore.
 *
 * An inverse semaphore blocks when the count is greater than zero (a regular
 * semaphore blocks when the count is at zero).
 */
typedef struct {
     pthread_cond_t cond;
     pthread_mutex_t mtx_inc_dec;
     pthread_mutex_t mtx_inc_pos;
     int value;
} InvSemaphore;

/** Initialize.
 *
 *  @return A pthread error code
 */
int
inv_semaphore_init(InvSemaphore *is);

/** Increment counter.
 *
 * This call will be blocked if the semaphore is blocked.
 *  @return A pthread error code
 */
int
inv_semaphore_inc(InvSemaphore *is);

/** Decrement counter.
 *
 *  @return A pthread error code
 */
int
inv_semaphore_dec(InvSemaphore *is);

/** Return the count */
int
inv_semaphore_count(InvSemaphore *is);

/** Block the semaphore until count reaches zero.
 *
 * While the semaphore is blocked @ref inv_semaphore_inc is blocked
 * but @ref inv_semaphore_dec is allowed.
 *
 * @return A pthread error code
 */
int
inv_semaphore_block(InvSemaphore *is);

/** Cancel the effect of @ref inv_semaphore_block
 *
 * @return A pthread error code
 */
int
inv_semaphore_release(InvSemaphore *is);

/** Destroy semaphore (does NOT free memory) */
int
inv_semaphore_destroy(InvSemaphore *is);

/// @}

/** @addtogroup TxnManager
 *
 * If you create transactions using this wrapper around an LMDB environment
 * it will track how many transactions are open and will allow you to make
 * safe database resizes.
 *
 * @{
 */

typedef enum {
     txn_manager_error_ok = 0,   /**< No error */
     txn_manager_error_internal, /**< Unexpected error */
     txn_manager_error_memory,   /**< Error allocating new memory */
     txn_manager_error_thread,   /**< Error inside pthreads */
     txn_manager_error_mdb       /**< Error inside LMDB */
} TxnManagerError;

/** Transaction Manager.
 *
 * LMDB has several restrictions in the operations it allows in multiple threads,
 * but some of these restrictions must be imposed in the application code.
 * In particular:
 *
 * 1. Some operations require that no transactions in the same process are
 *    active, for example mdb_env_set_mapsize
 *
 * 2. Some operations require that no write transactions are active. For
 *    example it is not documented, but it seems to happen that, mdb_env_info
 *    crashes if write transactions are active.
 *
 * This structure tracks the number of read and write transactions active inside the
 * process and allows blocking until all of them are aborted or commited.
 */
typedef struct {
     MDB_env *env; /**< LMDB environment where transactions happen */
     InvSemaphore txn_counter_read;  /**< Counter of read transactions */
     InvSemaphore txn_counter_write; /**< Counter of write transactions */

     Error *error;
} TxnManager;

/** Allocate a new TxnManager
 *
 * @param tm The new transaction manager.
 * @param env The LMDB environment where transactions will be opened, aborted or commited.
 *
 * @return 0 if success, otherwise error code.
 * */
TxnManagerError
txn_manager_new(TxnManager **tm, MDB_env *env);

/** Begin a new transaction.
 *
 * @param tm
 * @param flags The flags that you pass to LMDB's mdb_txn_begin. These flags
 *              will be checked for MDB_RDONLY to decide which transaction
 *              counter to increment. This operation will block if an
 *              environment resize is in progress.
 * @param txn New transaction.
 *
 * @return 0 if success, otherwise error code.
 */
TxnManagerError
txn_manager_begin(TxnManager *tm, int flags, MDB_txn **txn);

/** Commit transaction.
 *
 * The corresponding counter will be decremented
 */
TxnManagerError
txn_manager_commit(TxnManager *tm, MDB_txn *txn);

/** Abort transaction.
 *
 * The corresponding counter will be decremented
 */
TxnManagerError
txn_manager_abort(TxnManager *tm, MDB_txn *txn);

/** Destroy and free manager */
TxnManagerError
txn_manager_delete(TxnManager *tm);

/** Parameter associated to @ref txn_manager_expand.
 *
 * The mmap is resized when the remaining free space is less than this amount.
 */
#define MDB_MINIMUM_FREE_PAGES 10000

/** Check if the environment must be resized. If this is the case then resize
 *  it.
 *
 * This call will block for sure until there are no write transactions active.
 * This call may block until there are no read transactions active, only if a resize
 * is necessary.
 *
 * If a resize happens then creation of new read and write transactions will be
 * blocked until it finishes.
 */
TxnManagerError
txn_manager_expand(TxnManager *tm);

/// @}
#endif // __TXN_MANAGER_H__
