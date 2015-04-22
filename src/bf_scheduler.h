#ifndef __BF_SCHEDULER_H__
#define __BF_SCHEDULER_H__

#include <pthread.h>

#include "page_db.h"
#include "scheduler.h"
#include "scorer.h"
#include "txn_manager.h"

#define BF_SCHEDULER_MAX_ERROR_LENGTH 10000
#define BF_SCHEDULER_DEFAULT_SIZE PAGE_DB_DEFAULT_SIZE
#define BF_SCHEDULER_UPDATE_BATCH_SIZE 1000
#define BF_SCHEDULER_DEFAULT_PERSIST 1

typedef enum {
     bf_scheduler_error_ok = 0,       /**< No error */
     bf_scheduler_error_memory,       /**< Error allocating memory */
     bf_scheduler_error_invalid_path, /**< File system error */
     bf_scheduler_error_internal,     /**< Unexpected error */
     bf_scheduler_error_thread
} BFSchedulerError;

/** Flow:
 *
 *   None  ----> Working -----> Stopped -----> Finished
 *                 ^              |               |
 *                 +--------------+               |
 *                 +------------------------------+
 */
typedef enum {
     update_thread_none,
     update_thread_working,
     update_thread_stopped,
     update_thread_finished
} UpdateThreadState;

typedef struct {
     /** Page database
      *
      * The page database is neither created nor destroyed by the scheduler.
      * The rationale is that the scheduler can be changed while using the same
      * @ref PageDB. The schedure is "attached" to the @ref PageDB.
      * */
     PageDB *page_db;
     /** The scorer use to get page score.
      *
      * If not set up, the PageInfo.score will be used */
     Scorer scorer;

     /** The scheduler state is maintained inside am LMDB environment */
     TxnManager *txn_manager;
     /** Path to the @ref env
      *
      * It is built by appending `_bfs` to the @ref PageDB.path
      **/
     char *path;

     /** An stream of Hash/Index pairs.
      *
      * This is necessary because page scores can change. This is stream is used
      * to make sure all page scores are revisited periodically.
      **/
     HashIdxStream *stream;

     Error error;

// Update thread
// -----------------------------------------------------------------------------
     pthread_mutex_t n_pages_mutex;
     pthread_cond_t n_pages_cond;
     double n_pages_old; /**< Number of pages when last updated was done */
     double n_pages_new; /**< Current number of pages */

     pthread_t update_thread;
     pthread_mutex_t update_mutex;
     UpdateThreadState update_state;

// Options
// -----------------------------------------------------------------------------
     /** If true, do not delete files after deleting object*/
     int persist;
} BFScheduler;


/** Allocate memory and create a new scheduler
 *
 * @param sch Where to create it. `*sch` can be NULL in case of memory error
 * @param db PageDB to attach. Remember it will not be created nor destroyed by
 *           the scheduler
 *
 * @return 0 if success, otherwise the error code
 */
BFSchedulerError
bf_scheduler_new(BFScheduler **sch, PageDB *db);

/** Add a new crawled page
 *
 * It will add the page also to the PageDB.
 *
 * @param sch
 * @param page
 *
 * @return 0 if success, otherwise the error code
 */
BFSchedulerError
bf_scheduler_add(BFScheduler *sch, const CrawledPage *page);


/** Add a new crawled page
 *
 * It will add the page also to the PageDB.
 *
 * @param sch
 * @param page
 *
 * @return 0 if success, otherwise the error code
 */
BFSchedulerError
bf_scheduler_request(BFScheduler *sch, size_t n_pages, PageRequest **request);

/** Delete scheduler.
 *
 * It may or may not delete associated disk files depending on the
 * @ref BFScheduler.persist flag
 */
void
bf_scheduler_delete(BFScheduler *sch);

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_bf_scheduler_suite(void);
#endif

#endif // __BF_SCHEDULER_H__
