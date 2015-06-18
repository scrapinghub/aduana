#ifndef __BF_SCHEDULER_H__
#define __BF_SCHEDULER_H__

#include <pthread.h>

#include "page_db.h"
#include "scheduler.h"
#include "scorer.h"
#include "txn_manager.h"

/** @addtogroup BFScheduler
 * @{
 */

/** Size of the mmap to store the schedule */
#define BF_SCHEDULER_DEFAULT_SIZE PAGE_DB_DEFAULT_SIZE

/** Size of the batch used in updating the schedule.
 *
 * Updating the schedule involves starting a write transaction. However write
 * transactions coming from multiple threads are serialized. Since adding new
 * pages to the schedule and returning requests also start write transactions it
 * means that the update thread could block this more critical operations. To
 * avoid this we avoid long write transactions and split them in batches.
 */
#define BF_SCHEDULER_UPDATE_BATCH_SIZE 100

/** Default value for BFScheduler::persist */
#define BF_SCHEDULER_DEFAULT_PERSIST 1

/** Number of steps to take between soft and hard crawl rate limit */
#define BF_SCHEDULER_CRAWL_RATE_STEPS 5

/** Don't update scores until this amount of new pages has arrived */
#define BF_SCHEDULER_UPDATE_NUM_PAGES 100

/** Don't update scores until this percentage of new pages has arrived */
#define BF_SCHEDULER_UPDATE_PER_PAGES 0.01

typedef enum {
     bf_scheduler_error_ok = 0,       /**< No error */
     bf_scheduler_error_memory,       /**< Error allocating memory */
     bf_scheduler_error_invalid_path, /**< File system error */
     bf_scheduler_error_internal,     /**< Unexpected error */
     bf_scheduler_error_thread        /**< Error inside the threading library */
} BFSchedulerError;

/** Flow:
 *
   @verbatim
           start          stop          automatic
     None  ----> Working -----> Stopped----------> Finished
                   ^              |                   |
                   +--------------+                   |
                   |    start                         |
                   +----------------------------------+
                                    start

   @endverbatim
 *
 * start is commanded by @ref bf_scheduler_update_start and
 * stop is commanded by @ref bf_scheduler_update_stop
 */
typedef enum {
     update_thread_none,      /**< The thread has not been started */
     update_thread_working,   /**< Thread started and working */
     update_thread_stopped,   /**< Thread has been commanded to stop, but has not exited yet */
     update_thread_finished   /**< Thread finished */
} UpdateThreadState;

/** All variables associated with just the update thread */
typedef struct {
     /** An stream of Hash/Index pairs.
      *
      * This is necessary because page scores can change. This is stream is used
      * to make sure all page scores are revisited periodically.
      **/
     HashIdxStream *stream;

     /** We only perform an update of scores and schedule when enough new pages
      * have been added, otherwise the update thread sleeps */
     pthread_mutex_t n_pages_mutex;
     pthread_cond_t n_pages_cond;    /**< Signaled when a page is added */
     double n_pages_old;             /**< Number of pages when last updated was done */
     double n_pages_new;             /**< Current number of pages */

     /** The update thread runs in parallel the scorer and updates the schedule
         when changes in score happen */
     pthread_t thread;
     pthread_mutex_t state_mutex;   /**< Sync access to update_state */
     UpdateThreadState state; /**< See @ref UpdateThreadState */
} UpdateThread;

/** BestFirst scheduler.
 *
 * As it name implies this scheduler follows a greedy
 * strategy to decide which page is going to crawl next. It mains an ordered
 * list of uncrawled pages. To decide the next page to be crawled this scheduler
 * picks the highest score page and removes it from the top of the list.
 *
 * The key is then to assign valid scores to the pages. If no scorer is selected
 * this scheduler will use the score provided when the page is
 * crawled. Additionally an alternative scorer can be set up, see for example
 * @ref page_rank_scorer_setup or @ref hits_scorer_setup.
 */
typedef struct {
     /** Page database
      *
      * The page database is neither created nor destroyed by the scheduler.
      * The rationale is that the scheduler can be changed while using the same
      * @ref PageDB. The schedule is "attached" to the @ref PageDB.
      * */
     PageDB *page_db;
     /** The scorer use to get page score.
      *
      * If not set up, the PageInfo.score will be used */
     Scorer *scorer;

     /** The scheduler state is maintained inside am LMDB environment */
     TxnManager *txn_manager;
     /** Path to the @ref env
      *
      * It is built by appending `_bfs` to the @ref PageDB.path
      **/
     char *path;

     UpdateThread *update_thread;

     Error *error;
// Options
// -----------------------------------------------------------------------------
     /** If true, do not delete files after deleting object*/
     int persist;
     /** Maximum crawls per second per domain */
     float max_soft_domain_crawl_rate;
     /** Maximum crawls per second per domain */
     float max_hard_domain_crawl_rate;
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

/** Start the update thread.
 *
 * The update thread will run periodically the scorer, in case there is one,
 * to recompute page scores.
 **/
BFSchedulerError
bf_scheduler_update_start(BFScheduler *sch);

/** Stop the update thread */
BFSchedulerError
bf_scheduler_update_stop(BFScheduler *sch);

/** Set persist option for scheduler */
void
bf_scheduler_set_persist(BFScheduler *sch, int value);

/** Set @ref BFScheduler::max_soft_domain_crawl_rate and
 * @ref BFScheduler::max_hard_domain_crawl_rate*/
BFSchedulerError
bf_scheduler_set_max_domain_crawl_rate(BFScheduler *sch,
                                       float max_soft_crawl_rate,
                                       float max_hard_crawl_rate);

/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_bf_scheduler_suite(size_t n_pages);
#endif

#endif // __BF_SCHEDULER_H__
