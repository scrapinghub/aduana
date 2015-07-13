#ifndef __FREQ_SCHEDULER_H__
#define __FREQ_SCHEDULER_H__

#include "lmdb.h"

#include "txn_manager.h"
#include "scheduler.h"
#include "util.h"
#include "page_db.h"
#include "mmap_array.h"

/** @addtogroup FreqScheduler
 * @{
 */

/** A page recrawl frequency specification */
typedef struct {
     uint64_t hash; /**< URL hash as returned by @ref page_db_hash */
     float freq;    /**< Frequency (Hz) */
} PageFreq;

/** Size of the mmap to store the schedule */
#define FREQ_SCHEDULER_DEFAULT_SIZE PAGE_DB_DEFAULT_SIZE

typedef enum {
     freq_scheduler_error_ok = 0,       /**< No error */
     freq_scheduler_error_memory,       /**< Error allocating memory */
     freq_scheduler_error_invalid_path, /**< File system error */
     freq_scheduler_error_internal      /**< Unexpected error */
} FreqSchedulerError;

typedef struct {
     char *path;              /**< Path to the LMDB environment */
     PageDB *page_db;         /**< Associated PageDB */
     TxnManager *txn_manager; /**< Transcation manager */

     Error *error;
// Options
// -----------------------------------------------------------------------------
     /** If true, do not delete files after deleting object*/
     int persist;
     /** If positive, do not crawl above this margin frequency.
      *
      * More exactly, pause crawl (do not return more requests) if:
      *                                                  1
      *     current_time - last_crawl_time < --------------------------
      *                                      frequency * (1.0 + margin)
      */
     float margin;
     /** Do not crawl more than this specified number of times */
     size_t max_n_crawls;
} FreqScheduler;


/** Allocate memory and create a new scheduler
 *
 * @param sch Where to create it. `*sch` can be NULL in case of memory error
 * @param db PageDB to attach. Remember it will not be created nor destroyed by
 *           the scheduler
 * @param path Where to create the scheduler database. Can be NULL in which case
 *             the path will be the same one as db->path with suffix '_freqs'
 * @return 0 if success, otherwise the error code
 */
FreqSchedulerError
freq_scheduler_new(FreqScheduler **sch, PageDB *db, const char *path);

/** Load a simple frequency scheduler.
 *
 * @param sch          Frequency scheduler
 * @param freq_default This is a mandatory parameter. This is the frequency to
 *                     be used if no page change rate can be computed.
 * @param freq_scale   If positive pages will be crawled with frequency
 *                     freq_scale*page_change_rate. If negative this parameter is not
 *                     used and freq_default is used for all pages.
 */
FreqSchedulerError
freq_scheduler_load_simple(FreqScheduler *sch,
                           float freq_default,
                           float freq_scale);

/** Load frequency scheduler from an @ref MMapArray of @ref PageFreq */
FreqSchedulerError
freq_scheduler_load_mmap(FreqScheduler *sch, MMapArray *freqs);

/** Return new pages to be crawled
 *
 * @param sch
 * @param n_pages Maximum number of @ref PageRequest to return
 * @param request An array of at most n_pages elements
 *
 * @return 0 if success, otherwise the error code
 */
FreqSchedulerError
freq_scheduler_request(FreqScheduler *sch,
                       size_t max_requests,
                       PageRequest **request);

/** Add a new crawled page
 *
 * It will add the page also to the PageDB.
 *
 * @param sch
 * @param page
 *
 * @return 0 if success, otherwise the error code
 */
FreqSchedulerError
freq_scheduler_add(FreqScheduler *sch, const CrawledPage *page);

/** Delete scheduler.
 *
 * It may or may not delete associated disk files depending on the
 * @ref FreqScheduler.persist flag
 */
void
freq_scheduler_delete(FreqScheduler *sch);

/** Return a new cursor pointing to the schedule */
FreqSchedulerError
freq_scheduler_cursor_open(FreqScheduler *sch, MDB_cursor **cursor);

/** Save changes to cursor */
FreqSchedulerError
freq_scheduler_cursor_commit(FreqScheduler *sch, MDB_cursor *cursor);

/** Discard changes to cursor */
void
freq_scheduler_cursor_abort(FreqScheduler *sch, MDB_cursor *cursor);

/** Set crawl frequency for a given page
 *
 * @param sch
 * @param cursor As returned by @ref freq_scheduler_cursor_open
 * @param hash   URL hash as returned by @ref page_db_hash
 * @param freq   Frequency in Hz. 0 or negative will be ignored.
 * */
FreqSchedulerError
freq_scheduler_cursor_write(FreqScheduler *sch,
			    MDB_cursor *cursor,
			    uint64_t hash,
			    float freq);

/** Write schedule to file.
 *
 * There are 3 columns: score, URL hash and frequency.
 */
FreqSchedulerError
freq_scheduler_dump(FreqScheduler *sch, FILE *output);

/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_freq_scheduler_suite(size_t n_pages);
#endif

#endif // __FREQ_SCHEDULER_H__
