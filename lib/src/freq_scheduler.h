#ifndef __FREQ_SCHEDULER_H__
#define __FREQ_SCHEDULER_H__

#include "txn_manager.h"
#include "scheduler.h"
#include "util.h"
#include "page_db.h"
#include "mmap_array.h"

typedef struct {
     uint64_t hash;
     float freq;
} PageFreq;

/** Size of the mmap to store the schedule */
#define FREQ_SCHEDULER_DEFAULT_SIZE PAGE_DB_DEFAULT_SIZE

typedef enum {
     freq_scheduler_error_ok = 0,       /**< No error */
     freq_scheduler_error_memory,       /**< Error allocating memory */
     freq_scheduler_error_invalid_path, /**< File system error */
     freq_scheduler_error_internal     /**< Unexpected error */
} FreqSchedulerError;

typedef struct {
     char *path;
     PageDB *page_db;
     TxnManager *txn_manager;

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

FreqSchedulerError
freq_scheduler_new(FreqScheduler **sch, PageDB *path);

FreqSchedulerError
freq_scheduler_load_mmap(FreqScheduler *sch, MMapArray *freqs);

FreqSchedulerError
freq_scheduler_request(FreqScheduler *sch,
                       size_t max_requests,
                       PageRequest **request);

FreqSchedulerError
freq_scheduler_add(FreqScheduler *sch, const CrawledPage *page);

void
freq_scheduler_delete(FreqScheduler *sch);

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_freq_scheduler_suite(size_t n_pages);
#endif

#endif // __FREQ_SCHEDULER_H__
