#ifndef __BF_SCHEDULER_H__
#define __BF_SCHEDULER_H__

#include "page_db.h"
#include "scheduler.h"
#include "scorer.h"

#define BF_SCHEDULER_MAX_ERROR_LENGTH 10000
#define BF_SCHEDULER_DEFAULT_SIZE PAGE_DB_DEFAULT_SIZE

typedef enum {
     bf_scheduler_error_ok = 0,       /**< No error */
     bf_scheduler_error_memory,       /**< Error allocating memory */
     bf_scheduler_error_invalid_path, /**< File system error */
     bf_scheduler_error_internal      /**< Unexpected error */
} BFSchedulerError;

typedef struct {
     PageDB *page_db;
     Scorer *scorer;

     MDB_env *env;
     char *path;

     Error error;
} BFScheduler;


BFSchedulerError
bf_scheduler_new(BFScheduler **sch, PageDB *db, Scorer *scorer);

BFSchedulerError
bf_scheduler_add(BFScheduler *sch, const CrawledPage *page);

BFSchedulerError
bf_scheduler_request(BFScheduler *sch, size_t n_pages, PageRequest **request);

void
bf_scheduler_delete(BFScheduler *sch);

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_bf_scheduler_suite(void);
#endif

#endif // __BF_SCHEDULER_H__
