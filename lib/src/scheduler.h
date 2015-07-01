#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

#include "page_db.h"

/** @addtogroup Scheduler
 *
 * Common interface for all schedulers.
 * @{
 */

/** Structure used as key inside the schedule database.
 *
 * In the schedule we want to keep an ordered heap of hashes, where we can pop
 * the highest scores whenever we want. In this sense it would have been more
 * natural to use as keys the scores (a single float) and the page hashes as the
 * values.  However, the scores may change as the page database changes and we
 * want a fast method to change the score associated to a hash. In particular we
 * want to make the change:
 *
 *   (score_old, hash) --> (score_new, hash)
 *
 * Since there can be lots of pages with the same score we move the hash into
 * the key, so that finding the previous (score_old, hash) can be done very fast
 */
typedef struct {
     float score;
     uint64_t hash;
} ScheduleKey;

/** Order keys from higher to lower
 *
 * First by score (descending) and then by hash.
 * */
int
schedule_entry_mdb_cmp_desc(const MDB_val *a, const MDB_val *b);

/** Order keys from lower to higher
 *
 * First by score (ascending) and then by hash.
 * */
int
schedule_entry_mdb_cmp_asc(const MDB_val *a, const MDB_val *b);

/** A request is an array of URLS */
typedef struct {
     char **urls;
     size_t n_urls;
} PageRequest;

/** Create a new request
 *
 * @param n_url Preallocate memory for this number of URLs
 * @return Pointer to the newly allocated request
 **/
PageRequest*
page_request_new(size_t n_urls);

/** Free memory */
void
page_request_delete(PageRequest *req);

/** Add the URL to the array of URLs inside the request
 *
 * It will make a new copy of the URL.
 *
 * @param req
 * @param url URL to add
 *
 * @return 0 if success, -1 if error.
 **/
int
page_request_add_url(PageRequest *req, const char *url);

/** Notify the scheduler of a new crawled page */
typedef int (SchedulerCrawledPage)(void *scheduler, const CrawledPage* cp);
/** Ask the scheduler for a new request */
typedef int (SchedulerNextRequests)(void *scheduler, PageRequest *requests);

/// @}

#endif // __SCHEDULER_H__
