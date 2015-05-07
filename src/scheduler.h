#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

/** @addtogroup Scheduler
 *
 * Common interface for all schedulers.
 * @{
 */

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
