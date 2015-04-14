#ifndef __SCHEDULER_H__
#define __SCHEDULER_H__

/** A request is an array of URLS */
typedef struct {
     char **urls;
     size_t n_urls;
} PageRequest;

PageRequest*
page_request_new(size_t n_urls);

void
page_request_delete(PageRequest *req);

int
page_request_add_url(PageRequest *req, const char *url);

typedef int (SchedulerCrawledPage)(void *scheduler, const CrawledPage* cp);
typedef int (SchedulerNextRequests)(void *scheduler, PageRequest *requests);

#endif // __SCHEDULER_H__
