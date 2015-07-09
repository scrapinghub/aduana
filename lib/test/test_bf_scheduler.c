#include "CuTest.h"

void
test_bf_scheduler_requests(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     int ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 0;

     BFScheduler *sch;
     ret = bf_scheduler_new(&sch, db, 0);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 0;

     /* Make the link structure
      *
      *      0.0    1.0    0.1    0.5    0.4
      *   1 ---> 2 ---->4----->5------>8----->9
      *   |             |      |       |
      *   |      +------+   +--+--+    |0.2
      *   |      | 0.2   0.0|  0.5|    |
      *   | 0.1  v          v     v    |
      *   +----> 3          6     7<---+
      *
      */
     const size_t n_pages = 6;
     CrawledPage *cp;
     CrawledPage **crawl = calloc(n_pages, sizeof(*crawl));
     cp = crawl[0] = crawled_page_new("1");
     crawled_page_add_link(cp, "2", 0.0);
     crawled_page_add_link(cp, "3", 0.1);

     cp = crawl[1] = crawled_page_new("2");
     crawled_page_add_link(cp, "4", 1.0);

     cp = crawl[2] = crawled_page_new("4");
     crawled_page_add_link(cp, "3", 0.2);
     crawled_page_add_link(cp, "5", 0.1);

     cp = crawl[3] = crawled_page_new("5");
     crawled_page_add_link(cp, "6", 0.0);
     crawled_page_add_link(cp, "7", 0.5);
     crawled_page_add_link(cp, "8", 0.5);

     cp = crawl[4] = crawled_page_new("8");
     crawled_page_add_link(cp, "7", 0.2);
     crawled_page_add_link(cp, "9", 0.4);

     cp = crawl[5] = crawled_page_new("7");


     /* We have the following schedule after crawling pages 1, 2, 4, 5, 8 and 7
      * Note:
      *    - x: the page has been crawled
      *    - n: the link has not been added because it was already present
      *
      *     1           2           4           5           8           7
      * page score  page score  page score  page score  page score  page score
      * ---- -----  ---- -----  ---- -----  ---- -----  ---- -----  ---- -----
      * 3    0.1    4    1.0    4    1.0 x  4    1.0 x  4    1.0 x  4    1.0 x
      * 2    0.0    3    0.1    3    0.2 n  7    0.5    7    0.5    7    0.5 x
      *             2    0.0 x  3    0.1    8    0.5    8    0.5 x  8    0.5 x
      *                         5    0.1    3    0.2 n  9    0.4    9    0.4
      *                         2    0.0 x  3    0.1    3    0.2 n  3    0.2 n
      *                                     5    0.1 x  7    0.2 n  7    0.2 n
      *                                     6    0.0    3    0.1    3    0.1
					    2    0.0 x  5    0.1 x  5    0.1 x
      *                                                 6    0.0    6    0.0
      *                                                 2    0.0 x  2    0.0 x
      *
      */
     for (size_t i=0; i<n_pages; ++i) {
	  CuAssert(tc,
		   sch->error->message,
		   bf_scheduler_add(sch, crawl[i]) == 0);
	  crawled_page_delete(crawl[i]);
     }


     /* Requests should return:
      *
      * page score
      * ---- -----
      * 9    0.4
      * 3    0.1
      * 6    0.0
      */
     PageRequest *req;
     CuAssert(tc,
	      sch->error->message,
	      bf_scheduler_request(sch, 2, &req) == 0);

     CuAssertStrEquals(tc, "9", req->urls[0]);
     CuAssertStrEquals(tc, "3", req->urls[1]);
     page_request_delete(req);

     CuAssert(tc,
	      sch->error->message,
	      bf_scheduler_request(sch, 4, &req) == 0);

     CuAssertStrEquals(tc, "6", req->urls[0]);
     CuAssert(tc, "too many requests returned", req->n_urls == 1);
     page_request_delete(req);

     bf_scheduler_delete(sch);
     page_db_delete(db);

     free(crawl);
}

static void
test_bf_scheduler_crawl(CuTest *tc,
			BFScheduler *sch,
			size_t n_pages) {
     const size_t n_links = 10;

     LinkInfo links[n_links + 1];
     for (size_t j=0; j<=n_links; ++j) {
	  sprintf(links[j].url = malloc(50), "test_url_%zu", j);
	  links[j].score = ((float)j)/((float)n_pages);
     }

     time_t start = time(0);
     printf("%s: \n", __func__);
     for (size_t i=0; i<n_pages; ++i) {
#if 1
	  if (i % 10000 == 0) {
	       double delta = difftime(time(0), start);
	       if (delta > 0) {
		    printf("%10zuK/%zuK: %9zu pages/sec\n",
			   i/1000, n_pages/1000, i/((size_t)delta));
	       }
	  }
#endif
	  free(links[0].url);
	  for (size_t j=0; j<n_links; ++j)
	       links[j] = links[j+1];
	  sprintf(links[n_links].url = malloc(50), "test_url_%zu", i + n_links);
	  links[n_links].score = ((float)i)/((float)n_pages);

	  CrawledPage *cp = crawled_page_new(links[0].url);
	  for (size_t j=1; j<=n_links; ++j)
	       crawled_page_add_link(cp, links[j].url, 0.5);

	  CuAssert(tc,
		   sch->error->message,
		   bf_scheduler_add(sch, cp) == 0);
	  crawled_page_delete(cp);

	  if (i % 10 == 0) {
	       PageRequest *req;
	       CuAssert(tc,
			sch->error->message,
			bf_scheduler_request(sch, 10, &req) == 0);
	       page_request_delete(req);
	  }
     }
     for (size_t j=0; j<=n_links; ++j)
	  free(links[j].url);
}

static size_t test_n_pages = 50000;

/* Tests the typical database operations on a moderate crawl of 10M pages */
static void
test_bf_scheduler_page_rank(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     int ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 0;

     BFScheduler *sch;
     ret = bf_scheduler_new(&sch, db, 0);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 0;

     PageRankScorer *scorer;
     ret = page_rank_scorer_new(&scorer, db);
     CuAssert(tc,
	      scorer != 0? scorer->error->message: "NULL",
	      ret == 0);

     page_rank_scorer_set_use_content_scores(scorer, 1);
     page_rank_scorer_setup(scorer, sch->scorer);
     bf_scheduler_update_start(sch);

     test_bf_scheduler_crawl(tc, sch, test_n_pages);

     bf_scheduler_update_stop(sch);
     bf_scheduler_delete(sch);
     page_rank_scorer_delete(scorer);
     page_db_delete(db);
}

/* Tests the typical database operations on a moderate crawl of 10M pages */
static void
test_bf_scheduler_hits(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     int ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 0;

     BFScheduler *sch;
     ret = bf_scheduler_new(&sch, db, 0);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 0;

     HitsScorer *scorer;
     ret = hits_scorer_new(&scorer, db);
     CuAssert(tc,
	      scorer != 0? scorer->error->message: "NULL",
	      ret == 0);

     hits_scorer_set_use_content_scores(scorer, 1);
     hits_scorer_setup(scorer, sch->scorer);
     bf_scheduler_update_start(sch);

     test_bf_scheduler_crawl(tc, sch, test_n_pages);

     bf_scheduler_update_stop(sch);
     bf_scheduler_delete(sch);
     hits_scorer_delete(scorer);
     page_db_delete(db);
}

static void
test_bf_scheduler_restart(CuTest *tc) {
     char test_dir_db[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     int ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 1;

     BFScheduler *sch;
     ret = bf_scheduler_new(&sch, db, 0);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 1;

     CrawledPage *cp = crawled_page_new("http://www.foobar.com/spam");
     char link[1000];
     for (int i=0; i<100; ++i) {
	  sprintf(link, "http://www.foobar.com/page_%d", i);
	  crawled_page_add_link(cp, link, ((float)i)/100.0);
     }

     CuAssert(tc,
	      sch->error->message,
	      bf_scheduler_add(sch, cp) == 0);
     crawled_page_delete(cp);

     PageRequest *reqs;
     CuAssert(tc,
	      sch->error->message,
	      bf_scheduler_request(sch, 25, &reqs) == 0);
     for (int i=0; i<25; ++i) {
	  sprintf(link, "http://www.foobar.com/page_%d", 99 - i);
	  CuAssertStrEquals(tc, link, reqs->urls[i]);
     }
     page_request_delete(reqs);
     bf_scheduler_delete(sch);
     page_db_delete(db);

     // open again
     ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 0;

     ret = bf_scheduler_new(&sch, db, 0);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 0;

     CuAssert(tc,
	      sch->error->message,
	      bf_scheduler_request(sch, 25, &reqs) == 0);
     for (int i=0; i<25; ++i) {
	  sprintf(link, "http://www.foobar.com/page_%d", 74 - i);
	  CuAssertStrEquals(tc, link, reqs->urls[i]);
     }
     page_request_delete(reqs);
     bf_scheduler_delete(sch);
     page_db_delete(db);
}

CuSuite *
test_bf_scheduler_suite(size_t n_pages) {
     test_n_pages = n_pages;

     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_bf_scheduler_requests);
     SUITE_ADD_TEST(suite, test_bf_scheduler_restart);
     SUITE_ADD_TEST(suite, test_bf_scheduler_page_rank);
     SUITE_ADD_TEST(suite, test_bf_scheduler_hits);

     return suite;
}
