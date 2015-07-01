#include "CuTest.h"
static size_t test_n_pages = 50000;

static void
test_freq_scheduler_requests(CuTest *tc) {
     char test_dir_db[] = "test-freqs-XXXXXX";
     mkdtemp(test_dir_db);

     PageDB *db;
     int ret = page_db_new(&db, test_dir_db);
     CuAssert(tc,
	      db!=0? db->error->message: "NULL",
	      ret == 0);
     db->persist = 0;

     FreqScheduler *sch;
     ret = freq_scheduler_new(&sch, db);
     CuAssert(tc,
	      sch != 0? sch->error->message: "NULL",
	      ret == 0);
     sch->persist = 0;

     MMapArray *freqs;
     ret = mmap_array_new(&freqs,
			  build_path(test_dir_db, "freqs.bin"),
			  test_n_pages,
			  sizeof(PageFreq));
     CuAssert(tc,
	      freqs != 0? freqs->error->message: "NULL",
	      ret == 0);

     srand(42);
     size_t step = 0;
     PageFreq test[3];
     for (size_t i=0; i<test_n_pages; ++i) {

	  char url[100];
	  sprintf(url, "http://test_%zu", i);
	  CrawledPage *cp = crawled_page_new(url);
	  CuAssert(tc,
		   sch->error->message,
		   freq_scheduler_add(sch, cp) == 0);
	  crawled_page_delete(cp);

	  uint64_t hash = page_db_hash(url);
	  float freq;
	  switch (step) {
	  case 0:
	       test[step].hash = hash;
	       test[step].freq = freq = 0.1;
	       break;
	  case 1:
	       test[step].hash = hash;
	       test[step].freq = freq = 0.2;
	       break;
	  case 2:
	       test[step].hash = hash;
	       test[step].freq = freq = 0.4;
	       break;
	  default:
	       freq = 0.01*((float)rand())/((float)RAND_MAX);
	       break;
	  }
	  ++step;

	  PageFreq f = {
	       .hash = hash,
	       .freq = freq
	  };
	  CuAssert(tc,
		   freqs->error->message,
		   mmap_array_set(freqs, i, &f) == 0);
     }
     CuAssert(tc,
	      sch->error->message,
	      freq_scheduler_load(sch, freqs) == 0);

     time_t start = time(0);
     size_t n_reqs = 10;
     size_t total = 100*test_n_pages;
     for (size_t i=0; i<total; ++i) {
	  if (i % 10000 == 0) {
	       double delta = difftime(time(0), start);
	       if (delta > 0) {
		    printf("%10zuK/%zuK: %9zu pages/sec\n",
			   i/1000, total/1000, i/((size_t)delta));
	       }
	  }
	  PageRequest *reqs;
	  CuAssert(tc,
		   sch->error->message,
		   freq_scheduler_request(sch, n_reqs, &reqs) == 0);

	  for (size_t j=0; j<reqs->n_urls; ++j) {
	       CrawledPage *cp = crawled_page_new(reqs->urls[j]);
	       CuAssert(tc,
			sch->error->message,
			freq_scheduler_add(sch, cp) == 0);
	       crawled_page_delete(cp);
	  }
	  page_request_delete(reqs);
     }
     size_t n_tests = sizeof(test)/sizeof(PageFreq);
     float crawls[n_tests];
     for (size_t i=0; i<n_tests; ++i) {
	  PageInfo *pi;
	  ret = page_db_get_info(db, test[i].hash, &pi);
	  CuAssert(tc,
		   db->error->message,
		   ret == 0);
	  crawls[i] = (float)pi->n_crawls;

	  page_info_delete(pi);
     }
     freq_scheduler_delete(sch);
     page_db_delete(db);

     for (size_t i=0; i<(n_tests - 1); ++i) {
	  CuAssertDblEquals(tc,
			    1.0/test[i].freq,
			    crawls[i+1]/test[i+1].freq/crawls[i],
			    1e-2);
     }
}

CuSuite *
test_freq_scheduler_suite(size_t n_pages) {
     test_n_pages = n_pages/100;

     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_freq_scheduler_requests);
     return suite;
}
