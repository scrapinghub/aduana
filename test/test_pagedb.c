#include "CuTest.h"

/* Tests the loading/dumping of PageInfo from and into LMDB values */
void
test_page_info_serialization(CuTest *tc) {
     MDB_val val;
     PageInfo pi1 = {
          .url                 = "test_url_123",
          .first_crawl         = 123,
          .last_crawl          = 456,
          .n_changes           = 100,
          .n_crawls            = 20,
          .score               = 0.7,
          .content_hash_length = 8,
          .content_hash        = "1234567"
     };

     CuAssertTrue(tc, page_info_dump(&pi1, &val) == 0);
     CuAssertDblEquals(tc, 0.7, page_info_dump_get_score(&val), 1e-6);

     PageInfo *pi2 = page_info_load(&val);
     CuAssertPtrNotNull(tc, pi2);

     free(val.mv_data);

     CuAssertStrEquals(tc, pi1.url, pi2->url);
     CuAssertTrue(tc, pi1.first_crawl == pi2->first_crawl);
     CuAssertTrue(tc, pi1.last_crawl == pi2->last_crawl);
     CuAssertTrue(tc, pi1.n_changes == pi2->n_changes);
     CuAssertTrue(tc, pi1.n_crawls == pi2->n_crawls);
     CuAssertTrue(tc, pi1.score == pi2->score);
     CuAssertTrue(tc, pi1.content_hash_length == pi2->content_hash_length);
     CuAssertStrEquals(tc, pi1.content_hash, pi2->content_hash);

     page_info_delete(pi2);
}

/* Tests all the database operations on a very simple crawl of just two pages */
void
test_page_db_simple(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     int ret = page_db_new(&db, test_dir);
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              ret == 0);
     db->persist = 0;

     CrawledPage *cp1 = crawled_page_new("www.yahoo.com");
     crawled_page_add_link(cp1, "a", 0.1);
     crawled_page_add_link(cp1, "b", 0.2);
     crawled_page_add_link(cp1, "www.google.com", 0.3);
     crawled_page_set_hash64(cp1, 1000);
     cp1->score = 0.5;

     CrawledPage *cp2 = crawled_page_new("www.bing.com");
     crawled_page_add_link(cp2, "x", 1.1);
     crawled_page_add_link(cp2, "y", 1.2);
     crawled_page_set_hash64(cp2, 2000);
     cp2->score = 0.2;

     PageInfoList *pil;
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp1, &pil) == 0);
     page_info_list_delete(pil);

     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp2, &pil) == 0);
     page_info_list_delete(pil);

     crawled_page_set_hash64(cp2, 3000);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp2, &pil) == 0);
     page_info_list_delete(pil);

     MMapArray *scores = 0;
     CuAssert(tc,
              db->error->message,
              page_db_get_scores(db, &scores) == 0);

     size_t idx;
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash("www.yahoo.com"), &idx) == 0);
     CuAssertDblEquals(
          tc,
          0.5,
          *(float*)mmap_array_idx(scores, idx),
          1e-6);
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash("x"), &idx) == 0);
     CuAssertDblEquals(
          tc,
          1.1,
          *(float*)mmap_array_idx(scores, idx),
          1e-6);


     CuAssert(tc,
              scores->error->message,
              mmap_array_delete(scores) == 0);

     crawled_page_delete(cp1);
     crawled_page_delete(cp2);

     char pi_out[1000];
     char *print_pages[] = {"www.yahoo.com", "www.google.com", "www.bing.com"};
     for (size_t i=0; i<3; ++i) {
          PageInfo *pi;
          CuAssert(tc,
                   db->error->message,
                   page_db_get_info(db, page_db_hash(print_pages[i]), &pi) == 0);

          CuAssertPtrNotNull(tc, pi);

          switch(i) {
          case 0:
               CuAssertIntEquals(tc, 1, pi->n_crawls);
               CuAssertIntEquals(tc, 0, pi->n_changes);
               break;
          case 1:
               CuAssertIntEquals(tc, 0, pi->n_crawls);
               break;
          case 2:
               CuAssertIntEquals(tc, 2, pi->n_crawls);
               CuAssertIntEquals(tc, 1, pi->n_changes);
               break;
          }
          page_info_print(pi, pi_out);
          page_info_delete(pi);
/* show on screen the page info:
 *
 * Mon Apr  6 15:34:50 2015|Mon Apr  6 15:34:50 2015|1.00e+00|0.00e+00|www.yahoo.com
 * Thu Jan  1 01:00:00 1970|Thu Jan  1 01:00:00 1970|0.00e+00|0.00e+00|www.google.com
 * Mon Apr  6 15:34:50 2015|Mon Apr  6 15:34:50 2015|2.00e+00|1.00e+00|www.bing.com
 */
#if 0
          printf("%s\n", pi_out);
#endif
     }

     PageDBLinkStream *es;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&es, db) == 0);
     es->only_diff_domain = 0;

     if (es->state == stream_state_init) {
          Link link;
          int i=0;
          while (page_db_link_stream_next(es, &link) == stream_state_next) {
               switch(i++) {
               case 0:
                    CuAssertIntEquals(tc, 0, link.from);
                    CuAssertIntEquals(tc, 1, link.to);
                    break;
               case 1:
                    CuAssertIntEquals(tc, 0, link.from);
                    CuAssertIntEquals(tc, 2, link.to);
                    break;
               case 2:
                    CuAssertIntEquals(tc, 0, link.from);
                    CuAssertIntEquals(tc, 3, link.to);
                    break;
               case 3:
                    CuAssertIntEquals(tc, 4, link.from);
                    CuAssertIntEquals(tc, 5, link.to);
                    break;
               case 4:
                    CuAssertIntEquals(tc, 4, link.from);
                    CuAssertIntEquals(tc, 6, link.to);
                    break;
               default:
                    CuFail(tc, "too many links");
                    break;
               }
          }
          CuAssertTrue(tc, es->state != stream_state_error);
     }
     page_db_link_stream_delete(es);

     page_db_delete(db);
}

static size_t test_n_pages = 50000;

static void
test_page_db_crawl(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     int ret = page_db_new(&db, test_dir);
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              ret == 0);
     db->persist = 0;

     const size_t n_links = 10;

     LinkInfo links[n_links + 1];
     for (size_t j=0; j<=n_links; ++j) {
          sprintf(links[j].url = malloc(100),
                  "http://test_domain_%zu.org/test_url_%zu", j%100, j);
          links[j].score = j;
     }
     time_t start = time(0);
     printf("%s: \n", __func__);
     for (size_t i=0; i<test_n_pages; ++i) {
#if 1
          if (i % 10000 == 0) {
               double delta = difftime(time(0), start);
               if (delta > 0) {
                    printf("%10zuK/%zuK: %9zu pages/sec\n",
                           i/1000, test_n_pages/1000, i/((size_t)delta));
               }
          }
#endif
          free(links[0].url);
          for (size_t j=0; j<n_links; ++j)
               links[j] = links[j+1];
          sprintf(links[n_links].url = malloc(50), "test_url_%zu", i + n_links);
          links[n_links].score = i;

          CrawledPage *cp = crawled_page_new(links[0].url);
          for (size_t j=1; j<=n_links; ++j)
               crawled_page_add_link(cp, links[j].url, 0.5);

          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }
     for (size_t j=0; j<=n_links; ++j)
          free(links[j].url);

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);
     st->only_diff_domain = 0;

     Hits *hits;
     ret = hits_new(&hits, test_dir, test_n_pages);
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              ret == 0);

     hits->precision = 1e-3;
     HitsError hits_err = hits_compute(hits,
                                       st,
                                       page_db_link_stream_next,
                                       page_db_link_stream_reset);
     if (hits_err == hits_error_precision)
          hits_err = 0;

     CuAssert(tc, hits->error->message, hits_err == 0);

     CuAssert(tc,
              hits->error->message,
              hits_delete(hits) == 0);

     page_db_link_stream_delete(st);

     page_db_delete(db);
}

void
test_hashidx_stream(CuTest *tc) {
     char test_dir[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     int ret = page_db_new(&db, test_dir);
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              ret == 0);
     db->persist = 0;

     CrawledPage *cp = crawled_page_new("1");
     crawled_page_add_link(cp, "a", 0);
     crawled_page_add_link(cp, "b", 0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);

     cp = crawled_page_new("2");
     crawled_page_add_link(cp, "c", 0);
     crawled_page_add_link(cp, "d", 0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);

     HashIdxStream *stream;
     CuAssert(tc,
              db->error->message,
              hashidx_stream_new(&stream, db) == 0);

     CuAssert(tc,
              "stream was not initialized",
              stream->state == stream_state_init);

     uint64_t hash;
     size_t idx;

     uint64_t expected_hash[] = {
          page_db_hash("1"),
          page_db_hash("a"),
          page_db_hash("b"),
          page_db_hash("2"),
          page_db_hash("c"),
          page_db_hash("d")
     };

     for (int i=0; i<6; ++i) {
          CuAssert(tc,
                   "stream element expected",
                   hashidx_stream_next(stream, &hash, &idx) == stream_state_next);
          if (idx > 5)
               CuFail(tc, "unexpected index");
          CuAssert(tc,
                   "mismatch between index and hash",
                   hash == expected_hash[idx]);
     }
     hashidx_stream_delete(stream);

     page_db_delete(db);
}

void
test_link_stream(CuTest *tc) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     int ret = page_db_new(&db, test_dir);
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              ret == 0);
     db->persist = 0;

     /* +----------------+
      * |                |
      * a1 --> a2        |
      * |      ^         |
      * |      |         v
      * +----> b1 -----> b2
      */
     const char *url_a1 = "http://test_a.org/1";
     const char *url_a2 = "http://test_a.org/2";
     const char *url_b1 = "http://test_b.org/1";
     const char *url_b2 = "http://test_b.org/2";
     CrawledPage *cp = crawled_page_new(url_a1);
     crawled_page_add_link(cp, url_a2, 1.0);
     crawled_page_add_link(cp, url_b1, 1.0);
     crawled_page_add_link(cp, url_b2, 1.0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);

     cp = crawled_page_new(url_b1);
     crawled_page_add_link(cp, url_b2, 1.0);
     crawled_page_add_link(cp, url_a2, 1.0);
     CuAssert(tc,
              db->error->message,
              page_db_add(db, cp, 0) == 0);
     crawled_page_delete(cp);
     uint64_t idx[4];
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash(url_a1), idx + 0) == 0);
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash(url_a2), idx + 1) == 0);
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash(url_b1), idx + 2) == 0);
     CuAssert(tc,
              db->error->message,
              page_db_get_idx(db, page_db_hash(url_b2), idx + 3) == 0);

     Link links_diff[] = {
          {.from = idx[0], .to = idx[2]}, // a1 -> b1
          {.from = idx[0], .to = idx[3]}, // a1 -> b2
          {.from = idx[2], .to = idx[1]}  // b1 -> a2
     };
     Link links_same[] = {
          {.from = idx[0], .to = idx[1]}, // a1 -> a2
          {.from = idx[2], .to = idx[3]}, // b1 -> b2
     };
     PageDBLinkStream *st;
     Link link;
     size_t n_links;

     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);
     st->only_diff_domain = 1;
     n_links = 0;
     while (page_db_link_stream_next(st, &link) == stream_state_next) {
          int found = 0;
          for (int i=0; i<3; ++i)
               if ((links_diff[i].from == link.from) &&
                   (links_diff[i].to == link.to))
                    found = 1;
          CuAssertTrue(tc, found);
          ++n_links;
     }
     CuAssertIntEquals(tc, 3, n_links);
     page_db_link_stream_delete(st);

     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);
     st->only_diff_domain = 0;
     n_links = 0;
     while (page_db_link_stream_next(st, &link) == stream_state_next) {
          int found = 0;
          for (int i=0; i<3; ++i)
               if (((links_diff[i].from == link.from) &&
                    (links_diff[i].to == link.to)) ||
                   ((links_same[i].from == link.from) &&
                    (links_same[i].to == link.to)))
                    found = 1;
          CuAssertTrue(tc, found);
          ++n_links;
     }
     CuAssertIntEquals(tc, 5, n_links);
     page_db_link_stream_delete(st);
     page_db_delete(db);
}

CuSuite *
test_page_db_suite(size_t n_pages) {
     test_n_pages = n_pages;

     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_page_info_serialization);
     SUITE_ADD_TEST(suite, test_page_db_simple);
     SUITE_ADD_TEST(suite, test_page_db_crawl);
     SUITE_ADD_TEST(suite, test_hashidx_stream);
     SUITE_ADD_TEST(suite, test_link_stream);

     return suite;
}
