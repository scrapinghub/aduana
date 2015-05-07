#include "CuTest.h"
#include "test.h"

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
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
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

static void
test_page_db_crawl(CuTest *tc, size_t n_pages) {
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     const size_t n_links = 10;

     LinkInfo links[n_links + 1];
     for (size_t j=0; j<=n_links; ++j) {
          sprintf(links[j].url = malloc(50), "test_url_%zu", j);
          links[j].score = j;
     }
     time_t start = time(0);
     printf("%s: \n", __func__);
     for (size_t i=0; i<n_pages; ++i) {
#if 1
          if (i % 10000 == 0) {
               double delta = difftime(time(0), start);
               if (delta > 0) {
                    printf("%10zuK/%zuM: %9zu pages/sec\n",
                           i/1000, n_pages/1000000, i/((size_t)delta));
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

     Hits *hits;
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              hits_new(&hits, test_dir, n_pages) == 0);

     hits->precision = 1e-3;
     CuAssert(tc,
              hits->error->message,
              hits_compute(hits,
                           st,
                           page_db_link_stream_next,
                           page_db_link_stream_reset) == 0);

     CuAssert(tc,
              hits->error->message,
              hits_delete(hits) == 0);

     page_db_link_stream_delete(st);

     page_db_delete(db);
}

/* Tests the typical database operations on a moderate crawl of 10M pages */
void
test_page_db_large(CuTest *tc) {
     test_page_db_crawl(tc, 10000000);
}

/* Tests the typical database operations on a big crawl of 100M pages */
void
test_page_db_very_large(CuTest *tc) {
     test_page_db_crawl(tc, 100000000);
}

/* Checks the accuracy of the HITS computation */
void
test_page_db_hits(CuTest *tc) {
     /* Compute the HITS score of the following graph
      *        +-->2---+
      *        |   |   |
      *        |   v   v
      *        1-->5<--3
      *        ^   ^   |
      *        |   |   |
      *        +---4<--+
      *
      * The link matrix L[i,j], where L[i,j] = 1 means 'i' links to 'j' is:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 0 0 0 0 0 |
      *        +-         -+
      */
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
          links_1, links_2, links_3, links_4, 0
     };
     int n_links[5] = {2, 2, 2, 2, 0};

     for (int i=0; i<5; ++i) {
          CrawledPage *cp = crawled_page_new(urls[i]);
          for (int j=0; j<n_links[i]; ++j)
               crawled_page_add_link(cp, links[i][j].url, links[i][j].score);
          cp->score = i/5.0;
          crawled_page_set_hash64(cp, i);

          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);

     Hits *hits;
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              hits_new(&hits, test_dir, 5) == 0);

     hits->precision = 1e-8;
     CuAssert(tc,
              hits->error->message,
              hits_compute(hits,
                           st,
                           page_db_link_stream_next,
                           page_db_link_stream_reset) == 0);
     page_db_link_stream_delete(st);

     uint64_t idx;
     float *h_score;
     float *a_score;
     float h_scores[5] = {0.250, 0.250, 0.250, 0.250, 0.000};
     float a_scores[5] = {0.125, 0.125, 0.125, 0.125, 0.500};

     for (int i=0; i<5; ++i) {
          CuAssert(tc,
                   db->error->message,
                   page_db_get_idx(db, page_db_hash(urls[i]), &idx) == 0);

          CuAssertPtrNotNull(tc,
                             h_score = mmap_array_idx(hits->h1, idx));
          CuAssertPtrNotNull(tc,
                             a_score = mmap_array_idx(hits->a1, idx));

          CuAssertDblEquals(tc, h_scores[i], *h_score, 1e-6);
          CuAssertDblEquals(tc, a_scores[i], *a_score, 1e-6);
     }
     CuAssert(tc,
              hits->error->message,
              hits_delete(hits) == 0);

     page_db_delete(db);
}

/* Checks the accuracy of the PageRank computation */
void
test_page_db_page_rank(CuTest *tc) {
     /* Compute the PageRank score of the following graph
      *        +-->2---+
      *        |   |   |
      *        |   v   v
      *        1-->5<--3
      *        ^   ^   |
      *        |   |   |
      *        +---4<--+
      *
      * The link matrix L[i,j], where L[i,j] = 1 means 'i' links to 'j' is:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 0 0 0 0 0 |
      *        +-         -+
      *
      * Since page "5" has no outbound links, it is assumed it links to every other page:
      *
      *        +-         -+
      *        | 0 1 0 0 1 |
      *        | 0 0 1 0 1 |
      *    L = | 0 0 0 1 1 |
      *        | 1 0 0 0 1 |
      *        | 1 1 1 1 1 |
      *        +-         -+
      *
      * The out degree is:
      *
      *    deg = {2, 2, 2, 2, 5}
      *
      * Dividing each row with the out degree and transposing we get the matrix:
      *
      *    M[i, j] = L[j, i]/deg[j]
      *
      * we get:
      *
      *        +-                   -+
      *        | 0   0   0   0.5 0.2 |
      *        | 0.5 0   0   0   0.2 |
      *    M = | 0   0.5 0   0   0.2 |
      *        | 0   0   0.5 0   0.2 |
      *        | 0.5 0.5 0.5 0.5 0.2 |
      *        +-                   -+
      *
      * If 'd' is the damping then the PageRank 'PR' is:
      *
      *        1 - d
      *   PR = ----- + (d * M)*PR
      *          N
      *
      * For d=0.85 the numerical solution is:
      *
      *   PR(1) = PR(2) = PR(3) = PR(4) = 0.15936255
      *   PR(5) = 0.3625498
      */
     char test_dir[] = "test-pagedb-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
     db->persist = 0;

     char *urls[5] = {"1", "2", "3", "4", "5" };
     LinkInfo links_1[] = {{"2", 0.1}, {"5", 0.1}};
     LinkInfo links_2[] = {{"3", 0.1}, {"5", 0.1}};
     LinkInfo links_3[] = {{"4", 0.1}, {"5", 0.1}};
     LinkInfo links_4[] = {{"1", 0.1}, {"5", 0.1}};
     LinkInfo *links[5] = {
          links_1, links_2, links_3, links_4, 0
     };

     int n_links[5] = {2, 2, 2, 2, 0};

     for (int i=0; i<5; ++i) {
          CrawledPage *cp = crawled_page_new(urls[i]);
          for (int j=0; j<n_links[i]; ++j)
               crawled_page_add_link(cp, links[i][j].url, links[i][j].score);
          cp->score = i/5.0;
          crawled_page_set_hash64(cp, i);
          PageInfoList *pil;
          CuAssert(tc,
                   db->error->message,
                   page_db_add(db, cp, &pil) == 0);
          page_info_list_delete(pil);
          crawled_page_delete(cp);
     }

     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);

     PageRank *pr;
     CuAssert(tc,
              pr!=0? pr->error->message: "NULL",
              page_rank_new(&pr, test_dir, 5) == 0);

     pr->precision = 1e-6;
     CuAssert(tc,
              pr->error->message,
              page_rank_compute(pr,
                                st,
                                page_db_link_stream_next,
                                page_db_link_stream_reset) == 0);
     page_db_link_stream_delete(st);

     uint64_t idx;
     float *score;

     float scores[5] =  {0.15936255,  0.15936255,  0.15936255,  0.15936255,  0.3625498};
     for (int i=0; i<5; ++i) {
          CuAssert(tc,
                   db->error->message,
                   page_db_get_idx(db, page_db_hash(urls[i]), &idx) == 0);

          CuAssertPtrNotNull(tc,
                             score = mmap_array_idx(pr->value1, idx));

          CuAssertDblEquals(tc, scores[i], *score, 1e-6);
     }
     CuAssert(tc,
              pr->error->message,
              page_rank_delete(pr) == 0);

     page_db_delete(db);
}

void
test_hashidx_stream(CuTest *tc) {
     char test_dir[] = "test-bfs-XXXXXX";
     mkdtemp(test_dir);

     PageDB *db;
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              page_db_new(&db, test_dir) == 0);
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

CuSuite *
test_page_db_suite(TestOps ops) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_page_info_serialization);
     SUITE_ADD_TEST(suite, test_page_db_simple);
     SUITE_ADD_TEST(suite, test_page_db_hits);
     SUITE_ADD_TEST(suite, test_page_db_page_rank);
     if (ops == test_all || ops == test_large)
          SUITE_ADD_TEST(suite, test_page_db_large);
     if (ops == test_all)
          SUITE_ADD_TEST(suite, test_page_db_very_large);

     SUITE_ADD_TEST(suite, test_hashidx_stream);

     return suite;
}
