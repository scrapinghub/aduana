#include "CuTest.h"

#include "test.h"
#include "page_db.h"

/* Checks the accuracy of the HITS computation */
void
test_hits(CuTest *tc) {
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
     int ret = page_db_new(&db, test_dir);
     CuAssert(tc,
              db!=0? db->error->message: "NULL",
              ret == 0);
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
     st->only_diff_domain = 0;

     Hits *hits;
     ret = hits_new(&hits, test_dir, 5);
     CuAssert(tc,
              hits!=0? hits->error->message: "NULL",
              ret == 0);

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
     CHECK_DELETE(tc, hits->error->message, hits_delete(hits));

     page_db_delete(db);
}

CuSuite *
test_hits_suite() {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_hits);
     
     return suite;
}
