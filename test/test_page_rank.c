#include "CuTest.h"

#include "page_db.h"

/* Checks the accuracy of the PageRank computation */
void
test_page_rank(CuTest *tc) {
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

     // Without content scores
     // ------------------------------------------------------------------------
     PageDBLinkStream *st;
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);
     st->only_diff_domain = 0;

     PageRank *pr;
     ret = page_rank_new(&pr, test_dir, 5);
     CuAssert(tc,
              pr!=0? pr->error->message: "NULL",
              ret == 0);

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

     // With content scores
     // ------------------------------------------------------------------------
     CuAssert(tc,
              db->error->message,
              page_db_link_stream_new(&st, db) == 0);
     st->only_diff_domain = 0;

     ret = page_rank_new(&pr, test_dir, 5);
     CuAssert(tc,
              pr!=0? pr->error->message: "NULL",
              ret == 0);

     pr->precision = 1e-6;
     pr->damping = 0.0;

     CuAssert(tc,
              db->error->message,
              page_db_get_scores(db, &pr->scores) == 0);

     CuAssert(tc,
              pr->error->message,
              page_rank_compute(pr,
                                st,
                                page_db_link_stream_next,
                                page_db_link_stream_reset) == 0);
     page_db_link_stream_delete(st);

     for (int i=0; i<5; ++i)
          scores[i] = *((float*)mmap_array_idx(pr->scores, i));

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

CuSuite *
test_page_rank_suite() {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_page_rank);
     return suite;
}
