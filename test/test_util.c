#include "CuTest.h"

void
test_varint_uint64(CuTest *tc) {
     uint64_t test[] = {
          1000000, 10000, 100, 1, 0
     };
     size_t test_length = sizeof(test)/sizeof(uint64_t);

     uint8_t buf[10*test_length];
     uint8_t *next = buf;
     for (size_t i=0; i<test_length; ++i)
          next = varint_encode_uint64(test[i], next);

     uint8_t read = 0;
     next = buf;
     for (size_t i=0; i<test_length; ++i) {
          CuAssertIntEquals(tc,
                            test[i],
                            varint_decode_uint64(next, &read));
          next += read;
     }


}
void
test_varint_int64(CuTest *tc) {
     int64_t test[] = {
          -1000000, 10000, -100, 1, 0
     };
     size_t test_length = sizeof(test)/sizeof(uint64_t);

     uint8_t buf[10*test_length];
     uint8_t *next = buf;
     for (size_t i=0; i<test_length; ++i)
          next = varint_encode_int64(test[i], next);

     uint8_t read = 0;
     next = buf;
     for (size_t i=0; i<test_length; ++i) {
          CuAssertIntEquals(tc,
                            test[i],
                            varint_decode_int64(next, &read));
          next += read;
     }
}

void
test_url_domain(CuTest *tc) {
     char *url[] = {
          "https://fr.m.wikipedia.org/wiki/Jeudi",
          "https://apps.hclib.org/catalog/results.cfm?fq=author_f%3AUnited+States.+Congress.+Senate",
          "http://www.jstor.org:1000/stable/143091",
          "http://mlb.mlb.com/mlb/history/postseason/mlb_lcs.jsp?feature=mvp",
          "https://inclass.kaggle.com/c/adcg-ss14-challenge-03/forums/t/8293/jumbled-leaderboard/47136",
          "http://foo:xxyy@blabla.org"
     };
     char *domain[] = {
          "fr.m.wikipedia.org",
          "apps.hclib.org",
          "www.jstor.org",
          "mlb.mlb.com",
          "inclass.kaggle.com",
          "blabla.org"
     };
     int n_url = sizeof(url)/sizeof(char*);
     int start[n_url];
     int end[n_url];
     start[0] = start[1] = start[4] = strlen("https://");
     start[2] = start[3] = strlen("http://");
     start[5] = strlen("http://foo:xxyy@");

     for (int i=0; i<n_url; ++i)
          end[i] = start[i] + strlen(domain[i]) - 1;

     int s, e;
     for (int i=0; i<n_url; ++i) {
          CuAssert(tc,
                   "Failed to parse URL",
                   url_domain(url[i], &s, &e) == 0);
          CuAssertIntEquals(tc, start[i], s);
          CuAssertIntEquals(tc, end[i], e);
     }
     CuAssert(tc,
              "Parsed incorrect URL",
              url_domain("xxxxx", &s, &e) == -1);
}

CuSuite *
test_util_suite() {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_varint_uint64);
     SUITE_ADD_TEST(suite, test_varint_int64);
     SUITE_ADD_TEST(suite, test_url_domain);
     return suite;
}
