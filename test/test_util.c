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

CuSuite *
test_util_suite() {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_varint_uint64);
     SUITE_ADD_TEST(suite, test_varint_int64);

     return suite;
}
