#include "CuTest.h"

void
test_domain_temp(CuTest *tc) {
     printf("%s\n", __func__);
     DomainTemp *dh = domain_temp_new(10, 60.0);
     CuAssertPtrNotNull(tc, dh);

     domain_temp_heat(dh, 1);
     domain_temp_heat(dh, 1);
     domain_temp_heat(dh, 2);
     domain_temp_heat(dh, 1000);

     CuAssertDblEquals(tc, 2.0, domain_temp_get(dh, 1), 1e-6);
     CuAssertDblEquals(tc, 1.0, domain_temp_get(dh, 2), 1e-6);
     CuAssertDblEquals(tc, 1.0, domain_temp_get(dh, 1000), 1e-6);
     CuAssertDblEquals(tc, 0.0, domain_temp_get(dh, 3), 1e-6);

     domain_temp_update(dh, 1.0);

     float k = 1.0 - 1.0/60.0;
     CuAssertDblEquals(tc, 2.0*k, domain_temp_get(dh, 1), 1e-6);
     CuAssertDblEquals(tc, 1.0*k, domain_temp_get(dh, 2), 1e-6);
     CuAssertDblEquals(tc, 1.0*k, domain_temp_get(dh, 1000), 1e-6);
     CuAssertDblEquals(tc, 0.0, domain_temp_get(dh, 3), 1e-6);

     domain_temp_delete(dh);
}

CuSuite *
test_domain_temp_suite(void) {
     CuSuite *suite = CuSuiteNew();
     SUITE_ADD_TEST(suite, test_domain_temp);

     return suite;
}
