#include "CuTest.h"

#include "pagedb.h"

int main(void) {
     CuString *output = CuStringNew();
     CuSuite *suite = CuSuiteNew();
     
     CuSuiteAddSuite(suite, test_page_db_suite());
     
     CuSuiteRun(suite);

     CuSuiteSummary(suite, output);
     CuSuiteDetails(suite, output);
     printf("%s\n", output->buffer);
     
     return 0;
}
