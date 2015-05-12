#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "CuTest.h"

#include "page_db.h"
#include "bf_scheduler.h"

int main(int argc, char **argv) {
     size_t n_pages = 0;
     char *end;
     switch (argc) {
     case 1:
          n_pages = 50000;
          break;
     case 2:
          n_pages = strtol(argv[1], &end, 10);
          if (*end != '\0') {
               fprintf(stderr, "Please enter a valid number as argument\n");
               goto on_error;
          }
          break;
     default:
          goto on_error;
     }

     CuString *output = CuStringNew();
     CuSuite *suite = CuSuiteNew();

     CuSuiteAddSuite(suite, test_page_db_suite(n_pages));
     CuSuiteAddSuite(suite, test_bf_scheduler_suite(n_pages));
     CuSuiteAddSuite(suite, test_util_suite());
     CuSuiteRun(suite);

     CuSuiteSummary(suite, output);
     CuSuiteDetails(suite, output);
     printf("%s\n", output->buffer);

     CuStringDelete(output);

     return 0;

on_error:
     fprintf(stderr, "\n");
     fprintf(stderr, "Usage: %s [n_pages]\n", argv[0]);
     return -1;
}
