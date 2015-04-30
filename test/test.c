#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "CuTest.h"

#include "page_db.h"
#include "bf_scheduler.h"
#include "test.h"

int main(int argc, char **argv) {
     TestOps ops;
     char *end;
     switch (argc) {
     case 1:
          ops = test_all;
          break;
     case 2:
          ops = strtol(argv[1], &end, 10);
          if (*end != '\0') {
               fprintf(stderr, "Please enter a valid number as argument\n");
               goto on_error;
          }
          break;
     default:
          goto on_error;
     }

     switch (ops) {
     case test_all:
          printf("Running all tests\n");
          break;
     case test_memcheck:
          printf("Running memcheck tests\n");
          break;
     default:
          fprintf(stderr, "Unknown test specification\n");
          goto on_error;
     }
     CuString *output = CuStringNew();
     CuSuite *suite = CuSuiteNew();

     CuSuiteAddSuite(suite, test_page_db_suite(ops));
     CuSuiteAddSuite(suite, test_bf_scheduler_suite(ops));
     CuSuiteAddSuite(suite, test_util_suite());
     CuSuiteRun(suite);

     CuSuiteSummary(suite, output);
     CuSuiteDetails(suite, output);
     printf("%s\n", output->buffer);

     CuStringDelete(output);

     return 0;

on_error:
     fprintf(stderr, "\n");
     fprintf(stderr, "Usage: %s [OP_NUMBER]\n", argv[0]);
     fprintf(stderr, "OP_NUMBER one of\n");
     fprintf(stderr, "    0: run all tests (default)\n");
     fprintf(stderr, "    1: run all tests for memory checking\n");
     return -1;
}
