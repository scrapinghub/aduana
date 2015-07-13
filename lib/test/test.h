#ifndef __TEST_H__
#define __TEST_H__

#include "CuTest.h"

#define CHECK_DELETE(tc, msg, cmd) do {\
     int __ret = (cmd);\
     CuAssert(tc, __ret? msg: "", __ret == 0);\
} while (0)

#endif
