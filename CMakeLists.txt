cmake_minimum_required(VERSION 2.8)
project(stream-mmap)

set(WARNING_FLAGS 
  "-Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes -Wbad-function-cast -Wuninitialized -pedantic")

set(CMAKE_C_FLAGS 
  "-std=c99 -m64 -msse2 -pthread ${WARNING_FLAGS}")

set(CMAKE_C_FLAGS_RELEASE  "${CMAKE_C_FLAGS_RELEASE} -O3 -DDONT_CHECK_BOUNDS=1")

# External libraries
############################################################
add_library(xxhash SHARED
  lib/xxhash.c
)
include_directories(
  lib
)

add_definitions(-DMDB_MAXKEYSIZE=500)
add_library(lmdb SHARED
  lib/lmdb/mdb.c
  lib/lmdb/midl.c
)
include_directories(
  lib/lmdb
)

# Common code
############################################################
add_library(pagedb SHARED
  src/mmap_array.c
  src/page_db.c
  src/hits.c
  src/page_rank.c
  src/scheduler.c
  src/bf_scheduler.c
  src/util.c
)
include_directories(src)
target_link_libraries(pagedb lmdb xxhash)

# Benchmarks
#############################################################
add_library(hat-trie STATIC
  test/benchmark/hat-trie/ahtable.c
  test/benchmark/hat-trie/hat-trie.c
  test/benchmark/hat-trie/misc.c
  test/benchmark/hat-trie/murmurhash3.c
)
include_directories(
  test/benchmark/hat-trie
)
add_library(lz4 STATIC
  test/benchmark/lz4/lz4.c
  test/benchmark/lz4/lz4frame.c
  test/benchmark/lz4/lz4hc.c
)

include_directories(
  test/benchmark/lz4
)
target_link_libraries(lz4 xxhash)

add_library(lz4_link_stream STATIC
  test/benchmark/lz4_link_stream.c
)
include_directories(
  test/benchmark
)

add_executable(urldb test/benchmark/urldb.c)
add_executable(page_rank test/benchmark/page_rank.c)
add_executable(hits test/benchmark/hits.c)

target_link_libraries(lz4_link_stream lz4)
target_link_libraries(page_rank pagedb lz4_link_stream)
target_link_libraries(hits pagedb lz4_link_stream)
target_link_libraries(urldb hat-trie lmdb xxhash)

# Tests
#############################################################
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_definitions(-DTEST)
  add_library(CuTest SHARED
    test/CuTest.c
  )
  include_directories(test)
  target_link_libraries(pagedb CuTest)    
  add_executable(test test/test.c)
  target_link_libraries(test pagedb)
endif()