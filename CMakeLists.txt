cmake_minimum_required(VERSION 2.8)
project(stream-mmap)

set(WARNING_FLAGS 
  "-Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes -Wbad-function-cast -Wuninitialized -pedantic")

set(CMAKE_C_FLAGS 
  "-std=c99 -m64 -msse2 -pthread ${WARNING_FLAGS}")

set(CMAKE_C_FLAGS_RELEASE  "${CMAKE_C_FLAGS_RELEASE} -O3")

# External libraries
############################################################
add_library(xxhash STATIC
  lib/xxhash.c
)
include_directories(
  lib
)

add_definitions(-DMDB_MAXKEYSIZE=500)
add_library(lmdb STATIC
  lib/lmdb/mdb.c
  lib/lmdb/midl.c
)
include_directories(
  lib/lmdb
)

# Common code
############################################################
add_library(util STATIC
  src/mmap_array.c
)
include_directories(src)

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

add_library(edge_stream STATIC
  test/benchmark/edge_stream.c
)
include_directories(
  test/benchmark
)

add_executable(urldb test/benchmark/urldb.c)
add_executable(page_rank test/benchmark/page_rank.c)
add_executable(hits test/benchmark/hits.c)

target_link_libraries(edge_stream lz4)
target_link_libraries(page_rank util edge_stream)
target_link_libraries(hits util edge_stream)
target_link_libraries(urldb hat-trie lmdb xxhash)

# PageDB
###############################################################
add_executable(pagedb src/pagedb.c)
target_link_libraries(pagedb lmdb xxhash)

