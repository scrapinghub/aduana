cmake_minimum_required(VERSION 2.8)
project(stream-mmap)

set(WARNING_FLAGS 
  "-Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes -Wbad-function-cast -Wuninitialized -pedantic")

set(CMAKE_C_FLAGS 
  "-std=c99 -m64 -msse2 -pthread ${WARNING_FLAGS}")

set(CMAKE_C_FLAGS_RELEASE  "${CMAKE_C_FLAGS_RELEASE} -O3")

add_library(lz4 STATIC
  lib/lz4/lz4.c
  lib/lz4/lz4frame.c
  lib/lz4/lz4hc.c
  lib/lz4/xxhash.c)

include_directories(
  lib/lz4
)

add_library(util STATIC
  mmap_array.c
  edge_stream.c
)

add_executable(page_rank page_rank.c)
add_executable(hits hits.c)

target_link_libraries(util lz4)
target_link_libraries(page_rank util)
target_link_libraries(hits util)

