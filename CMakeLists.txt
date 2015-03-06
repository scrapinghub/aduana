cmake_minimum_required(VERSION 2.8)
project(stream-mmap)

set(CMAKE_C_FLAGS 
  "-std=c99 -m64 -pthread -O3 -Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes -Wbad-function-cast -Wuninitialized -pedantic")

add_library(lz4 STATIC
  lib/lz4/lz4.c
  lib/lz4/lz4frame.c
  lib/lz4/lz4hc.c
  lib/lz4/xxhash.c)

include_directories(
  lib/lz4
)

add_executable(stream-mmap 
  main.c
  edge_stream.c)

target_link_libraries(stream-mmap lz4)
