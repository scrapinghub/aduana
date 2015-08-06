cmake_minimum_required(VERSION 2.8.8)
project(stream-mmap)

set(WARNING_FLAGS
  "-Wall -Wextra -Wundef -Wshadow -Wcast-align -Wstrict-prototypes -Wbad-function-cast -Wuninitialized -pedantic")

set(CMAKE_C_FLAGS
  "-std=c99 -m64 -msse2 -pthread ${WARNING_FLAGS}")

set(CMAKE_C_FLAGS_RELEASE  "${CMAKE_C_FLAGS_RELEASE} -O3")

# External libraries
############################################################
add_library(xxhash OBJECT lib/xxhash.c)
set_property(TARGET xxhash PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(smaz OBJECT lib/smaz.c)
set_property(TARGET smaz PROPERTY POSITION_INDEPENDENT_CODE 1)

include_directories(lib)

add_definitions(-DMDB_MAXKEYSIZE=500)
add_library(lmdb OBJECT
  lib/lmdb/mdb.c
  lib/lmdb/midl.c
)
include_directories(lib/lmdb)
set_property(TARGET lmdb PROPERTY POSITION_INDEPENDENT_CODE 1)


if(WIN32)
    add_library(mman OBJECT lib/mman.c)
    set_property(TARGET mman PROPERTY POSITION_INDEPENDENT_CODE 1)
endif(WIN32)

# Main library
############################################################
set(ADUANA_SRC
  src/mmap_array.c
  src/page_db.c
  src/hits.c
  src/page_rank.c
  src/scheduler.c
  src/bf_scheduler.c
  src/util.c
  src/page_rank_scorer.c
  src/hits_scorer.c
  src/txn_manager.c
  src/domain_temp.c
  src/freq_scheduler.c
  src/freq_algo.c

  $<TARGET_OBJECTS:lmdb>
  $<TARGET_OBJECTS:xxhash>
  $<TARGET_OBJECTS:smaz>
)
if(WIN32)
    set(ADUANA_SRC ${ADUANA_SRC} $<TARGET_OBJECTS:mman>)
endif(WIN32)

# Tests
#############################################################
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  # LMDB generates quite a few warnings. Suppress them.
  set_property(TARGET lmdb PROPERTY COMPILE_FLAGS
    "-Wno-shadow -Wno-cast-align -Wno-unused-parameter -Wno-format-extra-args")

  add_definitions(-DTEST)
  include_directories(test)
  set(ADUANA_SRC ${ADUANA_SRC} test/CuTest.c)
  add_executable(test test/test.c)
  target_link_libraries(test aduana)
endif()


add_library(aduana SHARED ${ADUANA_SRC})
include_directories(src)
target_link_libraries(aduana pthread m)

add_executable(page_db_dump src/page_db_dump.c)
target_link_libraries(page_db_dump aduana)
add_executable(page_db_find src/page_db_find.c)
target_link_libraries(page_db_find aduana)
add_executable(page_db_links src/page_db_links.c)
target_link_libraries(page_db_links aduana)
add_executable(page_db_path src/page_db_path.c)
target_link_libraries(page_db_path aduana)
add_executable(freq_scheduler_dump src/freq_scheduler_dump.c)
target_link_libraries(freq_scheduler_dump aduana)

# Installation
#############################################################
install(TARGETS aduana DESTINATION lib)
install(TARGETS page_db_dump page_db_find page_db_links page_db_path freq_scheduler_dump
        DESTINATION bin)
