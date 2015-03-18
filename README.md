# Description
Prototype application for processing huge graphs and compute PageRank, HITS or other scores

# Setup

## Requirements
- CMake and C compiler (C99 support)
- Python with lz4tools package (available on pip)
- shell and wget

## To build debug
1. mkdir debug && cd debug
2. cmake .. -DCMAKE_BUILD_TYPE=Debug
3. make

## To build release
1. mkdir release && cd release
2. cmake .. -DCMAKE_BUILD_TYPE=Release
3. make

## Download test data
1. cd test/data
2. ./fetch_live_journal

## Test
1. cd release
2. ./page_rank ../test/data/live_journal/*.lz4
3. ./hits ../test/data/live_journal/*.lz4