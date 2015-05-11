# Description
A proof of concept to demonstrate that is possible to use a single
computer to direct a crawl using PageRank, HITS or other ranking
algorithms based on the link structure of the web graph, even when
making big crawls (one billion pages).

This is a pure C implementation of several algorithms intended to be
wrapped using python and called from
[Frontera](https://github.com/scrapinghub/frontera), a crawl frontier
module for [Scrapy](https://github.com/scrapy/scrapy).

Python bindings and an example spider are provided
[here](python/example).

# Setup

## Requirements
- CMake and C compiler (C99 support)
- Python

For the example spider, the following python modules:
- scrapy
- BeautifulSoup4

## To build debug
1. mkdir debug && cd debug
2. cmake .. -DCMAKE_BUILD_TYPE=Debug
3. make

## To build release
1. mkdir release && cd release
2. cmake .. -DCMAKE_BUILD_TYPE=Release
3. make

## Test
1. cd debug && make
2. ./test
