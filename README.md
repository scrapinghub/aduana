# Description [![Build Status](https://travis-ci.org/scrapinghub/aduana.svg?branch=master)](https://travis-ci.org/scrapinghub/aduana)
A proof of concept to demonstrate that is possible to
use a single computer to direct a crawl using
[PageRank](http://en.wikipedia.org/wiki/PageRank),
[HITS](http://en.wikipedia.org/wiki/HITS_algorithm) or other ranking
algorithms based on the link structure of the web graph, even when
making big crawls (one billion pages).

This is a pure C implementation of several algorithms intended to be
wrapped using python and called from
[Frontera](https://github.com/scrapinghub/frontera), a crawl frontier
module for [Scrapy](https://github.com/scrapy/scrapy).

Python bindings and an example scrapy spider are provided
[here](python/example).

## Warning
This is very alpha quality and experimental. At this point everything
could change: from the database used ([LMDB](http://symas.com/mdb/))
to the implementation language (C).

Also, I only test with regularity under Linux, my development
platform. From time to time I test also on Windows 8 using
[MinGW64](http://mingw-w64.yaxm.org/doku.php). Porting to OSX should
not be difficult but right now I have no time.


# Requirements
- CMake and C compiler (C99 support, gcc or clang for example)
- Pthreads
- Python (2.7 tested)

# Installation
1. mkdir release && cd release
2. cmake .. -DCMAKE_BUILD_TYPE=Release
3. make && sudo make install
4. cd ../python
5. python setup.py install

# Test
1. mkdir debug && cd debug
2. cmake .. -DCMAKE_BUILD_TYPE=Debug
3. make
4. ./test

# Example
The example spider has the following additional requirements. All can
be installed using pip.

- Scrapy
- Frontera
- BeautfilSoup4

To run:

1. cd python/example
2. scrapy crawl example

# Documentation

TODO.

Actually, I have tried to follow some discipline by using
[Doxygen](http://www.stack.nl/~dimitri/doxygen/), you can build the
docs running:

1. doxygen
2. Open with your favorite browser doc/index.html
