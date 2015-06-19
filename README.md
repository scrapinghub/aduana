# Description [![Build Status](https://travis-ci.org/scrapinghub/aduana.svg?branch=master)](https://travis-ci.org/scrapinghub/aduana)
A library to guide a web crawl using
[PageRank](http://en.wikipedia.org/wiki/PageRank),
[HITS](http://en.wikipedia.org/wiki/HITS_algorithm) or other ranking
algorithms based on the link structure of the web graph, even when
making big crawls (one billion pages).

An example scrapy spider is provided [here](example).

I only test with regularity under Linux, my development
platform. From time to time I test also on OS X and Windows 8 using
[MinGW64](http://mingw-w64.yaxm.org/doku.php).

# Installation
    pip install aduana

# Example
    cd example
    pip install -r requirements
    scrapy crawl example

# Documentation
    cd doc
    make html
    open _build/html/index.html
