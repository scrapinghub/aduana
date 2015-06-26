# Description [![Build Status](https://travis-ci.org/scrapinghub/aduana.svg?branch=master)](https://travis-ci.org/scrapinghub/aduana)
A library to guide a web crawl using
[PageRank](http://en.wikipedia.org/wiki/PageRank),
[HITS](http://en.wikipedia.org/wiki/HITS_algorithm) or other ranking
algorithms based on the link structure of the web graph, even when
making big crawls (one billion pages).

**Warning:** I only test with regularity under Linux, my development
platform. From time to time I test also on OS X and Windows 8 using
[MinGW64](http://mingw-w64.yaxm.org/doku.php).

# Installation
    pip install aduana

# Documentation
Available at [readthedocs](http://aduana.readthedocs.org/en/latest/)

I have started documenting plans/ideas at the
[wiki](https://github.com/scrapinghub/aduana/wiki).

# Example
Single spider example:

    cd example
    pip install -r requirements.txt
    scrapy crawl example

To run the distributed crawler see the
[docs](http://aduana.readthedocs.org/en/latest/python.html#running-the-examples)
