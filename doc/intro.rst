Introduction
============

Aduana is a component to be used with a web crawler. It contains the
logic to decide which page to crawl next. It accepts as inputs crawled
pages and it outputs the next pages to be crawled (requests).

.. figure:: _static/aduana-intro-arch.svg
   :align: center
   :figwidth: 75%

   Aduana input/output

The main objectives of Aduana are:

- Speed: it must be able to output thousands of requests per second.
- Scalability: it must be able to consider billions of crawled pages.
- Intelligence: it must be able to direct the crawl to interesting pages.

Components
-----------

There are two main components right now: a :doc:`C library <library>`
and :doc:`Python bindings <python>`.

The C library does the heavy lifting. In addition it also ships with
several command line tools. It's portable ANSI C99 code and all the
necessary dependencies are bundled with the library. Ideally you
should not concern yourself with this library unless you plan to
extend Aduana.

The Python bindings contain low-level bindings to the C library and
also:

- `Frontera <https://github.com/scrapinghub/frontera>`_
  backends. Frontera is an extension to `Scrapy <http://scrapy.org/>`_
  which allows to plug different crawl frontier backends. Aduana can
  be used as a Frontera backend.

- An Aduana server, to be used when crawling using multiple spiders.

Installation
------------

Making this pip-installable is right now in the TODO list (with high
priority). Meanwhile the following incantations are necessary:

1. Install the C library::

       mkdir release
       cmake .. -DCMAKE_BUILD_TYPE=Release
       make && sudo make install

2. Install the Python module::

       cd ../python
       pip install -r requirements.txt
       python setup.py install
