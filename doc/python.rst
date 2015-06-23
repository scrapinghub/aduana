Python library
==============

Installation
------------

To install just make::

    pip install aduana

It will automatically compile the C library and wrap it using
`CFFI <https://cffi.readthedocs.org/en/latest/>`_. Not all parts of
the C library are accessible from python, only the necessary ones for
the Frontera backends.

Apart from the python module it will also install two scripts:

- aduana-server.py
- aduana-server-cert.py

These scripts are to be used when using the `Distributed spider backend`_.


Using Scrapy/Frontera with Aduana
---------------------------------

Check the Frontera
`documentation <http://frontera.readthedocs.org/en/latest/>`_, for
general instructions about setting up Scrapy, Frontera and custom
backends. The workflow specific for Aduana is:

1. Set the backend, either as::

    BACKEND = 'aduana.frontera.Backend'

   or if you want to make a distributed crawl with multiple spiders as::

    BACKEND = 'aduana.frontera.WebBackend'

2. Set additional options, for example::

    PAGE_DB_PATH = 'test-crawl'
    SCORER = 'HitsScorer'
    USE_SCORES = True

3. Run your spider::

   scrapy crawl your_spider_here

Single spider backend
---------------------

This backend is the easiest one to run and works by calling
directly the wrapped C library. To use it set the backend as::

    BACKEND = 'aduana.frontera.Backend'

Additionally, the following setting are also used by this backend

- ``PAGE_DB_PATH``

  String with the path where you want the PageDB to be stored. Note
  that Aduana will actually make two directories. One will be the one
  specified by ``PAGE_DB_PATH`` and the other will add the suffix
  ``_bfs``. This second directory contains the database necessary for
  the operation of the (best first) scheduler. If this settings is not
  specified, of it is set to None, the directory will be generated
  randomly, with suffix ``frontera_`` and it will be automatically
  deleted when the spider is closed.

- ``SCORER``

  Strategy to use to compute final page scores. Can be one of the
  following:

    - ``None``
    - ``'HitsScorer'``
    - ``'PageRankScorer'``

- ``USE_SCORES``

   Set to ``True`` if you want that the scorer, in case that it was
   HITS or PageRank based merges the content scores with link based
   scores. Default is ``False``.

- ``SOFT_CRAWL_LIMIT``

   When a domain reaches this limit of crawls per second Aduana
   will try to make requests to other domains. Default is 0.25.

- ``HARD_CRAWL_LIMIT``

   When a domain reaches this limit of crawls per second Aduana will
   stop making new requests for this domain. Default is 100.

- ``PAGE_RANK_DAMPING``

   If the scorer is PageRank then set the damping to this
   value. Default is 0.85.


Distributed spider backend
--------------------------

This backend allows to use several spiders simultaneously, maybe at
different computers to improve CPU and network performance. It works
by having a central server and several spiders connecting to it
through a REST api.

The first thing you need to do is launch the server::

    aduana-server.py --help

    usage: aduana-server.py [-h] [--seeds [SEEDS]] [settings]

    Start Aduana server.

    positional arguments:
      settings         Path to python module containing server settings

    optional arguments:
      -h, --help       show this help message and exit
      --seeds [SEEDS]  Path to seeds file


Once the server is launched press Ctrl-C to exit.

The server settings are specified in a separate file that is passed as
a positional argument to the ``aduana-server.py`` script. The reason
is that they are settings that will be shared by all spiders that
connect to the server.

The following server settings have the same meaning as the ones in the
`Single spider backend`_.

- ``PAGE_DB_PATH``
- ``SCORER``
- ``USE_SCORES``
- ``SOFT_CRAWL_LIMIT``
- ``HARD_CRAWL_LIMIT``
- ``PAGE_RANK_DAMPING``

Additionally the following settings are available:

- ``SEEDS``

    Path to the seeds file, where each line is a different URL. This
    setting has no default and is mandatory. It can be
    specified/overriden with the ``--seeds`` option when launching the
    server.

- ``DEFAULT_REQS``

    If the client does not specify the desired number of requests
    serve this number. Default number is 10.

- ``ADDRESS``

    Server will listen on this address. Default ``'0.0.0.0'``.

- ``PORT``

    Server will listen on this port. Default 8000.

- ``PASSWDS``

    A dictionary mapping login name to password. If ``None`` then all
    connections will be accepted. Notice that it uses
    `BasicAuth <https://en.wikipedia.org/wiki/Basic_access_authentication>`_
    which sends login data in plain text. If security is of concern
    then it is adviced to use this option along with ``SSL_KEY`` and
    ``SSL_CERT``. Default value for this setting is ``None``.

- ``SSL_KEY``

    Path to SSL keyfile. If this setting is used then ``SSL_CERT``
    must be set too and all communications will be encrypted between
    server and clients using HTTPS. Default ``None``.

- ``SSL_CERT``

    Path to SSL certificate. Default ``None``.

The Frontera settings to use this backend are::

    BACKEND = 'aduana.frontera.WebBackend'

Additionally, the following setting are also used by this backend

- ``SERVER_NAME``

    Address of the server. Default ``'localhost'``

- ``SERVER_PORT``

    Server port number. Default 8000.

- ``SERVER_CERT``

    Path to server certificate. If this option is set it will try to
    connecto to the server using HTTPS. Default ``None``.

WebBackend REST API
~~~~~~~~~~~~~~~~~~~
There are two messages exchanged between the spiders and the server.

- Crawled

  When a spider crawls a page it sends a POST message to
  ``/crawled``. The body is a json dictionary with the following fields:

    - url: The URL of the crawled page, ASCII encoded. This is the
      only mandatory field.
    - score: a floating point number. If omited defaults to zero.
    - links: a list links. Each element of the links is a pair made
      from link URL and link score.

  En example message::

        { "url"  : "http://scrapinghub.com",
          "score": 0.5,
          "links": [["http://scrapinghub.com/professional-services/", 1.0],
                    ["http://scrapinghub.com/platform/", 0.5],
                    ["http://scrapinghub.com/pricing/", 0.8],
                    ["http://scrapinghub.com/clients/", 0.9]] }

- Request

  When the spider needs to know which pages to crawl next it sends a
  GET message to ``/request``. The query strings accepts an optional
  parameter ``n`` with the maximum number of URLs. If not specified
  the default value specified in the server settings will be used. The
  response will be a json encoded list of URLs.
  Example (``pip install httpie``)::

      $ http --auth test:123 --verify=no https://localhost:8000/request n==3

      HTTP/1.1 200 OK
      Date: Tue, 23 Jun 2015 08:40:46 GMT
      content-length: 120
      content-type: application/json

      [
          "http://www.reddit.com/r/MachineLearning/",
          "http://www.datanami.com/",
          "http://venturebeat.com/tag/machine-learning/"
      ]

Running the examples
--------------------

To run the single spider example just go to the example directory,
install the requirements and run the crawl::

    cd example
    pip install -r requirements.txt
    scrapy crawl example

To run the distributed spider example we need to dance a little more:

1. Go to the example directory::

    cd example


2. Generate a server certificate::

    aduana-server-cert.py

3. Launch the server::

    aduana-server.py server-config.py

4. Go to the example directory in another terminal and then::

    scrapy crawl -s FRONTERA_SETTINGS=example.frontera.web_settings example
