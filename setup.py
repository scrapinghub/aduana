#!/usr/bin/env python

from setuptools import setup

setup(name             = 'aduana',
      version          = '0.1',
      description      = 'Bindings for Aduana library',
      url              = 'https://github.com/scrapinghub/aduana',
      author           = 'Pedro Lopez-Adeva Fernandez-Layos',
      author_email     = 'pedro@scrapinghub.com',
      packages         = ['aduana'],
      scripts          = [
          'aduana/bin/aduana-server.py', 
          'aduana/bin/aduana-server-cert.py'
      ],
      setup_requires   = ['cffi >= 1.1.2'],
      install_requires = ['cffi >= 1.1.2'],
      cffi_modules     = ['build_wrapper.py:ffi']
)
