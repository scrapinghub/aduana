import collections
import codecs
import itertools
import logging
import os.path
import zipfile

import marisa_trie
import nltk
import requests
import numpy as np


def download(filename, url):
    """If filename is not present then download from url"""
    if not os.path.isfile(filename):
        logging.warning(
            'Could not find filename {0}. Downloading it now...'.format(filename))
        r = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for i, chunk in enumerate(r.iter_content(chunk_size=1024*1024)):
                f.write(chunk)
                if i % 50 == 0:
                    logging.info('{0}: {1: 4d}MB'.format(filename, i))


class GeoNamesRow(object):
    """An structure with all GeoNames data for a location.

    See: http://download.geonames.org/export/dump/readme.txt
    """
    def __init__(self, line):
        (self.gid,                 # integer id of record in geonames database
         self.name,                # name of geographical point (utf8)
         self.asciiname,           # name of geographical point in plain ascii characters
         self.alternates,          # alternatenames, comma separated
         self.lat,                 # latitude in decimal degrees (wgs84)
         self.lon,                 # longitude in decimal degrees (wgs84)
         self.feature_class,
         self.feature,
         self.country,             # ISO-3166 2-letter country code, 2 characters
         self.alternate_country,   # alternate country codes, comma separated, ISO-3166 2-letter country code
         self.admin1,              # fipscode (subject to change to iso code)
         self.admin2,              # code for the second administrative division, a county in the US
         self.admin3,              # code for third level administrative division
         self.admin4,              # code for fourth level administrative division
         self.population,          # bigint (8 byte int)
         self.elevation,           # in meters, integer
         self.dem,                 # digital elevation model, srtm3 or gtopo30
         self.timezone,            # the timezone id
         self.modification         # date of last modification in yyyy-MM-dd format
        ) = line.split('\t')

        self.gid = int(self.gid)
        self.population = int(self.population)
        self.lat = float(self.lat)
        self.lon = float(self.lon)

        try:
            self.elevation = int(self.elevation)
        except ValueError:
            self.elevation = 0

        self.alternates = self.alternates.split(',')

    def all_names(self):
        yield self.name
        for name in self.alternates:
            yield name


class CountryInfoRow(object):
    def __init__(self, line):
        (self.iso,
         self.iso3,
         self.iso_numeric,
         self.fips,
         self.name,
         self.capital,
         self.area,
         self.population,
         self.continent,
         self.tld,
         self.currency_code,
         self.currency_name,
         self.phone,
         self.postal_code_format,
         self.postal_code_regex,
         self.languages,
         self.gid,
         self.neighbours,
         self.equivalent_fips) = line.split('\t')

        self.languages = self.languages.split(',')
        self.population = int(self.population)
        self.area = float(self.area)
        self.neighbours = self.neighbours.split(',')


class GeoNames(object):
    def __init__(self, datafile='allCountries.zip'):
        country_path = 'countryInfo.txt'
        download(country_path,
                 'http://download.geonames.org/export/dump/countryInfo.txt')

        self._countries = {}
        with open(country_path, 'r') as data:
            for line in data:
                if line[0] != '#':
                    row = CountryInfoRow(line)
                    self._countries[row.iso] = row

        download(datafile,
                'http://download.geonames.org/export/dump/allCountries.zip')

        self._max_gid = 0
        def rows(data):
            for row in itertools.imap(
                    GeoNamesRow,
                    codecs.iterdecode(data.open('allCountries.txt', 'r'), 'utf-8')):
                self._max_gid = max(self._max_gid, row.gid)
                yield row

        self._extra = {}
        with zipfile.ZipFile(datafile) as data:
            i = 0
            self._trie = marisa_trie.Trie(name
                                          for row in rows(data)
                                          for name in row.all_names())

            self._gid = np.empty(shape=(len(self._trie),), dtype=object)
            self._extra = np.zeros(shape=(self._max_gid + 1, 3), dtype=float)
            self._names = np.empty(shape=(self._max_gid + 1,), dtype=object)
            self._country = np.empty(shape=(self._max_gid + 1,), dtype=object)

            for row in rows(data):
                for name in row.all_names():
                    idx = self._trie[name]
                    if self._gid[idx] is None:
                        self._gid[idx] = [row.gid]
                    else:
                        self._gid[idx].append(row.gid)

                self._extra[row.gid, :] = row.population, row.lat, row.lon
                self._names[row.gid] = row.name
                self._country[row.gid] = row.country

    def gid(self, name):
        """Return the GeoName ID for the location"""
        idx = self._trie.get(name, False)
        if idx:
            return self._gid[idx]
        else:
            return None

    def name(self, gid):
        if gid < len(self._names):
            return self._names[gid]
        else:
            return None

    def population(self, gid):
        return self._extra[gid, 0]

    def coordinates(self, gid):
        return self._extra[gid, 1:]

    def country(self, gid):
        return self._country[gid]

    def country_info(self, iso_code):
        return self._countries.get(iso_code, None)


def flatten(x):
    r = []
    for a in x:
        if hasattr(a, '__iter__'):
            r += flatten(a)
        else:
            r.append(a)
    return r


def extract_gpe(t):
    """Extract GPEs (Geo-Political Entity) from an annotated tree"""
    try:
        label = t.label()
    except AttributeError:
        return []

    if label == 'GPE':
        return [' '.join(child[0] for child in t)]
    else:
        locations = []
        for child in t:
            locations += extract_gpe(child)
        return locations


def count_locations(geonames, text):
    """Count number of occurences of locations inside text.

    Returns a dictionary where each key is a location, given either by
    name or by id, depending on the value of the 'names' parameter.
    The values are the number of occurences.

    In case of ambiguity this method will select the location with highest
    population.
    """
    sentences = map(nltk.pos_tag,
                    map(nltk.word_tokenize, nltk.sent_tokenize(text)))
    locations = [w
                 for s in sentences
                 for w in extract_gpe(nltk.ne_chunk(s))]

    gids = []
    for loc in locations:
        gid = geonames.gid(loc)
        if gid:
            gids.append(gid)

    countries = collections.defaultdict(int)
    for gid in flatten(gids):
        countries[geonames.country(gid)] += geonames.population(gid)

    countries = sorted(
        countries.iteritems(), key=lambda x: x[1], reverse=True)
    accepted = set()
    for country, population in countries:
        info = geonames.country_info(country)
        if info and population >= info.population/100:
            accepted.add(country)

    total = collections.defaultdict(int)
    for gid_group in gids:
        accepted_gids = filter(
            lambda gid: geonames.country(gid) in accepted,
            gid_group)
        if accepted_gids:
            gid = max((geonames.population(gid), gid)
                      for gid in accepted_gids)[1]
            total[gid] += 1

    return total


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    gn = GeoNames()
    print count_locations(gn, r"""
    The United States of America (USA), commonly referred to as the
    United States (U.S.) or America, is a federal republic[16][17]
    consisting of 50 states and a federal district. The 48 contiguous
    states and Washington, D.C., are in central North America between
    Canada and Mexico. The state of Alaska is located in the
    northwestern part of North America and the state of Hawaii is an
    archipelago in the mid-Pacific. The country also has five
    populated and numerous unpopulated territories in the Pacific and
    the Caribbean. At 3.8 million square miles (9.842 million km2)[18]
    and with over 320 million people, the United States is the world's
    fourth-largest country by total area and third most populous. It
    is one of the world's most ethnically diverse and multicultural
    nations, the product of large-scale immigration from many
    countries.[19] The geography and climate of the United States are
    also extremely diverse, and the country is home to a wide variety
    of wildlife.
    """)
