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
    def __init__(self, line):
        (self.gid,
         self.name,
         self.asciiname,
         self.alternates,
         self.lat,
         self.lon,
         self.feature_class,
         self.feature,
         self.country,
         self.alternate_country,
         self.admin1,
         self.admin2,
         self.admin3,
         self.admin4,
         self.population,
         self.elevation,
         self.dem,
         self.timezone,
         self.modification) = line.split('\t')

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


class GeoNames(object):
    def __init__(self, datafile='allCountries.zip'):
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
            self._names = marisa_trie.Trie(name
                                           for row in rows(data)
                                           for name in row.all_names())

            self._gid = np.empty(shape=(len(self._names),), dtype=object)
            self._extra = np.zeros(shape=(self._max_gid + 1, 3), dtype=float)
            for row in rows(data):
                for name in row.all_names():
                    idx = self._names[name]
                    if self._gid[idx] is None:
                        self._gid[idx] = [row.gid]
                    else:
                        self._gid[idx].append(row.gid)

                self._extra[row.gid, :] = row.population, row.lat, row.lon

    def count(self, text, names=True):
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

        total = collections.defaultdict(int)
        if names:
            for loc in locations:
                if loc in self._names:
                    total[loc] += 1
        else:
            for loc in locations:
                idx = self._names.get(loc, False)
                if idx:
                    gid, population = max(
                        ((gid, self._extra[gid, 0]) for gid in self._gid[idx]),
                        key = lambda x: x[1])
                    total[gid] += 1

        return total


    def gid(self, name):
        """Return the GeoName ID for the location"""
        idx = self._names.get(name, False)
        if idx:
            return self._gid[idx]
        else:
            return None

    def extra(self, gid):
        """Return the stored information of the GeoName ID"""
        return self._extra[gid, :]

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    gn = GeoNames()
    print gn.count(r"""
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
    """, names=False)
