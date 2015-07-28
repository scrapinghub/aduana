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
import networkx as nx


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
        try:
            self.gid = int(self.gid)
        except ValueError:
            self.gid = -1

        self.iso_numeric = int(self.iso_numeric)

class GeoNames(object):
    def __init__(self, datafile='allCountries.zip'):
        hierarchy_path = 'hierarchy.zip'
        download(hierarchy_path,
                 'http://download.geonames.org/export/dump/hierarchy.zip')

        self._children = collections.defaultdict(set)
        self._parents = collections.defaultdict(set)
        with zipfile.ZipFile(hierarchy_path) as data:
            for line in data.open('hierarchy.txt', 'r'):
                parent, child = map(int, line.split()[:2])
                self._children[parent].add(child)
                self._parents[child].add(parent)


        country_path = 'countryInfo.txt'
        download(country_path,
                 'http://download.geonames.org/export/dump/countryInfo.txt')

        self._country_code = {}
        self._country_info = {}
        with open(country_path, 'r') as data:
            for line in data:
                if line[0] != '#':
                    row = CountryInfoRow(line)
                    self._country_info[row.iso_numeric] = row
                    self._country_code[row.iso] = row.iso_numeric


        download(datafile,
                'http://download.geonames.org/export/dump/allCountries.zip')
        with zipfile.ZipFile(datafile) as data:
            i = 0
            if (not os.path.exists('geonames.marisa') or
                not os.path.exists('geonames.npz')):
                self._max_gid = 0

                def rows(data):
                    for row in itertools.imap(
                            GeoNamesRow,
                            codecs.iterdecode(data.open('allCountries.txt', 'r'), 'utf-8')):
                        self._max_gid = max(self._max_gid, row.gid)
                        yield row

                self._trie = marisa_trie.Trie(name
                                              for row in rows(data)
                                              for name in row.all_names())
                self._trie.save('geonames.marisa')
                self._gid = np.empty(shape=(len(self._trie),), dtype=object)

                self._info = np.zeros(self._max_gid + 1, dtype=[
                    ('lat',        'f4'),
                    ('lon',        'f4'),
                    ('population', 'i8'),
                    ('names',      'i4'),
                    ('country',    'i4')
                ])

                for row in rows(data):
                    for name in row.all_names():
                        idx = self._trie[name]
                        if self._gid[idx] is None:
                            self._gid[idx] = [row.gid]
                        else:
                            self._gid[idx].append(row.gid)

                    self._info[row.gid] = (
                        row.lat,
                        row.lon,
                        row.population,
                        self._trie.get(row.name),
                        self._country_code.get(row.country, -1)
                    )
                np.savez('geonames.npz', gid=self._gid, info=self._info)

            else:
                self._trie = marisa_trie.Trie()
                self._trie.load('geonames.marisa')

                with np.load('geonames.npz') as data:
                    self._gid = data['gid']
                    self._info = data['info']

                self._max_gid = len(self._info) - 1


    @property
    def max_gid(self):
        return self._max_gid

    def gid(self, name):
        """Return the GeoName ID for the location"""
        idx = self._trie.get(name, False)
        if idx:
            return self._gid[idx]
        else:
            return None

    def name(self, gid):
        if gid < len(self._info):
            return self._trie.restore_key(self._info[gid]['names'])
        else:
            return None

    def iter_names(self):
        return self._trie.iterkeys()

    def population(self, gid):
        return self._info[gid]['population']

    def coordinates(self, gid):
        return (self._info[gid]['lat'], self._info[gid]['lon'])

    def country(self, gid):
        try:
            return self._country_info[self._info[gid]['country']].iso
        except KeyError:
            return None

    def country_info(self, country):
        try:
            return self._country_info[self._country_code[country]]
        except KeyError:
            return None

    def children(self, gid):
        return self._children[gid]

    def parents(self, gid):
        return self._parents[gid]


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

    gids = {}
    for loc in set(locations):
        gid = geonames.gid(loc)
        if gid:
            gids[loc] = gid

    G = nx.DiGraph()
    def grow_graph(gid):
        G.add_node(gid)
        for parent in geonames.parents(gid):
            G.add_node(parent)
            G.add_edge(gid, parent)

            grow_graph(parent)

    for gid in flatten(gids.values()):
        grow_graph(gid)

    hscore, ascore = nx.hits(G)
    best = {}
    for loc, gid in gids.iteritems():
        score, gid = max((ascore[g], g) for g in gid)
        best[loc] = gid

    total = collections.defaultdict(int)
    for loc in locations:
        try:
            total[best[loc]] += 1
        except KeyError:
            pass

    return total

class CommonWords(object):
    def __init__(self, words='common_words.txt'):
        with open(words, 'r') as data:
            self._words = set(line.strip() for line in data)

    def __contains__(self, word):
        return word.lower() in self._words

common_words = CommonWords()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)

    import requests
    from bs4 import BeautifulSoup

    r = requests.get('https://en.wikipedia.org/wiki/Madrid')
    soup = BeautifulSoup(r.text)
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()

    gn = GeoNames()
    count = sorted(
        count_locations(gn, text).items(),
        key=lambda x: x[1],
        reverse=True
    )
    for loc, n in count:
        print loc, n
