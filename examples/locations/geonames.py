import collections
import itertools
import logging
import os.path
import zipfile

import nltk
import requests

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

class GeoNames(object):       
    def __init__(self, datafile='allCountries.zip'):
        if not os.path.isfile(datafile):
            logging.warning(
                'Could not file datafile {0}. Downloading it now...'.format(datafile))
            r = requests.get(
                'http://download.geonames.org/export/dump/allCountries.zip',
                stream=True)
            with open(datafile, 'wb') as f:
                for i, chunk in enumerate(r.iter_content(chunk_size=1024*1024)):
                    f.write(chunk)
                    if i % 50 == 0:
                        logging.info('{0}: {1: 4d}MB'.format(datafile, i))

        self._names = collections.defaultdict(list)
        self._extra = {}
        with zipfile.ZipFile(datafile) as data:
            i = 0
            for row in itertools.imap(GeoNamesRow,
                                      data.open('allCountries.txt', 'r')):
                i += 1
                if i % 100000 == 0:
                    logging.info("{0}K".format(i/1000))

                self._extra[row.gid] = (row.population, row.lat, row.lon)
                names = [row.name] + row.alternates
                for n in names:
                    if n:
                        self._names[n].append(row.gid)

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
                gid, population = max(
                    ((gid, self._extra[gid][0]) for gid in self._names.get(loc, [])),
                    key = lambda x: x[1])
                total[gid] += 1

        return total


    def gid(self, name):
        """Return the GeoName ID for the location"""
        return self._names[name]

    def extra(self, gid):
        """Return the stored information of the GeoName ID"""
        return self._extra[gid]

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
