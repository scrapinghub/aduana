# -*- coding: utf-8 -*-

import collections
import codecs
import itertools
import logging
import os.path
import zipfile
import struct

import marisa_trie
import nltk
import requests
import numpy as np
import networkx as nx
import lmdb
import sklearn.neighbors


def geod2ecef(geod):
    """Convert from WGS84 geodetic coordinates to ECEF coordinates.

    - geod: NumPy array with geodetic latitude, longitude and, optionally,
            height. Latitude and longitude are in radians and height in meters.
            If no height is provided then height is assumed to be zero. Valid
            array shapes are:

                (2,  ) -> latitude and longitude
                (3,  ) -> latitude, longitude and height
                (N, 2) -> each row has latitude and longitude
                (N, 3) -> each row has latitude, longitude and height

    - returns: an array with shape (3, ) or (N, 3) whith ECEF coordinates in
               meters.

    Note: we need this function to perform valid distance comparisons between
    locations.
    """
    if len(geod.shape) == 1:
        lat = geod[0]
        lon = geod[1]
        if len(geod) == 3:
            alt = geod[2]
        else:
            alt = 0.0
    else:
        lat = geod[:, 0]
        lon = geod[:, 1]
        if geod.shape[1] == 3:
            alt = geod[:, 2]
        else:
            alt = 0.0

    a = 6378137
    e = 8.1819190842622e-2
    N = a / np.sqrt(1 - e**2 * np.sin(lat)**2)

    x = (N + alt) * np.cos(lat) * np.cos(lon)
    y = (N + alt) * np.cos(lat) * np.sin(lon)
    z = ((1-e**2) * N + alt) * np.sin(lat)

    if len(geod.shape) == 1:
        return np.array([x, y, z])
    else:
        return np.column_stack((x, y, z))

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
    """An structure with all GeoNames data for a country.

    See: http://download.geonames.org/export/dump/readme.txt
    """
    def __init__(self, line):
        (self.iso,                # ISO 2 letter alphabetic code
         self.iso3,               # ISO 3 letter alphabetic code
         self.iso_numeric,        # ISO Numeric code
         self.fips,               # https://en.wikipedia.org/wiki/List_of_FIPS_country_codes
         self.name,
         self.capital,            # Capital name
         self.area,               # In km^2
         self.population,         # Integer
         self.continent,          # EU, AS, NA, SA, AF, OC, AN (Antarctica)
         self.tld,                # Top Level Domain
         self.currency_code,      # EUR, USD, ...
         self.currency_name,      # Euro, Dollar, ...
         self.phone,              # International call prefix
         self.postal_code_format,
         self.postal_code_regex,
         self.languages,          # Delimited by commas
         self.gid,                # GeoName ID
         self.neighbours,         # ISO 2-letter codes delimited by commaas
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
    int_struct = struct.Struct('=i')

    def __init__(self):
        # self._hierarchy: a DiGraph
        # self._root: a set of locations without parents (they have no parents
        #             in self._hierarchy AND we could not find any other ones).
        hierarchy_path = 'hierarchy.zip'
        download(hierarchy_path,
                 'http://download.geonames.org/export/dump/hierarchy.zip')
        self._hierarchy = nx.DiGraph()
        self._root = set()
        with zipfile.ZipFile(hierarchy_path) as data:
            for line in data.open('hierarchy.txt', 'r'):
                parent, child = map(int, line.split()[:2])
                self._hierarchy.add_edge(parent, child)

        # self._country_info: map ISO numeric to CountryInfoRow
        # self._country_code: map ISO 2-letter code to ISO numeric
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

        # A name can map to several different locations (GeoNamesIDs), and two
        # names can map to the same location too.
        #
        # Name1 --+--> GeoNameID 1 --> (name, population, country, coordinates)
        #         +--> GeoNameID 2 --> (name, population, country, coordinates)
        #         +--> GeoNameID 3 --> ...
        #               ^
        # Name2 ---+----+
        #          |
        #          +-> GeoNameID 4 --> ...

        # self._trie: map name to trie index
        # self._gid : map trie index to a list of GeoName IDs
        # self._info: array mapping gid to (population, name, country)
        # self._geod: array mapping gid to geodetic coordinates
        # self._ecef: self._geod converted to ECEF
        # self._nn  : KDTree to query nearest neighbors
        data_path='allCountries.zip'
        download(data_path,
                'http://download.geonames.org/export/dump/allCountries.zip')
        with zipfile.ZipFile(data_path) as data:
            self._gid = lmdb.open('geonames_lmdb', max_dbs=2, map_size=1000000000)
            if (not os.path.exists('geonames.marisa') or
                not os.path.exists('geonames.npz') or
                not os.path.exists('geonames_lmdb')):
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

                self._geod = np.zeros(shape=(self._max_gid + 1, 2))
                self._info = np.zeros(self._max_gid + 1, dtype=[
                    ('population', 'i8'),
                    ('name',       'i4'),
                    ('country',    'i4')
                ])

                with self._gid.begin(
                        db=self._gid.open_db(
                            key='gid', dupsort=True, create=True),
                        write=True) as txn:
                    for row in rows(data):
                        for name in row.all_names():
                            txn.put(GeoNames.int_struct.pack(self._trie[name]),
                                    GeoNames.int_struct.pack(row.gid))

                        self._info[row.gid] = (
                            row.population,
                            self._trie.get(row.name),
                            self._country_code.get(row.country, -1)
                        )
                        self._geod[row.gid, :] = row.lat, row.lon

                np.savez('geonames.npz', info=self._info, geod=self._geod)

            else:
                self._trie = marisa_trie.Trie()
                self._trie.load('geonames.marisa')

                with np.load('geonames.npz') as data:
                    self._info = data['info']
                    self._geod = data['geod']

                self._max_gid = len(self._info) - 1

            self._ecef = geod2ecef(np.pi/180.0*self._geod)
            self._nn = sklearn.neighbors.KDTree(self._ecef)


    @property
    def max_gid(self):
        return self._max_gid

    def gid(self, name):
        """Return a list of GeoName IDs associated with the location name.

        If name is not inside database then return None.
        """
        idx = self._trie.get(name, False)
        if idx:
            db = self._gid.open_db(key='gid', dupsort=True)
            with self._gid.begin(db=db, write=False) as txn:
                cur = txn.cursor(db)
                if not cur.set_key(GeoNames.int_struct.pack(idx)):
                    return None
                return [GeoNames.int_struct.unpack(x)[0] for x in cur.iternext_dup()]
        else:
            return None

    def name(self, gid):
        """Return (main) name for the given GeoName ID or None if the ID does
        not exist"""
        if gid < len(self._info):
            return self._trie.restore_key(self._info[gid]['name'])
        else:
            return None

    def iter_names(self):
        """Iterate over all location names"""
        return self._trie.iterkeys()

    def population(self, gid):
        """Return population for the given GeoName ID"""
        return self._info[gid]['population']

    def coordinates(self, gid):
        """Return WGS84 coordinates for the GeoName ID.

        Coordinates are (latitude, longitude) on decimal degrees.
        """
        return self._geod[gid]

    def coordinates_ecef(self, gid):
        """Return ECEF coordinates in meters for the GeoName ID"""
        return self._ecef[gid, :]

    def country(self, gid):
        """Return 2-letter ISO country code for GeoName ID"""
        try:
            return self._country_info[self._info[gid]['country']].iso
        except KeyError:
            return None

    def country_info(self, country):
        """Return all country information for the given country
        (2-letter ISO code)"""
        try:
            return self._country_info[self._country_code[country]]
        except KeyError:
            return None

    def children(self, gid):
        """Return dirent descendants in the hierarchy for this GeoName ID"""
        try:
            return self._hierarchy.successors(gid)
        except NetworkXError:
            return []

        return self._children[gid]

    def parents(self, gid):
        """Return direct asscendants in the hierarchy for this GeoName ID.

        If the location has not parents in the hierarchy it will attempt to
        find them nonetheless using the following algorithm:

        1. Find all descendants
        2. Find the 1000 nearest locations, if any of them has the same name
           or has more population and it's not a descendant then it's the
           new parent.

        The descendants check is to avoid loops in the hierarchy.
        """
        try:
            p = self._hierarchy.predecessors(gid)
        except nx.NetworkXError:
            p = []

        if not p and gid not in self._root:
            name = self.name(gid)
            population = self.population(gid)
            try:
                descendants = nx.descendants(self._hierarchy, gid)
            except nx.NetworkXError:
                descendants = set()
            for neighbor in self.nearest(gid, 1000):
                match_name = self.name(neighbor) == name
                bigger = (population > 0) and (self.population(neighbor) > population)
                if ((match_name or bigger) and (neighbor not in descendants)):
                    p.append(neighbor)
                    self._hierarchy.add_edge(neighbor, gid)
                    break
            if not p:
                self._root.add(gid)
        return p

    def nearest(self, location, k=1):
        """Find nearest locations to this one.

        Location can be a GeoName ID (which will be pruned from the results) or
        ECEF coordinates.
        """
        gid = location if isinstance(location, int) else None

        near = self._nn.query(
            self.coordinates_ecef(gid) if gid else location,
            k=k+1,
            return_distance=False,
            sort_results=True
        )[0,1:].tolist()

        if gid:
            return filter(lambda x: x != gid, near)
        else:
            return near


def window_iter(x, size):
    """Iterate over x using windows of the given size.

    For example:

    for window in window_iter(range(8), 3):
        print window

    Outputs:

    [0, 1, 2]
    [1, 2, 3]
    [2, 3, 4]
    [3, 4, 5]
    [4, 5, 6]
    [5, 6, 7]
    """
    it = iter(x)
    window = collections.deque(itertools.islice(it, size) , maxlen=size)
    yield tuple(window)

    for element in it:
        window.popleft()
        window.append(element)
        yield tuple(window)


def ner_tokenizer(text):
    """Apply NLTK's Named Entity Recognizer to the text to extract candidate
    locations"""

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

    sentences = map(nltk.pos_tag,
                    map(nltk.word_tokenize, nltk.sent_tokenize(text)))
    return [w
            for s in sentences
            for w in extract_gpe(nltk.ne_chunk(s))]


def graph_location(geonames, prefix, location, max_gids=20):
    """Build the graph associated with location. Append prefix to each node."""
    gids = geonames.gid(location)
    if not gids:
        return

    # Restrict the number of gids per location. We will add negative ties
    # between these gids and some locations have more then 1000 gids, which
    # gives more than 1 million ties.
    gids = map(lambda x: x[1],
               sorted([(geonames.population(gid), gid) for gid in gids],
                      reverse=True)[:max_gids])

    # Build a node from a gid by adding the prefix
    def node(gid):
        return (prefix, gid)

    G = nx.Graph(prefix=prefix)

    def add_ancestors(gid, parents):
        ngid = node(gid)
        for parent in parents:
            nparent = node(parent)
            if nparent not in G:
                G.add_node(nparent, label=geonames.name(parent), bias=0.0)
            G.add_edge(ngid, nparent, weight=1.0)
            add_ancestors(parent, geonames.parents(parent))

    for gid in gids:
        parents = geonames.parents(gid)
        if parents:
            G.add_node(node(gid), label=geonames.name(gid), bias=1.0)
            add_ancestors(gid, parents)

        # Add ties between neighbouring countries
        c_info = geonames.country_info(geonames.country(gid))
        if c_info:
            for neighbour in c_info.neighbours:
                n_info = geonames.country_info(neighbour)
                if n_info:
                    nc = node(c_info.gid)
                    nn = node(n_info.gid)
                    if nc not in G:
                        G.add_node(nc, label=c_info.name, bias=0.0)
                    if nn not in G:
                        G.add_node(nn, label=n_info.name, bias=0.0)
                    G.add_edge(nc, nn, weight=1.0)

    # Add a negative tie between gids competing for the same location
    for g1, g2 in itertools.combinations(map(node, gids), 2):
        if (g1 in G) and (g2 in G) and not G.has_edge(g1, g2):
            G.add_edge(g1, g2, weight=-1.0)

    return G


def propagate(A, b=None, start=None, eps=1e-3, max_iter=1000):
    """Propagate neural network signals until an energy minimum is achieved"""
    x = start if start is not None else 1e-3*np.random.uniform(-1.0, 1.0, size=A.shape[0])
    err = eps + 1
    i = 0
    e1 = None
    logging.info('Iteration/Error/Energy')
    while err > eps:
        for k in xrange(A.shape[0]):
            x[k] = A[k,:].dot(x)
            if b is not None:
                x[k] += b[k]

            # Soft Hopfield network
            x[k] = -1.0 + 2.0/(1.0 + np.exp(-x[k]))

        e2 = -0.5*x.dot(A.dot(x)) - x.dot(b)
        if e1:
            err = e1 - e2
        e1 = e2

        i += 1

        logging.info('{0: 4d}/{1:.2e}/{2:.2e}'.format(i, err, e1))
        if i>max_iter:
            logging.warning(
                'Exceeded maximum number of iterations ({0}) with error {1}>{2}'.format(
                    max_iter, err, eps))
            return x
    return x


def tag_locations(geonames, text, tokenizer=ner_tokenizer, out_graph=None):
    """Return a list of tuples where the first element is the location, the
    second element is the associated geoname id and the third element is an
    score between -1 and 1.
    """

    # Extract locations names from text
    locations = filter(
        lambda location: geonames.gid(location),
        tokenizer(text))

    subgraphs = filter(lambda x: x is not None,
                       [graph_location(geonames, pos, location)
                        for pos, location in enumerate(locations)])

    G = nx.union_all(subgraphs)
    for window in window_iter(subgraphs, 5):
        gids = [set(gid for (_, gid) in H.nodes_iter()) for H in window]
        for (G1, gids_1), (G2, gids_2) in itertools.combinations(zip(window, gids), 2):
            p1 = G1.graph['prefix']
            p2 = G2.graph['prefix']
            for gid in (gids_1 & gids_2):
                G.add_edge((p1, gid), (p2, gid), weight=1.0)

    # Assign an index to each graph node
    index = {node: i for (i, node) in enumerate(G.nodes())}

    A = nx.adjacency_matrix(G, weight='weight')
    b = np.array([data['bias'] for node, data in G.nodes(data=True)])

    activation = propagate(A, b)
    for node, data in G.nodes_iter(data=True):
        data['score'] = float(activation[index[node]])

    if out_graph is not None:
        nx.write_gexf(G, out_graph)

    geotags = []
    for pos, location in enumerate(locations):
        best_score = None
        best_gid = None
        for gid in geonames.gid(location):
            try:
                score = activation[index[(pos, gid)]]
            except KeyError:
                continue
            if score > best_score:
                best_gid = gid
                best_score = score

        if best_gid:
            geotags.append((location, best_gid, best_score))
            logging.info(u'{0} -> {1} (country: {2}, score: {3:.4f})'.format(
                location, best_gid, geonames.country(best_gid), best_score))

    return geotags

class CommonWords(object):
    def __init__(self, words='common_words.txt'):
        with open(words, 'r') as data:
            self._words = set(line.strip() for line in data)

    def __contains__(self, word):
        return word.lower() in self._words

common_words = CommonWords()

def get_test_text():
    import requests
    from bs4 import BeautifulSoup

    r = requests.get('https://en.wikipedia.org/wiki/Madrid')
    soup = BeautifulSoup(r.text)
    for script in soup(["script", "style"]):
        script.extract()
    return soup.get_text()

def get_short_text():
    return u"""
    Madrid (/məˈdrɪd/, Spanish: [maˈðɾið], locally: [maˈðɾiθ, -ˈðɾi]) is a
    south-western European city and the capital and largest municipality
    of Spain. The population of the city is almost 3.2 million[4] and that
    of the Madrid metropolitan area, around 7 million. It is the
    third-largest city in the European Union, after London and Berlin, and
    its metropolitan area is the third-largest in the European Union after
    Paris and London.[5][6][7][8] The city spans a total of 604.3 km2
    (233.3 sq mi).[9]

    The city is located on the Manzanares River in the centre of both the
    country and the Community of Madrid (which comprises the city of
    Madrid, its conurbation and extended suburbs and villages); this
    community is bordered by the autonomous communities of Castile and
    León and Castile-La Mancha. As the capital city of Spain, seat of
    government, and residence of the Spanish monarch, Madrid is also the
    political, economic and cultural centre of Spain.[10] The current
    mayor is Manuela Carmena from Ahora Madrid.

    The Madrid urban agglomeration has the third-largest GDP[11] in the
    European Union and its influences in politics, education,
    entertainment, environment, media, fashion, science, culture, and the
    arts all contribute to its status as one of the world's major global
    cities.[12][13] Due to its economic output, high standard of living,
    and market size, Madrid is considered the major financial centre of
    Southern Europe[14][15] and the Iberian Peninsula; it hosts the head
    offices of the vast majority of the major Spanish companies, such as
    Telefónica, Iberia and Repsol. Madrid is the 17th most livable city in
    the world according to Monocle magazine, in its 2014 index.[16][17]
    """

def test_counter():
    gn = GeoNames()
    text = get_test_text()
    geotags = tag_locations(gn, text)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    test_counter()
