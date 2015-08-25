import collections
import itertools
import string

import nltk


def is_stop_char(c):
    return (c in string.punctuation or
            c in string.whitespace)

class StopCharTokenizer(object):
    def __init__(self, window, is_stop_char=is_stop_char):
        self.is_stop_char=is_stop_char
        self.window = window

    def index_stop_chars(self, text):
        return itertools.imap(
            lambda (i, c): i,
            itertools.ifilter(
                lambda (i, c): self.is_stop_char(c), enumerate(text)))

    def starts(self, text):
        i = self.index_stop_chars(text)
        a = -1
        for b in itertools.chain(i, [len(text)]):
            if b > a + 1:
                yield a + 1
            a = b

    def stops(self, text):
        i = self.index_stop_chars(text)
        a = -1
        for b in itertools.chain(i, [len(text)]):
            if b > a + 1:
                yield b
            a = b

    def index_stop_words(self, text):
        a = self.starts(text)
        b = self.stops(text)

        def end(i):
            return i is None

        def next():
            try:
                return b.next()
            except StopIteration:
                return None

        w = collections.deque()
        i = 0
        j = next()
        for i in a:
            while not end(j) and j < i + self.window:
                w.append(j)
                j = next()
            if w:
                for k in w:
                    yield (i, k)
                i = w.popleft()

    def tokens(self, text):
        for a, b in self.index_stop_words(text):
            yield text[a:b]

    def __call__(self, text):
        return self.tokens(text)


def new_stop_char_tokenizer(geonames, is_stop_char=is_stop_char):
    return StopCharTokenizer(
        window=max(itertools.imap(len, geonames.iter_names())),
        is_stop_char=is_stop_char)


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
