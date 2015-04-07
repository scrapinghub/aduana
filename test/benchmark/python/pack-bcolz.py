#!/usr/bin/env python

# Drop this script into the directory holding the Common Crawl Corpus 
# Hypergraph and run it. It will transform every part-*.gz file into bcolz
# format.
# 
# Requirements: bcolz
import glob
import gzip
import os.path

from multiprocessing import Pool

import bcolz


def iterlines(fgz):
    """iterate lines, skipping those starting with #"""
    return (l for l in fgz if not l.startswith('#'))


def count_lines(fgz):
    pos = fgz.tell()
    nlines = sum(1 for _ in iterlines(fgz))
    fgz.seek(pos)
    return nlines


def transform(pgz):
    pbz = pgz[:-3] + '-bcolz'
    if os.path.exists(pbz):
        try:
            bz = bcolz.open(pbz, mode='r')
            if bz.attrs['completed']:
                print 'Skipping', pbz
                return
        except:
            pass
        print 'Deleting incomplete', pbz

    print "Processing {0} -> {1}".format(pgz, pbz)
    def edge_iterator(fgz):
        a = 0
        b = 0
        for line in iterlines(fgz):
            fields = line.split()
            x = int(fields[0])
            y = int(fields[1])            
            yield (x - a, y - b)

            a = x
            b = y

    fgz = gzip.open(pgz, 'r')

    nlines = count_lines(fgz)
    bz = bcolz.fromiter(edge_iterator(fgz), dtype='i8, i8', 
                        count=nlines, rootdir=pbz, mode='w')
    bz.attrs['completed'] = True
    bz.flush()    
    print "    {0}: {1}".format(pbz, nlines)
    fgz.close()


if __name__ == '__main__':
    p = Pool(8)
    p.map(transform, glob.glob('part-r-*.gz'))
