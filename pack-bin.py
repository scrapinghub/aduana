#!/usr/bin/env python

# Drop this script into the directory holding the Common Crawl Corpus 
# Hypergraph and run it. It will transform every part-*.gz file into LZ4
# format.
# 
# Requirements: lz4tools
import glob
import gzip
import struct
import os.path

from multiprocessing import Pool

import lz4f

def transform(pgz):
    nedge = 0

    pz4 = pgz[:-2] + 'lz4'
    if os.path.exists(pz4):
        print 'Skipping', pgz
        return

    print 'Processing', pgz
    with gzip.open(pgz, 'r') as fgz,  open(pz4, 'w') as fz4:
        ctx = lz4f.createCompContext()
        fz4.write(lz4f.compressBegin(ctx))

        nline = 0
        a = 0
        b = 0
        for line in fgz:
            nline += 1
            try:
                if line.strip()[0] == '#':
                    continue
                fields = line.split()
                x = int(fields[0])
                y = int(fields[1])

                # write compression data
                data = lz4f.compressUpdate(struct.pack('<qq', x - a, y - b), ctx)
                if data:
                    fz4.write(data)

                a = x
                b = y

                nedge += 1
                if nedge % 50000000 == 0:
                    print '    {0}: {1}M'.format(pgz, nedge/1000000)
            except Exception,e:
                print 'Error processing line', nline, e
                return

        # finish compression
        fz4.write(lz4f.compressEnd(ctx))
        # destroy compression context
        lz4f.freeCompContext(ctx)

    print '{0}, number of edges: {1}'.format(pgz, nedge)


if __name__ == '__main__':
    p = Pool(8)
    p.map(transform, glob.glob('part-r-*.gz'))
    
    
