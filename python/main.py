import sys
import bcolz

from pagerank import PageRank

if __name__ == '__main__':
    pr = PageRank(n_pages=4e9)
    print 'Computing pages out degree'

    n_files = len(sys.argv) - 1
    
    k = 0
    for fname in sys.argv[1:]:
        k += 1
        if k % 10 == 0:
            print 'Init [{0: 4d}/{1: 4d}]'.format(k, n_files)


        pr.degree(bcolz.open(fname, mode='r'))


    for i in xrange(4):
        print 'PageRank iter', i
        print '------------------------------'
        pr.begin_updates()
        k = 0
        for fname in sys.argv[1:]:
            k += 1
            if k % 10 == 0:
                print 'Update [{0: 4d}/{1: 4d}]'.format(k, n_files)

            pr.update(bcolz.open(fname, mode='r'))

            
        pr.end_updates()
