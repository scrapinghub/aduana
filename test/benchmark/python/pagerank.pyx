import tempfile
import bcolz
import numpy as np
cimport numpy as np
cimport cython

cdef packed struct Edge:
    np.int64_t start
    np.int64_t end

class PageRank(object):
    def __init__(self, n_pages, damping=0.85):
        self.n_pages = n_pages
        self.damping = damping

        N = int(n_pages)

        self.pr1 = np.memmap('/dev/zram0', dtype=np.float32, mode='w+', shape=(N,))
        self.pr2 = np.memmap('/dev/zram1', dtype=np.float32, mode='w+', shape=(N,))
        self.out_degree = np.memmap('/dev/zram2', dtype=np.float32, mode='w+', shape=(N,))

        #self.pr1 = np.memmap('/tmp/v0', dtype=np.float32, mode='w+', shape=(N,))
        #self.pr2 = np.memmap('/tmp/v1', dtype=np.float32, mode='w+', shape=(N,))
        #self.out_degree = np.memmap('/tmp/v2', dtype=np.float32, mode='w+', shape=(N,))

        self.pr1[:] = 1.0/n_pages
        self.pr2[:] = 0.0
        self.out_degree[:] = 0.0

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def degree(self, edges):
        cdef np.int64_t a = 0
        cdef np.ndarray[Edge, ndim=1] block
        cdef Py_ssize_t i
        cdef np.ndarray[np.float32_t, ndim=1] deg = self.out_degree

        for block in bcolz.iterblocks(edges):
            for i in range(block.shape[0]):
                a += block[i].start
                deg[a] += 1.0

    def begin_updates(self):
        self.pr1 *= self.damping/self.out_degree
        self.pr2[:] = (1.0 - self.damping)/self.n_pages        

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def update(self, edges):
        cdef Py_ssize_t i
        cdef np.int64_t a = 0
        cdef np.int64_t b = 0
        cdef Edge edge
        cdef np.ndarray[Edge, ndim=1] block
        cdef np.ndarray[np.float32_t, ndim=1] pr1 = self.pr1
        cdef np.ndarray[np.float32_t, ndim=1] pr2 = self.pr2

        for block in bcolz.iterblocks(edges):
            for i in range(block.shape[0]):
                edge = block[i]
                a += edge.start
                b += edge.end
                pr2[b] += pr1[a]

    def end_updates(self):
        self.pr1, self.pr2 = self.pr2, self.pr1


