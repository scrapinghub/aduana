#ifndef _EDGE_STREAM_H
#define _EDGE_STREAM_H

#include <stdint.h>
#include <stdio.h>
#include <lz4frame.h>
#include <stdbool.h>

/* A region of memory for intermediate storage

           max_size = 7
   |---------------------------|
           size = 5      available = 2
   |-------------------|-------|

                         * end (pointer to next element to be written)
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | a | b | c | d | e | x | x |
   +---+---+---+---+---+---+---+
     R   R   R   
                 * position (pointer to next element to be read)

   |-----------|-------|
      read = 3   unread = 2

  It must be:
      read <= size <= max_size        


  Legend:
     R -> read element
     x -> garbage
*/
typedef struct {
     uint8_t *start;

     /* Maximum buffer size, in bytes */
     size_t max_size;
     /* Current buffer size, in bytes */
     size_t size;
     /* Number of read elements*/
     size_t read;
} ByteBuffer;

/* LZ4 expects us to decompress from one region of memory to another.
   This structure allow us to move from FILE to compressed memory, 
   and from compressed memory to decompressed memory, while tracking
   which bytes have been already been consumed.

        fread             LZ4 decompress
   file --------> src ----------------------> dst  

*/
typedef struct {
     FILE *file;
     LZ4F_decompressionContext_t ctx;     
     ByteBuffer src;
     ByteBuffer dst;
} BufferedLZ4;

typedef struct {
     int64_t from;
     int64_t to;
} Edge;

typedef struct {
     int64_t off_from;
     int64_t off_to;

     BufferedLZ4 blz4;
} EdgeStream;


void
edge_stream_new(EdgeStream *es, const char *filename);

void
edge_stream_delete(EdgeStream *es);

/* Read next edge inside next. If success return true, otherwise no more data is
   available and returns false */
bool
edge_stream_next(EdgeStream *es, Edge *next);

#endif /* _EDGE_STREAM_H */
