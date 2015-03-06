#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <malloc.h>
#include <stdbool.h>
#include <lz4frame.h>

#include "edge_stream.h"

/* Return number of bytes available */
size_t
byte_buffer_available(const ByteBuffer *bb) {
     return bb->max_size - bb->size;
}

/* Return number of bytes unread */
size_t
byte_buffer_unread(const ByteBuffer *bb) {
     return bb->size - bb->read;
}

/* Get a pointer to the current buffer position, the next
   element to be read */
uint8_t*
byte_buffer_position(const ByteBuffer *bb) {
     return bb->start + bb->read;
}

/* A pointer to the end of the contents */
uint8_t*
byte_buffer_end(const ByteBuffer *bb) {
     return bb->start + bb->size;
}

/* Allocate memory and initialize fields for an empty buffer */
void
byte_buffer_new(ByteBuffer *bb, size_t max_size) {
     bb->max_size = max_size;
     bb->size = 0;
     bb->read = 0;

     bb->start = (uint8_t*)calloc(max_size, sizeof(uint8_t));
}

/* Free memory */
void
byte_buffer_delete(ByteBuffer *bb) {
     free(bb->start);
     bb->max_size = 0;
     bb->size = 0;
     bb->read = 0;
}

/* Empty contents 
           max_size = 7
   |---------------------------|
   size = 0
   ||
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | x | x | x | x | x | x | x |
   +---+---+---+---+---+---+---+     
   ||
   read = 0
*/
void
byte_buffer_reset(ByteBuffer *bb) {
     bb->size = 0;
     bb->read = 0;
}


/* Move unread data to beginning of buffer 

   BEFORE:
            size = 6
   |-----------------------|
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | a | b | c | d | e | f | x |
   +---+---+---+---+---+---+---+     
     R   R   R   R
   |---------------|-------|
        read = 4    unread = 2

  AFTER:

    size = unread = 2
   |-------|
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | e | f | x | x | x | x | x |
   +---+---+---+---+---+---+---+     
   ||
   read = 0

   Notice that 'unread' remains constant

*/
void
byte_buffer_clean(ByteBuffer *bb) {
     if (bb->read > 0) {
	  const size_t new_size = byte_buffer_unread(bb);
	  memmove(bb->start, byte_buffer_position(bb), new_size);
	  bb->size = new_size;
	  bb->read = 0;
     }
}

/* Mark the buffer as bigger. This function es called
   after appending new data to the end of the buffer */
void
byte_buffer_mark_write(ByteBuffer *bb, size_t n) {
     bb->size += n;
     assert(bb->size <= bb->max_size);
}

/* Advance current position inside buffer. Return true number
   of advanced positions */
size_t
byte_buffer_mark_read(ByteBuffer *bb, size_t n) {
     const size_t start = bb->read;
     bb->read += n;
     if (bb->read > bb->size) {
	  bb->read = bb->size;
     }
     return bb->read - start;
}

/* Read contents of file until buffer is filled 

  BEFORE:

  file: A B C D E F G H
        *  

            size = 5
   |-------------------|
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | a | b | c | d | e | x | x |
   +---+---+---+---+---+---+---+     
     R   R   R  
   |-----------|
      read = 3  

  AFTER:

  file: A B C D E F G H
                  *  

         size = max_size = 7
   |---------------------------|
     0   1   2   3   4   5   6
   +---+---+---+---+---+---+---+
   | d | e | A | B | C | D | E |
   +---+---+---+---+---+---+---+     
   ||
   read = 0  
*/
size_t
byte_buffer_read_file(ByteBuffer *bb, FILE *file) {
     /* Move unread contents to beginning of buffer */
     byte_buffer_clean(bb);
     /* Add new contents to buffer */
     size_t nread = fread(
	  byte_buffer_end(bb), 
	  sizeof(uint8_t), 
	  byte_buffer_available(bb),
	  file);

     byte_buffer_mark_write(bb, nread);
     return nread;
}



void
buffered_lz4_new(BufferedLZ4* blz4, const char *filename, int src_size, int dst_size) {
     blz4->file = fopen(filename, "rb");

     byte_buffer_new(&(blz4->src), src_size);
     byte_buffer_new(&(blz4->dst), dst_size);
     LZ4F_createDecompressionContext(&(blz4->ctx), LZ4F_VERSION);
}

void
buffered_lz4_delete(BufferedLZ4 *blz4) {
     fclose(blz4->file);

     LZ4F_freeDecompressionContext(blz4->ctx);
     byte_buffer_delete(&(blz4->src));
     byte_buffer_delete(&(blz4->dst));
}

/* Get a pointer to the next unread uncompressed byte */
uint8_t*
buffered_lz4_position(const BufferedLZ4 *blz4) {
     return byte_buffer_position(&(blz4->dst));
}

/* How many uncompressed unread bytes remain */
size_t
buffered_lz4_unread(const BufferedLZ4 *blz4) {
     return byte_buffer_unread(&(blz4->dst));
}

/* Mark n uncompressed bytes as read */
size_t
buffered_lz4_mark_read(BufferedLZ4 *blz4, size_t n) {
     return byte_buffer_mark_read(&(blz4->dst), n);
}

/* Decompress more bytes, if possible */
void
buffered_lz4_fill(BufferedLZ4 *blz4) {
     const LZ4F_decompressOptions_t dopts = { .stableDst = 0 };

     /* We need to feed more data */
     byte_buffer_clean(&(blz4->dst));
     (void)byte_buffer_read_file(&(blz4->src), blz4->file);    
     size_t dst_size = byte_buffer_available(&(blz4->dst));
     size_t src_size = blz4->src.size;

     /* TODO 
	This function return value, as was as src_size, gives a hint
	of a good source size parameter for next calls, in order to 
	improve performance. Right now is ignored, which is perfectly valid
	but should be analyzed further.
      */
     (void)LZ4F_decompress(blz4->ctx, 
			   byte_buffer_end(&(blz4->dst)), &dst_size, 
			   blz4->src.start, &src_size, 
			   &dopts);	       

     /* Note that dst_size and src_size can be less than requested */
     byte_buffer_mark_write(&(blz4->dst), dst_size);
     byte_buffer_mark_read(&(blz4->src), src_size);
}


#define MB (1024 * 1024)
void
edge_stream_new(EdgeStream *es, const char *filename) {
     es->off_from = 0;
     es->off_to = 0;
     buffered_lz4_new(&(es->blz4), filename, 25*MB, 100*MB);
}

void
edge_stream_delete(EdgeStream *es) {
     buffered_lz4_delete(&(es->blz4));
}

/* Read next edge inside next. If success return true, otherwise no more data is
   available and returns false */
bool
edge_stream_next(EdgeStream *es, Edge *next) {
     size_t unread_edges = buffered_lz4_unread(&(es->blz4)) / sizeof(Edge);
     if (unread_edges == 0) {
	  buffered_lz4_fill(&(es->blz4));    
	  unread_edges = buffered_lz4_unread(&(es->blz4)) / sizeof(Edge);
	  if (unread_edges == 0) {
	       return false;
	  }
     }
     Edge *offset = (Edge*)buffered_lz4_position(&(es->blz4));
     next->from = es->off_from + offset->from;
     next->to = es->off_to + offset->to;
     es->off_from = next->from;
     es->off_to = next->to;

     buffered_lz4_mark_read(&(es->blz4), sizeof(Edge));

     return true;
}
