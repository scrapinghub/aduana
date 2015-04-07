#ifndef _LZ4_LINK_STREAM_H
#define _LZ4_LINK_STREAM_H

#include <stdint.h>
#include <stdio.h>
#include <lz4frame.h>

#include "pagedb.h"

#define LZ4_LINK_STREAM_MAX_ERROR_LENGTH 10000

typedef struct {
     char *fname; /**< Filename of the LZ4 file */
     int fd;      /**< Open file descriptor to fname */

     uint8_t *src; /**< Source buffer, mmap of fd */
     uint8_t *dst; /**< Destination buffer, depends on source block size */

     size_t src_read;         /**< Number of bytes read in src */
     size_t src_size;         /**< Total number of bytes in src */
     size_t src_block_size;   /**< Compression block size */

     size_t dst_read;         /**< Number of bytes read in dst */
     size_t dst_max_size;     /**< Allocated memory for dst: dst_size <= dst_max_size */
     size_t dst_size;         /**< Number of bytes decompressed inside dst */

     /** LZ4 frame decompression context. Saves decompression state */
     LZ4F_decompressionContext_t ctx;

     /** Next link. Must be saved because we use delta encoding */
     Link next;

     LinkStreamState state;

     /** In case of error a description will be written here */
     char error_msg[LZ4_LINK_STREAM_MAX_ERROR_LENGTH + 1];
} LZ4LinkStream;

/** Create new stream.
 *
 * @return 0 if success, -1 if failure
 */
int
lz4_link_stream_new(LZ4LinkStream **es, const char *fname);

/** Get next link */
LinkStreamState
lz4_link_stream_next(void *st, Link *next);

/** Reset stream to beginning */
LinkStreamState
lz4_link_stream_reset(void *st);

/** Delete link stream. Close any open file and free all memory if succesfull.
 *
 * If there is a failure the memory will not be freed so that the error message
 * can be checked.
 *
 * @return 0 if success, -1 if failure
 */
int
lz4_link_stream_delete(LZ4LinkStream *es);


#endif /* _LZ4_LINK_STREAM_H */
