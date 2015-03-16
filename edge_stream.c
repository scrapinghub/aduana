#define _BSD_SOURCE 1

#include <assert.h>
#include <malloc.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include <lz4frame.h>

#include "edge_stream.h"

#define KB 1024
#define MB (1024 * KB)

static size_t
file_size(int fd) {
     struct stat fd_st;
     fstat(fd, &fd_st);
     return fd_st.st_size;
}

#define DST_MAX_LEFT (2*sizeof(int64_t) - 1)

EdgeStream *
edge_stream_new(int fd) {
     EdgeStream *es = (EdgeStream*)malloc(sizeof(EdgeStream));
     if (!edge_stream_init(es, fd)) {
	  free(es);
	  return 0;
     }
     return es;
}

bool
edge_stream_init(EdgeStream *es, int fd) {
     es->src_size = file_size(fd);
     es->src = mmap(0, es->src_size, PROT_READ, MAP_SHARED, fd, 0);
     if (es->src == MAP_FAILED) {
	  return false;
     }
     /* TODO check error code */
     madvise(es->src, es->src_size, MADV_SEQUENTIAL);     
     madvise(es->src, es->src_size, MADV_HUGEPAGE);     

     LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&(es->ctx), LZ4F_VERSION);
     if (LZ4F_isError(err)) {
	  return false;
     }

     LZ4F_frameInfo_t frame_info;
     size_t src_size = es->src_size;
     es->src_block_size = LZ4F_getFrameInfo(es->ctx, &frame_info, es->src, &src_size);
     es->src_read = src_size;
     if (LZ4F_isError(es->src_block_size)) {
	  return false;	  
     }

     es->dst_read = 0;
     es->dst_size = 0;    
     switch(frame_info.blockSizeID) {
	  /* LZ4_default is defined inside lz4frame.c as 64KB*/
     case LZ4F_default:
     case max64KB:
	  es->dst_max_size = 64 * KB;
	  break;
     case max256KB:
	  es->dst_max_size = 256 * KB;
	  break;
     case max1MB:
	  es->dst_max_size = 1 * MB;
	  break;
     case max4MB:
	  es->dst_max_size = 4 * MB;
	  break;
     default:
	  /* no way */
	  return false;
     }
     /* Add the maximum number of bytes that could be left unread
	before decompressing another block of data */
     es->dst_max_size += DST_MAX_LEFT;
     es->dst = (uint8_t*)malloc(es->dst_max_size);     

     es->next.from = 0;
     es->next.to = 0;

     return true;
}

EdgeStreamState
edge_stream_next(EdgeStream *es, Edge *next) {    
     // Remaning bytes in destination buffer
     size_t bytes_in_dst = es->dst_size - es->dst_read;      
     // Fill additional data inside dst buffer
     while (bytes_in_dst < DST_MAX_LEFT) {
	  // More data inside file
	  if (es->src_read < es->src_size) {
	       // Move reamining bytes to beginning of buffer
	       for (size_t i=0; i<bytes_in_dst; ++i) {
		    es->dst[i] = es->dst[es->dst_read + i];
	       }
	       es->dst_read = 0;
	       es->dst_size = es->dst_max_size - bytes_in_dst; // temporarily, just for LZ4F_decompress 
	       size_t src_read = es->src_block_size;
	       es->src_block_size = LZ4F_decompress(
		    es->ctx,                // LZ4F contxt
		    es->dst + bytes_in_dst, // output buffer
		    &(es->dst_size),        // size of output, overwritten with actual written bytes
		    es->src + es->src_read, // input buffer
		    &src_read,              // size of input, overwritten with actual read bytes
		    0);                     // options, can be NULL
	       es->src_read += src_read;

	       if (LZ4F_isError(es->src_block_size)) {
		    es->error_name = LZ4F_getErrorName(es->src_block_size);
		    return EDGE_STREAM_ERROR;
	       }
	       es->dst_size += bytes_in_dst;
	       bytes_in_dst = es->dst_size - es->dst_read;
	  } else if (bytes_in_dst == 0) {
	       return EDGE_STREAM_END;
	  } else {
	       /* If no more data is available no remaining bytes can be left */
	       es->error_name = "Unexpected end of file";
	       return EDGE_STREAM_ERROR;
	  }
     }
     // Return edges from decompressed dst buffer
     int64_t *off_from = (int64_t*)(es->dst + es->dst_read);
     int64_t *off_to = ((int64_t*)off_from) + 1;

     es->next.from += *off_from;
     es->next.to += *off_to;
     es->dst_read += 2*sizeof(int64_t);

     *next = es->next;	  

     return EDGE_STREAM_NEXT;
}

void
edge_stream_delete(EdgeStream *es) {
     edge_stream_close(es);
     free(es);
}

void
edge_stream_close(EdgeStream *es) {
     munmap(es->src, es->src_size);
     free(es->dst);
     LZ4F_freeDecompressionContext(es->ctx);     
}

