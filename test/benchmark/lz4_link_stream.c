#define _BSD_SOURCE 1
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#ifdef _WIN32
#include "mman.h"
#else
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

#include <lz4frame.h>

#include "lz4_link_stream.h"

static size_t
file_size(int fd) {
     struct stat fd_st;
     fstat(fd, &fd_st);
     return fd_st.st_size;
}

#define DST_MAX_LEFT (2*sizeof(int64_t) - 1)

static void
lz4_link_stream_set_error(LZ4LinkStream *es, const char *msg) {
     es->state = stream_state_error;
     strncpy(es->error_msg, msg, LZ4_LINK_STREAM_MAX_ERROR_LENGTH);
}

#if 0 // TODO add more descriptive errors
static void
lz4_link_stream_add_error_aux(LZ4LinkStream *es, const char *msg) {
     (void)strncat(
	  es->error_msg,
	  msg,
	  LZ4_LINK_STREAM_MAX_ERROR_LENGTH -
	       strnlen(es->error_msg, LZ4_LINK_STREAM_MAX_ERROR_LENGTH));
}

static void
lz4_link_stream_add_error(LZ4LinkStream *es, const char *msg) {
    lz4_link_stream_add_error_aux(es, ": ");
    lz4_link_stream_add_error_aux(es, msg);
}
#endif

int
lz4_link_stream_new(LZ4LinkStream **es, const char *fname) {
     LZ4LinkStream *p = *es = malloc(sizeof(*p));
     if (!p)
	  return -1;

     p->error_msg[0] = p->error_msg[LZ4_LINK_STREAM_MAX_ERROR_LENGTH] = '\0';

     if (!(p->fname = malloc(strlen(fname) + 1))) {
	  lz4_link_stream_set_error(p, "could not malloc fname");
	  return -1;
     }
     strcpy(p->fname, fname);
     if ((p->fd = open(fname, O_RDONLY)) == -1) {
	  lz4_link_stream_set_error(p, "could not open fname");
	  return -1;
     }

     p->src_size = file_size(p->fd);
     p->src = mmap(0, p->src_size, PROT_READ, MAP_SHARED, p->fd, 0);
     if (p->src == MAP_FAILED) {
	  lz4_link_stream_set_error(p, "mmap failed");
	  return -1;
     }

     if (madvise(p->src, p->src_size, MADV_SEQUENTIAL) != 0) {
	  lz4_link_stream_set_error(p, "madvise failed");
	  return -1;
     }

     LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&(p->ctx), LZ4F_VERSION);
     if (LZ4F_isError(err)) {
	  lz4_link_stream_set_error(p, "error creating decompression context");
	  return -1;
     }

     LZ4F_frameInfo_t frame_info;
     size_t src_size = p->src_size;
     p->src_block_size = LZ4F_getFrameInfo(p->ctx, &frame_info, p->src, &src_size);
     p->src_read = src_size;
     if (LZ4F_isError(p->src_block_size)) {
	  lz4_link_stream_set_error(p, "error getting frame info");
	  return -1;
     }

     p->dst_read = 0;
     p->dst_size = 0;
     switch(frame_info.blockSizeID) {
	  /* LZ4_default is defined inside lz4frame.c as 64KB*/
     case LZ4F_default:
     case max64KB:
	  p->dst_max_size = 64 * KB;
	  break;
     case max256KB:
	  p->dst_max_size = 256 * KB;
	  break;
     case max1MB:
	  p->dst_max_size = 1 * MB;
	  break;
     case max4MB:
	  p->dst_max_size = 4 * MB;
	  break;
     default:
	  /* no way */
	  lz4_link_stream_set_error(p, "unknown block size");
	  return -1;
     }
     /* Add the maximum number of bytes that could be left unread
	before decompressing another block of data */
     p->dst_max_size += DST_MAX_LEFT;
     if (!(p->dst = malloc(p->dst_max_size))) {
	  lz4_link_stream_set_error(p, "could not malloc destination buffer");
	  return -1;
     }

     p->next.from = 0;
     p->next.to = 0;

     return 0;
}


StreamState
lz4_link_stream_next(void *st, Link *next) {
     LZ4LinkStream *es = st;
     // Remaning bytes in destination buffer
     size_t bytes_in_dst = es->dst_size - es->dst_read;
     // Fill additional data inside dst buffer
     while (bytes_in_dst < DST_MAX_LEFT) {
	  // More data inside file
	  if (es->src_read < es->src_size) {
	       // Move reamining bytes to beginning of buffer
	       for (size_t i=0; i<bytes_in_dst; ++i)
		    es->dst[i] = es->dst[es->dst_read + i];

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
		    lz4_link_stream_set_error(es, LZ4F_getErrorName(es->src_block_size));
		    return stream_state_error;
	       }
	       es->dst_size += bytes_in_dst;
	       bytes_in_dst = es->dst_size - es->dst_read;
	  } else if (bytes_in_dst == 0) {
	       return stream_state_end;
	  } else {
	       /* If no more data is available no remaining bytes can be left */
	       lz4_link_stream_set_error(es, "unexpected end of file");
	       return stream_state_error;
	  }
     }
     // Return edges from decompressed dst buffer
     int64_t *off_from = (int64_t*)(es->dst + es->dst_read);
     int64_t *off_to = off_from + 1;

     es->next.from += *off_from;
     es->next.to += *off_to;
     es->dst_read += 2*sizeof(int64_t);

     *next = es->next;

     return stream_state_next;
}

StreamState
lz4_link_stream_reset(void *st) {
     LZ4LinkStream *es = st;
     char *fname = malloc(strlen(es->fname) + 1);
     strcpy(fname, es->fname);
     lz4_link_stream_delete(es);

     if (lz4_link_stream_new(&es, fname) != 0)
	  return stream_state_error;

     return 0;
}

int
lz4_link_stream_delete(LZ4LinkStream *es) {
     if (munmap(es->src, es->src_size) != 0) {
	  lz4_link_stream_set_error(es, "could not munmap");
	  return -1;
     }
     if (close(es->fd) != 0) {
	  lz4_link_stream_set_error(es, "could not close file");
	  return -1;
     }
     if (LZ4F_isError(LZ4F_freeDecompressionContext(es->ctx))) {
	  lz4_link_stream_set_error(es, "freeing decompression context");
	  return -1;
     }
     free(es->dst);
     free(es->fname);
     free(es);

     return 0;
}
