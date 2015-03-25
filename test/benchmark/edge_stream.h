#ifndef _EDGE_STREAM_H
#define _EDGE_STREAM_H

#include <stdint.h>
#include <stdio.h>
#include <lz4frame.h>
#include <stdbool.h>

typedef struct {
     int64_t from;
     int64_t to;
} Edge;

typedef struct {
     int fd;
     uint8_t *src;
     uint8_t *dst;

     size_t src_read;
     size_t src_size;
     size_t src_block_size;

     size_t dst_read;
     size_t dst_max_size;
     size_t dst_size;

     LZ4F_decompressionContext_t ctx;

     Edge next;
     
     const char *error_name;
} EdgeStream;

typedef enum {
     EDGE_STREAM_NEXT,
     EDGE_STREAM_END,
     EDGE_STREAM_ERROR
} EdgeStreamState;

EdgeStream *
edge_stream_new(int fd);

bool
edge_stream_init(EdgeStream *es, int fd);

EdgeStreamState
edge_stream_next(EdgeStream *es, Edge *next);

void
edge_stream_delete(EdgeStream *es);

void
edge_stream_close(EdgeStream *es);

#endif /* _EDGE_STREAM_H */
