#ifndef __LINK_STREAM_H__
#define __LINK_STREAM_H__

#include <stdint.h>

typedef struct {
     int64_t from;
     int64_t to;
} Link;

typedef StreamState (LinkStreamNextFunc)(void *state, Link *link);
typedef StreamState (LinkStreamResetFunc)(void *state);

#endif // __LINK_STREAM_H__
