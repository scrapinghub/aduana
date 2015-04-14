#ifndef __LINK_STREAM_H__
#define __LINK_STREAM_H__

typedef struct {
     int64_t from;
     int64_t to;
} Link;

typedef enum {
     link_stream_state_init, /**< Stream ready */
     link_stream_state_next, /**< A new element has been obtained */
     link_stream_state_end,  /**< No more elements */
     link_stream_state_error /**< Unexpected error */
} LinkStreamState;

typedef LinkStreamState (LinkStreamNextFunc)(void *state, Link *link);
typedef LinkStreamState (LinkStreamResetFunc)(void *state);

#endif // __LINK_STREAM_H__
