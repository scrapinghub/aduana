#ifndef __UTIL_H__
#define __UTIL_H__

#include <pthread.h>
#include <stdint.h>

#include "lmdb.h"

/** Posible states of a stream.
 *
 * @verbatim
 *  ---> Error <-----+
 *        ^          |
 *        |          |
 *  ---> Init ----> Next --+-> End
 *             ^           |
 *             +-----------+
 * @endverbatim
 */
typedef enum {
     stream_state_init, /**< Stream ready */
     stream_state_next, /**< A new element has been obtained */
     stream_state_end,  /**< No more elements */
     stream_state_error /**< Unexpected error */
} StreamState;

/** @addtogroup Error
 *
 * Most error handling is done using this structure. Using @ref error_set and
 * @ref error_add it's possible to give useful error messages for debugging.
 * @{
 */

/** Maximum length of error message */
#define MAX_ERROR_LENGTH 10000

typedef struct {
     /** Make operations on errors atomic.
      *
      * If an error is produced dealing with this mutex it will be silently
      * ignored
      */
     pthread_mutex_t mtx;

     /** Error code, depends on the application but 0 always signals no error */
     int code;
     /** A descriptive message associated with the error code. If no error then
      *  it contains "NO ERROR" */
     char message[MAX_ERROR_LENGTH + 1];
} Error;

/** Initialize structure.
 *
 * Error code is set to 0 and message to "NO ERROR".
 * */
void
error_init(Error *error);

/** Clean up. Will NOT free `error` */
void
error_destroy(Error *error);

/** Allocate and initialize a new error structure */
Error *
error_new(void);

/** Destroy and free an error structure */
void
error_delete(Error *error);

/** Set error.
 *
 * If an error is already present then do nothing. If you want to overwrite an
 * already existing error then first call @ref error_clean
 */
void
error_set(Error* error, int code, const char *msg);

/** Clean error.
 *
 * Error code is set to 0 and the message to NO ERROR.
 */
void
error_clean(Error *error);

/** Add a description message to the existing message and leaves as is the error
 * code */
void
error_add(Error* error, const char *msg);

/** Return error message if error, otherwise NULL */
const char *
error_message(const Error *error);

/** Return error code */
int
error_code(const Error *error);

/// @}

/** Returns a newly allocated string made by concatenating two strings
 * with a separator.
 *
 * @param s1 The first string
 * @param s2 The second string
 * @param separator The separator
 *
 * @return A malloced new string: s1 + separator + s2. NULL if error.
 */
char *
concat(const char *s1, const char *s2, char separator);

/** Concatenates `path` and `fname` with the filesystem separator.
 *
 * @return path + '/' fname
 */
char *
build_path(const char *path, const char *fname);

/** Make a new directory at `path` if not already present.
 *
 * Permissions are set to 0775
 *
 * @return An strerror with a description of the error or NULL if success.
 */
char *
make_dir(const char *path);

/** @addtogroup Varint
 *
 * Variable length integer encoding following Google's
 * [protocol buffers](https://developers.google.com/protocol-buffers/docs/encoding#varints)
 *
 * The main use case of these functions is inside the links database. The reason
 * is that we need 64bit numbers to represent links for very large crawls, however
 * most of the time link  numbers fit in much less than 8 bytes, and if we use
 * delta encoding typically only 1 byte is necessary per link, allowing compression
 * ratios of 8x.
 * @{
 */

/** Encode unsigned 64bit integer using varint encoding.
 *
 * @param n Number to encode
 * @param out Output byte stream
 * @return Pointer at the end of the output byte stream
 * */
uint8_t*
varint_encode_uint64(uint64_t n, uint8_t *out);

/** Decode unsigned 64bit integer using varint encoding
 *
 * @param in Input byte stream
 * @param read Output parameter with the number bytes read from the input byte stream
 * @return The decoded number
 **/
uint64_t
varint_decode_uint64(uint8_t *in, uint8_t* read);

/** Encode signed 64bit integer using varint encoding
 *
 * @param n Number to encode
 * @param out Output byte stream
 * @return Pointer at the end of the output byte stream
 **/
uint8_t*
varint_encode_int64(int64_t n, uint8_t *out);

/** Decode signed 64bit integer using varint encoding
 *
 * @param in Input byte stream
 * @param read Output parameter with the number bytes read from the input byte stream
 * @return The decoded number
 * */
int64_t
varint_decode_int64(uint8_t *in, uint8_t* read);

/// @}

/** Extract domain from a valid http URL */
int
url_domain(const char *url, int *start, int *end);

/** 1 if the two URLs share the same domain */
int
same_domain(const char *url1, const char *url2);

#ifdef _WIN32
void
mkdtemp(char *template);
#endif

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_util_suite(void);
#endif
#endif // __UTIL_H__
