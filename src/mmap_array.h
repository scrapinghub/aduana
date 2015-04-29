#ifndef _MMAP_ARRAY_H
#define _MMAP_ARRAY_H

#include <stdlib.h>
#include "util.h"

/// @addtogroup MMapArray
/// @{

typedef enum {
     mmap_array_error_ok = 0,         /**< No error */
     mmap_array_error_memory,         /**< Error allocation memory */
     mmap_array_error_internal,       /**< Unexpected error */
     mmap_array_error_mmap,           /**< Error with a mmap operation (creation, unmapping, advise...) */
     mmap_array_error_file,           /**< Error manipulating the file system */
     mmap_array_error_out_of_bounds   /**< Tried to access array past boundaries */

} MMapArrayError;

#define MMAP_ARRAY_DEFAULT_PERSIST 0 /**< Default value for @ref MMapArray::persist */
/** A memory mapped array */
typedef struct {
     char *mem;            /**< Pointer to data */
     int fd;               /**< File descriptor for data */
     char *path;           /**< Path to data file */
     size_t n_elements;    /**< Number of elements */
     size_t element_size;  /**< Size of each element */

     Error *error;

// Options
// -----------------------------------------------------------------------------
     /** If true, do not delete files after deleting object*/
     int persist;
} MMapArray;

/** Create a new MMapArray
 *
 * @param marr Will be changed to point to the newly allocated structure, or NULL if failure
 * @param path Path to the associated file. Can be NULL in which case the mapping is made anonymous.
 * @param n_elements Number of elements (can be changed later with @ref mmap_array_resize)
 * @param element_size Number of bytes of each element
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr if not NULL)
 */
MMapArrayError
mmap_array_new(MMapArray **marr,
               const char *path,
               size_t n_elements,
               size_t element_size);

/** Delete MMapArray
 *
 * If the structure cannot be deleted, the memory will not be freed
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr)
 */
MMapArrayError
mmap_array_delete(MMapArray *marr);

/** Advise memory use pattern
 *
 *  It accepts any flag that madvise accepts
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr)
 */
MMapArrayError
mmap_array_advise(MMapArray *marr, int flag);

/** Force memory-disk syncronization
 *
 * It accepts any flag that msync accepts
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr)
 */
MMapArrayError
mmap_array_sync(MMapArray *marr, int flag);

/** Returns pointer to the array element
 *
 * @return In case of failure it will return NULL. The error code is available in @ref marr
 */
void *
mmap_array_idx(MMapArray *marr, size_t n);

/** Set array element value
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr)
 */
MMapArrayError
mmap_array_set(MMapArray *marr, size_t n, const void *x);

/** Set all elements of array to zero */
void
mmap_array_zero(MMapArray *marr);

/** Change number of elements
 *
 * The new memort is initialized to 0
 *
 * @return 0 if success, otherwise the error code (also available in @ref marr)
 */
MMapArrayError
mmap_array_resize(MMapArray *marr, size_t n_elements);

/// @}

#endif // _MMAP_ARRAY
