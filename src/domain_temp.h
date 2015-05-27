#ifndef __DOMAIN_TEMP_H__
#define __DOMAIN_TEMP_H__

#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdint.h>
#include <stdlib.h>

typedef struct {
     uint32_t hash;
     float temp;
} DomainTempEntry;

/** Tracks how "hot" are the most crawled domains.
 *
 * We want to avoid crawling the same domain repeatedly. For this purpose this
 * structure tracks how many times a domain has been crawled in the specified
 * time window. For performance reasons an approximation of the actual number
 * of crawls is maintained. Under certain assumptions it can be shown that
 * if 'n' is the number of crawled for a domain it follows the following (cool down)
 * differential equation:
 * @verbatim
      dn     1
      -- = - -n
      dt     T
   @endverbatim
   *
   * where 'T' is the time window.
 */
typedef struct {
     DomainTempEntry *table;
     size_t length;

     float time;
     float window;
} DomainTemp;

/// @addtogroup DomainTemp
/// @{

/** Create a new domain temp tracking structure
 *
 * @param length Maximum number of domains to track
 * @param window Time window
 *
 * @returns A pointer to the new struct of NULL if failure
 */
DomainTemp *
domain_temp_new(size_t length, float window);

/** Updates temp up to current time t */
void
domain_temp_update(DomainTemp *dh, float t);

/** Adds another count to domain */
void
domain_temp_heat(DomainTemp *dh, uint32_t hash);

/** Gets domain temp */
float
domain_temp_get(DomainTemp *dh, uint32_t hash);

/** Free memory */
void
domain_temp_delete(DomainTemp *dh);

/// @}

#if (defined TEST) && TEST
#include "CuTest.h"
CuSuite *
test_domain_temp_suite(void);
#endif
#endif // __DOMAIN_TEMP_H
