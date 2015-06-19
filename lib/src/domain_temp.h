#ifndef __DOMAIN_TEMP_H__
#define __DOMAIN_TEMP_H__

#define _POSIX_C_SOURCE 200809L
#define _BSD_SOURCE 1
#define _GNU_SOURCE 1

#include <stdint.h>
#include <stdlib.h>

/** Associate a domain hash with a temperature */
typedef struct {
     uint32_t hash; /**< Domain hash */
     float temp;    /**< Domain temperature: an estimation of how many times
                     * the domain has been crawled in the time window */
} DomainTempEntry;

/** Tracks how "hot" are the most crawled domains.
 *
 * We want to avoid crawling the same domain repeatedly. For this purpose this
 * structure tracks how many times a domain has been crawled in the specified
 * time window. For performance reasons an approximation of the actual number
 * of crawls is maintained. Under certain assumptions it can be shown that
 * if 'n' is the number of crawled for a domain it follows the following (cool down)
 * differential equation:
 * @f[
      \frac{dn}{dt} = -\frac{1}{T}n
   @f]
 *
 * where @f$T@f$ is the time window.
 */
typedef struct {
     DomainTempEntry *table; /**< An array of domain/temperature pairs */
     size_t length;          /**< Length of @ref DomainTemp::table */

     float time;   /**< Last time temperatures were updated */
     float window; /**< Time window to consider in the cooldown */
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

/** Adds another count to domain.
 *
 * If the domain already in already tracked its counter is incremented.
 * If the domain is not present then we try to initialize it in an empty slot.
 * If not empty slot is available then the domain with fewest crawls is replaced
 * with the new domain if its counter is below 1.
 * */
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
