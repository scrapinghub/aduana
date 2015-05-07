#ifndef __SCORER_H__
#define __SCORER_H__

#include "page_db.h"

/** @addtogroup Scorer
 *
 * Scorers are responsible of computing a measure between 0 and 1 of the
 * relevance of a given page.
 *
 * In order to be used in different schedulers they must obey the following interface.
 * @{
 */

typedef int (ScorerUpdateFunc)(void *state);
typedef int (ScorerAddFunc)(void *state, const PageInfo *page_info, float *score);
typedef int (ScorerGetFunc)(void *state, size_t idx, float *score_old, float *score_new);

typedef struct {
     /** Scorer specific state */
     void *state;

     ScorerUpdateFunc *update; /**< Update scorer */
     ScorerAddFunc *add;       /**< Add new page to scorer */
     ScorerGetFunc *get;       /**< Get a page score */
} Scorer;

/// @}

#endif // __SCORER_H__
