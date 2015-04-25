#ifndef __SCORER_H__
#define __SCORER_H__

#include "page_db.h"

/// @addtogroup Scorer
/// @{
                                                        
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
