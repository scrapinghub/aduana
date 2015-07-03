import aduana
import aduana.frontera

#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
BACKEND = 'aduana.frontera.Backend'
MAX_REQUESTS = 100000000
MAX_NEXT_REQUESTS = 10
DELAY_ON_EMPTY = 0.0
PAGE_DB_PATH = 'test-crawl'
SCORER = aduana.HitsScorer
USE_SCORES = True

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = False
LOGGING_BACKEND_ENABLED = True
LOGGING_DEBUGGING_ENABLED = False
