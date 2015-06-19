#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
BACKEND = 'aduana.frontera.Backend'
MAX_REQUESTS = 100000000
MAX_NEXT_REQUESTS = 10
DELAY_ON_EMPTY = 0.0
PAGE_DB_PATH = 'test-crawl'
SCORER = 'HitsScorer'
USE_SCORES = True

# Only used if BACKEND = 'aduana.frontera.WebBackend'
SERVER_NAME = 'localhost'
SERVER_PORT = 8000
SERVER_CERT = 'aduana-server.crt'
#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = False
LOGGING_BACKEND_ENABLED = False
LOGGING_DEBUGGING_ENABLED = False
