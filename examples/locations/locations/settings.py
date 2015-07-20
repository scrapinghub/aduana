#--------------------------------------------------------------------------
# Scrapy Settings
#--------------------------------------------------------------------------
BOT_NAME = 'locations'

SPIDER_MODULES = ['locations.spiders']
NEWSPIDER_MODULE = 'locations.spiders'

HTTPCACHE_ENABLED = True
REDIRECT_ENABLED = True
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 10
RETRY_ENABLED = False
AJAXCRAWL_ENABLED = True

CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 2

LOGSTATS_INTERVAL = 10

SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 500,
}

ITEM_PIPELINES = {
    'locations.pipelines.LocationsPipeline': 500,
}

#--------------------------------------------------------------------------
# Frontier Settings
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 999},
)
DOWNLOADER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 999}
)
SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'
FRONTERA_SETTINGS = 'locations.frontera.settings'


#--------------------------------------------------------------------------
# Seed loaders
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update({
    'frontera.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
})
SEEDS_SOURCE = 'seeds.txt'

#--------------------------------------------------------------------------
# Testing
#--------------------------------------------------------------------------
#CLOSESPIDER_PAGECOUNT = 1
