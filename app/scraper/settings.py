from app.common.config import get_config

config = get_config()

BOT_NAME = "wrc_scraper"

SPIDER_MODULES = ["app.scraper.spiders"]
NEWSPIDER_MODULE = "app.scraper.spiders"

ROBOTSTXT_OBEY = False

# --- Concurrency & speed ---
DOWNLOAD_DELAY = config.scraper_download_delay
RANDOMIZE_DOWNLOAD_DELAY = True          # jitter: 0.5x – 1.5x of DOWNLOAD_DELAY
CONCURRENT_REQUESTS = config.scraper_concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = config.scraper_concurrent_requests

# --- AutoThrottle: dynamically adjusts delay to avoid overloading the server ---
AUTOTHROTTLE_ENABLED = config.scraper_autothrottle_enabled
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0
AUTOTHROTTLE_DEBUG = False

# --- Retries ---
RETRY_ENABLED = True
RETRY_TIMES = config.retry_times
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

# --- Timeouts ---
DOWNLOAD_TIMEOUT = config.request_timeout_seconds

# --- Default headers: mimic a real browser ---
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IE,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# --- Middlewares ---
DOWNLOADER_MIDDLEWARES = {
    # Disable the built-in user-agent middleware and replace with ours
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "app.scraper.middlewares.RotateUserAgentMiddleware": 400,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
}

# --- Pipelines ---
ITEM_PIPELINES = {
    "app.scraper.pipelines.DecisionFilePipeline": 300,
}

FEED_EXPORT_ENCODING = "utf-8"
LOG_ENABLED = True
