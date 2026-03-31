"""Scrapy downloader middlewares."""

import logging

from fake_useragent import UserAgent, FakeUserAgentError

logger = logging.getLogger(__name__)

# Initialise once at module load — the library fetches a remote UA database on
# first use, so sharing the instance avoids redundant network calls.
try:
    _ua = UserAgent(browsers=["chrome", "firefox", "safari"])
except FakeUserAgentError:
    _ua = None
    logger.warning("fake_useragent database unavailable; falling back to static UA")

_FALLBACK_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class RotateUserAgentMiddleware:
    """Replace the User-Agent header on every outgoing request with a random
    browser string.  This is the primary anti-bot-detection measure for a
    site that doesn't use JavaScript challenges.
    """

    def process_request(self, request, spider):
        ua = _ua.random if _ua is not None else _FALLBACK_UA
        request.headers["User-Agent"] = ua
