import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import datetime
import time
import subprocess
import logging
import urllib.robotparser
from collections import deque
from requests.adapters import HTTPAdapter, Retry
from typing import Set, List

# Constants
BASE_URL = 'https://sintosa.de'
SITEMAP_FILE = 'sitemap.xml'
CRAWL_LIMIT = 10000
REQUEST_DELAY = 0.5
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

# Setup logger
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def setup_robots_parser(base_url: str) -> urllib.robotparser.RobotFileParser:
    rp = urllib.robotparser.RobotFileParser()
    robots_url = urljoin(base_url, "/robots.txt")
    rp.set_url(robots_url)
    try:
        rp.read()
        logger.info(f"Robots.txt loaded from {robots_url}")
    except Exception as e:
        logger.warning(f"Failed to load robots.txt: {e}")
    return rp

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    # Remove trailing slash except for root URL
    path = parsed.path if parsed.path == "/" else parsed.path.rstrip('/')
    normalized = parsed._replace(scheme=scheme, netloc=netloc, path=path, query='', fragment='').geturl()
    return normalized

def is_internal(url: str, base_netloc: str) -> bool:
    parsed_netloc = urlparse(url).netloc
    return parsed_netloc == base_netloc or parsed_netloc == ""

def crawl_site(start_url: str, crawl_limit: int, request_delay: float, user_agent: str) -> List[str]:
    base_netloc = urlparse(start_url).netloc
    to_visit = deque([start_url])
    visited: Set[str] = set()
    all_urls: Set[str] = set()
    session = requests.Session()

    # Add retry strategy for robustness
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    rp = setup_robots_parser(start_url)

    logger.info(f"Starting crawl at {start_url}")

    while to_visit and len(visited) < crawl_limit:
        url = to_visit.popleft()
        url = normalize_url(url)

        if url in visited:
            logger.debug(f"Already visited: {url}")
            continue

        # Check robots.txt permission
        if not rp.can_fetch(user_agent, url):
            logger.info(f"Disallowed by robots.txt: {url}")
            continue

        logger.info(f"Visiting ({len(visited)+1}/{crawl_limit}): {url}")
        visited.add(url)
        all_urls.add(url)

        try:
            resp = session.get(url, timeout=10, headers={"User-Agent": user_agent})
            if not resp.ok or "text/html" not in resp.headers.get("Content-Type", ""):
                logger.info(f"Skipping non-HTML or bad response: {url}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            found_links = 0

            for link in soup.find_all("a", href=True):
                href = link["href"].strip()
                if href.startswith(("mailto:", "tel:", "javascript:")):
                    continue
                abs_url = urljoin(BASE_URL, href)
                abs_url = normalize_url(abs_url)
                if not is_internal(abs_url, base_netloc):
                    continue
                if abs_url in visited or abs_url in to_visit:
                    continue
                to_visit.append(abs_url)
                found_links += 1

            logger.info(f"Found {found_links} new links, queue size: {len(to_visit)}")
            time.sleep(request_delay)

        except Exception as e:
            logger.error(f"Error visiting {url}: {e}")

    logger.info(f"Crawl finished. Visited {len(visited)} URLs")
    return sorted(all_urls)

def generate_sitemap(urls: List[str], outfile: str = SITEMAP_FILE) -> None:
    logger.info(f"Writing sitemap to {outfile} ({len(urls)} URLs)")
    with open(outfile, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        today = datetime.date.today().isoformat()
        for url in urls:
            f.write('  <url>\n')
            f.write(f'    <loc>{url}</loc>\n')
            f.write(f'    <lastmod>{today}</lastmod>\n')
            f.write('    <changefreq>weekly</changefreq>\n')
            f.write('    <priority>0.5</priority>\n')
            f.write('  </url>\n')
        f.write('</urlset>\n')
    logger.info(f"Sitemap generated: {outfile} ({len(urls)} URLs)")

def main():
    logger.info(f"Starting crawl of: {BASE_URL}")
    urls = crawl_site(BASE_URL, CRAWL_LIMIT, REQUEST_DELAY, USER_AGENT)
    logger.info(f"Total unique URLs found: {len(urls)}")

    generate_sitemap(urls)

    subprocess.run(['python3', 'upload_only.py'])

if __name__ == "__main__":
    main()
