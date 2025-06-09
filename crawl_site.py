import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import datetime
import time
import boto3
import subprocess
import logging
import urllib.robotparser
from collections import deque

# Constants
BASE_URL = 'https://sintosa.de'
SITEMAP_FILE = 'sitemap.xml'
CRAWL_LIMIT = 10000
REQUEST_DELAY = 0.5
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " \
             "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

# AWS S3 Config
S3_BUCKET = 'your-s3-bucket-name'
S3_KEY = 'sitemap.xml'
S3_REGION = 'eu-central-1'

# Setup logger
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Initialize robots.txt parser
rp = urllib.robotparser.RobotFileParser()
rp.set_url(urljoin(BASE_URL, "/robots.txt"))
rp.read()

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip('/')
    normalized = parsed._replace(scheme=scheme, netloc=netloc, path=path, query='', fragment='').geturl()
    return normalized

def is_internal(url: str, base_netloc: str) -> bool:
    parsed_netloc = urlparse(url).netloc
    return parsed_netloc == base_netloc or parsed_netloc == ""

def crawl_site(start_url: str):
    base_netloc = urlparse(start_url).netloc
    to_visit = deque([start_url])
    visited = set()
    all_urls = set()
    session = requests.Session()

    logger.info(f"Starting crawl at {start_url}")

    while to_visit and len(visited) < CRAWL_LIMIT:
        url = to_visit.popleft()
        url = normalize_url(url)

        if url in visited:
            logger.debug(f"Already visited: {url}")
            continue

        # Check robots.txt permission
        if not rp.can_fetch(USER_AGENT, url):
            logger.info(f"Disallowed by robots.txt: {url}")
            continue

        logger.info(f"Visiting ({len(visited)+1}/{CRAWL_LIMIT}): {url}")
        visited.add(url)
        all_urls.add(url)

        try:
            resp = session.get(url, timeout=10, headers={"User-Agent": USER_AGENT})
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
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            logger.error(f"Error visiting {url}: {e}")

    logger.info(f"Crawl finished. Visited {len(visited)} URLs")
    return sorted(all_urls)

def generate_sitemap(urls, outfile=SITEMAP_FILE):
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

def upload_to_s3(local_file, bucket, key, region):
    logger.info(f"Uploading {local_file} to s3://{bucket}/{key}")
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.upload_file(local_file, bucket, key, ExtraArgs={'ACL': 'public-read', 'ContentType': 'application/xml'})
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        logger.info(f"Upload successful! Public URL: {url}")
        return url
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return None

if __name__ == "__main__":
    logger.info(f"Starting crawl of: {BASE_URL}")
    urls = crawl_site(BASE_URL)
    logger.info(f"Total unique URLs found: {len(urls)}")

    generate_sitemap(urls)

    sitemap_url = upload_to_s3(SITEMAP_FILE, S3_BUCKET, S3_KEY, S3_REGION)
    if sitemap_url:
        logger.info(f"Sitemap public URL: {sitemap_url}")
        logger.info("Submit this URL to Google Search Console!")
    else:
        logger.error("Failed to upload sitemap. Check credentials and bucket configuration.")

    logger.info("Pushing sitemap to GitHub repo...")
    result = subprocess.run(['python3', 'upload_only.py'])
    if result.returncode == 0:
        logger.info("Sitemap pushed successfully!")
    else:
        logger.error("Failed to push sitemap to GitHub repo.")
