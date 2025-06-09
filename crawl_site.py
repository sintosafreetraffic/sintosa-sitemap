import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import datetime
import time
import boto3
import subprocess
BASE_URL = 'https://sintosa.de'
SITEMAP_FILE = 'sitemap.xml'
CRAWL_LIMIT = 10000
REQUEST_DELAY = 0.5

# S3 Config
S3_BUCKET = 'your-s3-bucket-name'      # e.g. 'sintosa-sitemaps'
S3_KEY = 'sitemap.xml'                 # The file name in S3
S3_REGION = 'eu-central-1'             # Change to your AWS S3 region

# Make sure you have AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your environment,
# or configure AWS CLI with `aws configure`.

def is_internal(url, base_netloc):
    return urlparse(url).netloc == base_netloc or urlparse(url).netloc == ""

def normalize_url(url):
    parsed = urlparse(url)
    clean = parsed._replace(fragment="").geturl()
    if clean.endswith('/') and clean != BASE_URL + '/':
        clean = clean[:-1]
    return clean

def crawl_site(start_url):
    print(f"[DEBUG] Entered crawl_site for: {start_url}")
    base_netloc = urlparse(start_url).netloc
    to_visit = set([start_url])
    visited = set()
    all_urls = set()
    session = requests.Session()

    crawl_round = 0
    print(f"[INIT] Starting crawl at {start_url}")
    while to_visit and len(visited) < CRAWL_LIMIT:
        crawl_round += 1
        url = to_visit.pop()
        url = normalize_url(url)
        if url in visited:
            print(f"[SKIP] Already visited: {url}")
            continue
        print(f"\n[VISIT {len(visited)+1}/{CRAWL_LIMIT}] Fetching: {url}")
        print(f"        Visited: {len(visited)} | Queue: {len(to_visit)} | Round: {crawl_round}")
        visited.add(url)
        all_urls.add(url)
        try:
            print(f"[DEBUG] Preparing to request: {url}")
            resp = session.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            })
            print(f"[DEBUG] Got response: {resp.status_code}")
            if not resp.ok or "text/html" not in resp.headers.get("Content-Type", ""):
                print(f"[SKIP] Non-HTML or Bad response at: {url} (type: {resp.headers.get('Content-Type')})")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            found_links = 0
            for link in soup.find_all("a", href=True):
                href = link["href"].strip()
                if href.startswith(("mailto:", "tel:", "javascript:")):
                    print(f"    [SKIP LINK] Non-HTTP ({href})")
                    continue
                abs_url = urljoin(BASE_URL, href)
                abs_url = normalize_url(abs_url)
                if not is_internal(abs_url, base_netloc):
                    print(f"    [SKIP LINK] External ({abs_url})")
                    continue
                if abs_url in visited or abs_url in to_visit:
                    print(f"    [SKIP LINK] Already queued or visited: {abs_url}")
                    continue
                to_visit.add(abs_url)
                found_links += 1
                print(f"    [ADD LINK] Queued for visit: {abs_url}")
            print(f"[INFO] {url}: Found {found_links} new links. Queue: {len(to_visit)}")
            print(f"[SLEEP] Pausing {REQUEST_DELAY} sec to be polite...")
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            print(f"[ERROR] Visiting {url}: {e}")

    print(f"\n[DONE] Crawl finished. Total visited: {len(visited)} URLs.\n")
    return sorted(all_urls)

def generate_sitemap(urls, outfile=SITEMAP_FILE):
    print(f"[SITEMAP] Writing sitemap to {outfile} ({len(urls)} URLs)")
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
    print(f"\n✅ Sitemap generated: {outfile} ({len(urls)} URLs)\n")

def upload_to_s3(local_file, bucket, key, region):
    print(f"[S3] Uploading {local_file} to s3://{bucket}/{key}")
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.upload_file(local_file, bucket, key, ExtraArgs={'ACL': 'public-read', 'ContentType': 'application/xml'})
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
        print(f"[S3] ✅ Uploaded successfully! Public URL: {url}")
        return url
    except Exception as e:
        print(f"[S3] ❌ Upload failed: {e}")
        return None

if __name__ == "__main__":
    print(f"[START] Crawling: {BASE_URL}")
    urls = crawl_site(BASE_URL)
    print(f"\n[RESULT] Total unique URLs found: {len(urls)}\n")
    generate_sitemap(urls)
    # --- Upload to S3 ---
    sitemap_url = upload_to_s3(SITEMAP_FILE, S3_BUCKET, S3_KEY, S3_REGION)
    if sitemap_url:
        print(f"\n[SITEMAP PUBLIC URL] → {sitemap_url}")
        print("Submit this URL to Google Search Console as an extra sitemap!\n")
    else:
        print("Failed to upload sitemap. Please check credentials and bucket configuration.")

    print("[INFO] Sitemap generated, now pushing to GitHub repo...")

    result = subprocess.run(['python3', 'upload_only.py'])
    if result.returncode == 0:
        print("[INFO] Sitemap pushed successfully!")
    else:
        print("[ERROR] Failed to push sitemap to GitHub repo.")
