"""
Small Business IT Feed Scraper
================================
Aggregates IT news relevant to small and family businesses.
Pure RSS/feedparser — no API keys required.

Categories (filter tabs on the dashboard):
  infrastructure  — On-prem hardware, networking, Wi-Fi
  cloud_saas      — Cloud tools, M365, Google Workspace, SaaS management
  finance         — IT budgeting, ROI, FinOps, leasing vs buying
  continuity      — Disaster recovery, backups, BCP
  policy          — BYOD, compliance, acceptable use, GDPR/PCI for SMBs

Output files (served by Vercel):
  feed.json        page 1 — newest 50 articles
  feed-2.json      page 2  … feed-5.json page 5 (250 total)
  archive.json     full rolling store (not served publicly)
"""

import feedparser
import json
import hashlib
import re
import os
import logging
import time
from datetime import datetime, timezone

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# PAGINATION SETTINGS
# ============================================================================

ARCHIVE_SIZE = 250
PAGE_SIZE    = 50
ARCHIVE_PATH = 'archive.json'

def page_filename(n):
    return 'feed.json' if n == 1 else f'feed-{n}.json'

# ============================================================================
# NEWS SOURCES
# ============================================================================

NEWS_SOURCES = {
    'biztech': {
        'name': 'BizTech Magazine',
        'url':  'https://biztechmagazine.com/rss/small-business',
    },
    'techrepublic': {
        'name': 'TechRepublic SME',
        'url':  'https://www.techrepublic.com/rssfeeds/topic/smb/',
    },
    'zdnet': {
        'name': 'ZDNet Business',
        'url':  'https://www.zdnet.com/topic/business/rss.xml',
    },
    'cioindex': {
        'name': 'CIO Index',
        'url':  'https://cioindex.com/rss-feeds/',
    },
    'smallbiztrends': {
        'name': 'Small Biz Trends',
        'url':  'https://smallbiztrends.com/category/technology/feed',
    },
    'arstechnica': {
        'name': 'Ars Technica',
        'url':  'https://arstechnica.com/information-technology/feed',
    },
    'theregister': {
        'name': 'The Register',
        'url':  'https://www.theregister.com/data_centre/systems/headlines.atom',
    },
    'serverwatch': {
        'name': 'ServerWatch',
        'url':  'https://www.serverwatch.com/feed/',
    },
    'networkworld': {
        'name': 'Network World',
        'url':  'https://www.networkworld.com/category/smb/feed/',
    },
    'infoworld': {
        'name': 'InfoWorld Cloud',
        'url':  'https://www.infoworld.com/cloud-computing/index.rss',
    },
    'awsblog': {
        'name': 'AWS News Blog',
        'url':  'http://feeds.feedburner.com/AmazonWebServicesBlog',
    },
    'continuitycentral': {
        'name': 'Continuity Central',
        'url':  'https://www.continuitycentral.com/index.php/rss-feed',
    },
    'smartermsp': {
        'name': 'Smarter MSP',
        'url':  'https://smartermsp.com/feed',
    },
    'accountingtoday': {
        'name': 'Accounting Today Tech',
        'url':  'https://www.accountingtoday.com/tag/technology.rss',
    },
    'thenewstack': {
        'name': 'The New Stack',
        'url':  'https://thenewstack.io/blog/feed/',
    },
    'malwarebytes': {
        'name': 'Malwarebytes Labs',
        'url':  'https://www.malwarebytes.com/blog/feed',
    },
    'searchstorage': {
        'name': 'SearchStorage',
        'url':  'https://searchstorage.techtarget.com/rss/Storage-for-SMBs.xml',
    },
}

# ============================================================================
# CATEGORY KEYWORDS
# ============================================================================
# Articles are assigned the FIRST category whose keywords match.
# Order here is the priority order — product_review must come first so that
# review/roundup articles don't fall into infrastructure or continuity.

CATEGORY_PRIORITY = [
    'product_review',   # highest priority — catch reviews before topic categories
    'continuity',
    'policy',
    'finance',
    'cloud_saas',
    'infrastructure',
]

CATEGORY_KEYWORDS = {
    'product_review': [
        # Title patterns that signal a review or roundup rather than a guide/news piece
        ' review:', 'review —', '- review', 'hands-on', 'hands on review',
        'best business', 'best smb', 'best small business', 'best for business',
        'we tested', 'i tested', 'i tried', 'we tried',
        'expert tested', 'expert reviewed', 'editors tested',
        'vs.', ' vs ', 'compared', 'comparison',
        'buyer''s guide', 'buyers guide', 'buying guide',
        'roundup', 'round-up', 'top picks', 'top 5', 'top 10',
        'ahead of the pack', 'ahead of the rest',
        'star rating', 'rating out of', '/5 stars', '/10 stars',
        # Consumer electronics that don't belong in a business IT feed
        'airpods', 'headphones', 'earbuds', 'earphones', 'headset review',
        'cleaning kit', 'cleaning tool', 'sparkling clean',
        'smartwatch', 'wearable', 'fitness tracker',
        'gaming headset', 'gaming mouse', 'gaming keyboard',
        'smartphone review', 'tablet review', 'laptop review',
    ],
    'infrastructure': [
        # Kept specific — removed bare 'VoIP' and 'wi-fi' to avoid catching reviews
        'VoIP for business', 'VoIP phone system', 'business VoIP',
        'VoIP deployment', 'VoIP setup', 'hosted PBX',
        'office wi-fi', 'wi-fi 7', 'wi-fi deployment', 'wireless deployment',
        'access point deployment', 'mesh network office',
        'small business NAS', 'NAS drive', 'network attached storage',
        'UPS backup power', 'uninterruptible power supply',
        'LAN setup', 'local area network', 'structured cabling',
        'point of sale', 'POS hardware', 'POS system',
        'server rack', 'patch panel', 'network switch',
        'office router setup', 'on-premise', 'on-prem',
        'hardware refresh', 'printer server',
    ],
    'cloud_saas': [
        'Microsoft 365', 'M365', 'Google Workspace', 'G Suite',
        'SaaS', 'cloud migration', 'AWS for small business', 'Azure SMB',
        'Slack', 'Microsoft Teams', 'cloud cost', 'SaaS sprawl',
        'software as a service', 'cloud subscription', 'identity management',
        'SSO', 'single sign-on', 'OneDrive', 'SharePoint',
        'Dropbox business', 'cloud backup', 'QuickBooks Online',
    ],
    'finance': [
        'IT ROI', 'return on investment', 'TCO', 'total cost of ownership',
        'IT budget', 'FinOps', 'leasing vs buying', 'hardware lease',
        'cost per user', 'managed service pricing', 'IT spending',
        'technology budget', 'capex vs opex', 'cloud cost optimisation',
        'cost optimization', 'IT audit', 'technology spend',
    ],
    'continuity': [
        # Tightened — removed bare 'offsite storage', 'backup strategy', 'data recovery'
        # which were too generic and catching unrelated AWS/storage articles
        'business continuity', 'disaster recovery', 'DR plan', 'DR testing',
        'business continuity plan', 'BCP', 'continuity planning',
        '3-2-1 backup', 'offsite backup', 'air-gapped backup',
        'recovery time objective', 'RTO', 'RPO',
        'ransomware recovery', 'ransomware restore',
        'failover', 'failover testing', 'failback',
        'business interruption', 'incident response plan',
        'backup and recovery', 'backup testing', 'restore testing', 'restore test',
        'disaster recovery plan', 'crisis management plan',
    ],
    'policy': [
        'BYOD', 'bring your own device', 'acceptable use policy',
        'PCI compliance', 'PCI DSS', 'GDPR', 'data protection',
        'remote work policy', 'cybersecurity policy', 'IT governance',
        'compliance framework', 'HIPAA', 'SOC 2', 'employee IT policy',
        'password policy', 'multi-factor authentication policy', 'MFA policy',
        'security awareness training', 'phishing training',
    ],
}


def categorize(title, summary):
    """Return the highest-priority matching category, or None."""
    combined = (title + ' ' + summary).lower()
    for cat in CATEGORY_PRIORITY:
        for kw in CATEGORY_KEYWORDS[cat]:
            if kw.lower() in combined:
                return cat
    return None


# ============================================================================
# HELPERS
# ============================================================================

def clean_text(text):
    if not text:
        return ''
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def make_hash(url):
    return hashlib.sha256(url.encode()).hexdigest()


def parse_date(entry):
    for attr in ('published_parsed', 'updated_parsed'):
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


RSS_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (compatible; SMBITFeed/1.0; '
        '+https://github.com/your-org/smbit-feed)'
    )
}

# ============================================================================
# SCRAPING
# ============================================================================

def fetch_source(source_id, config):
    articles = []
    logging.info(f"  Fetching: {config['name']}")
    try:
        feed = feedparser.parse(config['url'], request_headers=RSS_HEADERS)
        if feed.bozo:
            logging.warning(f"    Feed warning: {feed.bozo_exception}")

        for item in feed.entries:
            try:
                title = clean_text(item.get('title', ''))
                url   = item.get('link', '').strip()
                if not title or not url:
                    continue

                raw = (
                    item.get('summary', '') or
                    item.get('description', '') or
                    (item.get('content') or [{}])[0].get('value', '')
                )
                summary  = clean_text(raw)
                category = categorize(title, summary)
                if category is None:
                    continue

                articles.append({
                    'id':       make_hash(url),
                    'title':    title,
                    'url':      url,
                    'summary':  summary[:300],
                    'source':   config['name'],
                    'category': category,
                    'date':     parse_date(item),
                })
            except Exception as e:
                logging.warning(f"    Skipped entry: {e}")

        logging.info(f"    → {len(articles)} articles")
    except Exception as e:
        logging.error(f"    Failed: {e}")
    return articles


def scrape_all():
    seen   = set()
    result = []
    for sid, cfg in NEWS_SOURCES.items():
        for art in fetch_source(sid, cfg):
            if art['id'] not in seen:
                seen.add(art['id'])
                result.append(art)
        time.sleep(1)
    result.sort(key=lambda a: a['date'], reverse=True)
    return result

# ============================================================================
# ARCHIVE MERGE
# ============================================================================

def load_archive():
    if os.path.exists(ARCHIVE_PATH):
        try:
            with open(ARCHIVE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f).get('articles', [])
        except Exception as e:
            logging.warning(f"Could not read archive: {e}")
    return []


def merge_into_archive(new_articles, existing):
    merged_map = {a['id']: a for a in existing}
    for art in new_articles:
        merged_map[art['id']] = art
    merged = list(merged_map.values())
    merged.sort(key=lambda a: a['date'], reverse=True)
    return merged[:ARCHIVE_SIZE]


def save_archive(articles, updated_ts):
    with open(ARCHIVE_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            'updated':  updated_ts,
            'total':    len(articles),
            'articles': articles,
        }, f, ensure_ascii=False, indent=2)
    logging.info(f"Wrote {ARCHIVE_PATH}  ({len(articles)} articles)")

# ============================================================================
# PAGE FILE OUTPUT
# ============================================================================

def write_page_files(archive, updated_ts):
    total     = len(archive)
    act_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    for page_num in range(1, act_pages + 1):
        start = (page_num - 1) * PAGE_SIZE
        chunk = archive[start:start + PAGE_SIZE]
        path  = page_filename(page_num)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({
                'updated':        updated_ts,
                'page':           page_num,
                'total_pages':    act_pages,
                'total_articles': total,
                'page_size':      PAGE_SIZE,
                'articles':       chunk,
            }, f, ensure_ascii=False, indent=2)
        logging.info(f"Wrote {path}  ({len(chunk)} articles, page {page_num}/{act_pages})")

# ============================================================================
# MAIN
# ============================================================================

def main():
    logging.info('=' * 60)
    logging.info('Small Business IT Feed Scraper')
    logging.info('=' * 60)

    updated_ts   = datetime.now(timezone.utc).isoformat()
    new_articles = scrape_all()
    logging.info(f'Total unique new articles: {len(new_articles)}')

    existing = load_archive()
    archive  = merge_into_archive(new_articles, existing)
    logging.info(f'Archive size after merge: {len(archive)}')

    save_archive(archive, updated_ts)
    write_page_files(archive, updated_ts)
    logging.info('Done.')


if __name__ == '__main__':
    main()
