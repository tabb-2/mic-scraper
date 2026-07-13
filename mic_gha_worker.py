"""
mic_gha_worker.py — GitHub Actions worker для парсинга MIC.

Читает checkpoint из YC S3, обрабатывает батч категорий,
пишет результаты и checkpoint обратно в S3.

Переменные окружения (GitHub Secrets):
  YC_ACCESS_KEY_ID      — S3 access key
  YC_SECRET_ACCESS_KEY  — S3 secret key
  YC_BUCKET             — имя бакета (default: tebiz-data)
  S3_PREFIX             — префикс в бакете (default: parsed/mic)
  BATCH_SIZE            — категорий за один run (default: 999)
  MAX_RUNTIME_MIN       — лимит минут на run (default: 300)
  PAGES_PER_CAT         — страниц на категорию (default: 3, 0 = все)
"""

import gzip
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    sys.exit("pip install boto3")

try:
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("pip install beautifulsoup4")

try:
    from tenacity import (
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential,
        wait_random,
    )
except ImportError:
    sys.exit("pip install tenacity")

# ── Structured logging ────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
_log = logging.getLogger(__name__)


def log(event: str, **kw):
    _log.info(json.dumps({"ts": datetime.utcnow().isoformat(), "event": event, **kw}))


# ── Конфиг ────────────────────────────────────────────────────────────────────
YC_ENDPOINT   = "https://storage.yandexcloud.net"
BUCKET        = os.environ.get("YC_BUCKET", "tebiz-data")
S3_PREFIX     = os.environ.get("S3_PREFIX", "parsed/mic").rstrip("/")
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", "999"))
MAX_RUNTIME_S = int(os.environ.get("MAX_RUNTIME_MIN", "300")) * 60
PAGES_PER_CAT = int(os.environ.get("PAGES_PER_CAT", "3"))  # 0 = все страницы

DELAY_MIN   = 2.5
DELAY_MAX   = 4.5
TIMEOUT     = 15
MAX_RETRIES = 3

BASE = "https://www.made-in-china.com"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# ── S3 ─────────────────────────────────────────────────────────────────────────
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=YC_ENDPOINT,
        aws_access_key_id=os.environ["YC_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["YC_SECRET_ACCESS_KEY"],
        region_name="ru-central1",
    )


def s3_get(key: str) -> Optional[bytes]:
    try:
        r = s3_client().get_object(Bucket=BUCKET, Key=f"{S3_PREFIX}/{key}")
        return r["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def s3_put(key: str, data: bytes):
    s3_client().put_object(Bucket=BUCKET, Key=f"{S3_PREFIX}/{key}", Body=data)


def s3_put_json(key: str, obj):
    s3_put(key, json.dumps(obj, ensure_ascii=False, indent=2).encode())


def s3_get_json(key: str) -> Optional[dict]:
    raw = s3_get(key)
    return json.loads(raw) if raw else None


# ── HTTP (tenacity retry + jitter) ────────────────────────────────────────────
def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, HTTPError):
        # не ретраить клиентские ошибки кроме 429/408
        return exc.code in (408, 429, 500, 502, 503, 504)
    return isinstance(exc, (URLError, OSError, TimeoutError))


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30) + wait_random(0, 2),
    retry=retry_if_exception_type((HTTPError, URLError, OSError, TimeoutError)),
    reraise=True,
)
def _fetch_raw(url: str) -> str:
    t0 = time.time()
    req = Request(url, headers={
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
        "Referer":         BASE + "/",
    })
    with urlopen(req, timeout=TIMEOUT) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        elapsed = round((time.time() - t0) * 1000)
        log("fetch", url=url, status=r.status, elapsed_ms=elapsed)
        return raw.decode("utf-8", errors="replace")


def _get(url: str) -> Optional[str]:
    try:
        return _fetch_raw(url)
    except Exception as e:
        log("fetch_failed", url=url, error=str(e))
        return None


def _sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ── Локальный checkpoint (артефакт GHA) ───────────────────────────────────────
ARTIFACT_PATH = os.environ.get("CHECKPOINT_ARTIFACT", "checkpoint_artifact.json")


def load_artifact_checkpoint() -> Optional[dict]:
    if os.path.exists(ARTIFACT_PATH):
        try:
            with open(ARTIFACT_PATH) as f:
                cp = json.load(f)
            log("artifact_checkpoint_loaded", path=ARTIFACT_PATH, done=len(cp.get("done_cats", [])))
            return cp
        except Exception as e:
            log("artifact_checkpoint_error", error=str(e))
    return None


def save_artifact_checkpoint(cp: dict):
    try:
        with open(ARTIFACT_PATH, "w") as f:
            json.dump(cp, f)
    except Exception as e:
        log("artifact_checkpoint_save_error", error=str(e))


# ── Пагинация ─────────────────────────────────────────────────────────────────
def _page_url(base_url: str, page: int) -> str:
    if page == 1:
        return base_url
    if base_url.endswith(".html"):
        return base_url[:-5] + f"-p{page}.html"
    return base_url + f"?page={page}"


def _detect_last_page(soup) -> int:
    for el in soup.find_all(class_=re.compile(r"total-page|page-total|total_page", re.I)):
        m = re.search(r"(\d+)", el.get_text())
        if m:
            return int(m.group(1))
    pages = []
    for a in soup.find_all("a", href=re.compile(r"-p(\d+)\.html")):
        m = re.search(r"-p(\d+)\.html", a["href"])
        if m:
            pages.append(int(m.group(1)))
    return max(pages) if pages else 1


# ── Парсинг карточки ───────────────────────────────────────────────────────────
def _parse_card(card, cat_id: str) -> Optional[dict]:
    url = sku = ""
    for a in card.find_all("a", href=True):
        m = re.search(r"/product/([A-Za-z0-9]+)", a["href"])
        if m:
            sku = m.group(1)
            href = a["href"]
            url = href if href.startswith("http") else "https:" + href
            break
    if not sku:
        sku = "unk_" + str(abs(hash(card.get_text())))[:8]

    name = ""
    for a in card.find_all("a", title=True):
        t = a["title"].strip()
        if len(t) > 5:
            name = t[:200]
            break
    if not name:
        for cls in ["product-name", "proName", "pro-name"]:
            el = card.find(class_=re.compile(cls, re.I))
            if el:
                name = el.get_text(strip=True)[:200]
                break

    price_min = price_max = None
    price_unit = "piece"
    for cls in ["price", "proPrice", "pro-price"]:
        price_el = card.find(class_=re.compile(cls, re.I))
        if price_el:
            vals = []
            for s in price_el.find_all(["span", "em", "b"]):
                try:
                    v = float(re.sub(r"[^\d.]", "", s.get_text(strip=True)))
                    if 0.0001 < v < 1_000_000:
                        vals.append(v)
                except ValueError:
                    pass
            if vals:
                price_min, price_max = min(vals), max(vals)
            text = price_el.get_text(" ", strip=True)
            if re.search(r"\bset\b", text, re.I):
                price_unit = "set"
            elif re.search(r"\bkg\b", text, re.I):
                price_unit = "kg"
            elif re.search(r"\bpiece|pcs\b", text, re.I):
                price_unit = "piece"
            break

    moq = None
    text = card.get_text(" ", strip=True)
    m = re.search(r"MOQ[:\s]*([\d,]+)", text, re.I)
    if m:
        try:
            moq = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    supplier_id = supplier_name = ""
    for a in card.find_all("a", href=True):
        m = re.search(r"//([a-z0-9-]+)\.en\.made-in-china\.com", a["href"])
        if m:
            supplier_id = m.group(1)
            t = a.get_text(strip=True)
            if t:
                supplier_name = t[:200]
            break
    if not supplier_name:
        for cls in ["company-name", "companyName", "supplier-name", "factory-name"]:
            el = card.find(class_=re.compile(cls, re.I))
            if el:
                supplier_name = el.get_text(strip=True)[:200]
                break
    if supplier_name:
        supplier_name = re.split(r"(Diamond|Audited|Gold|Verified|Member)\b", supplier_name)[0].strip()
        key = supplier_name[:15]
        second = supplier_name.find(key, 10)
        if second > 0:
            supplier_name = supplier_name[:second]
        else:
            half = len(supplier_name) // 2
            if half > 5 and supplier_name[:half] == supplier_name[half:]:
                supplier_name = supplier_name[:half]
        supplier_name = supplier_name.strip("., ")[:100]

    transaction_level = None
    for cls in ["transaction-level", "transactionLevel", "trade-level"]:
        el = card.find(class_=re.compile(cls, re.I))
        if el:
            transaction_level = el.get_text(strip=True)
            break
    if not transaction_level:
        m = re.search(r"(\d+)\s*(?:transaction|order)s?", text, re.I)
        if m:
            transaction_level = m.group(0)

    response_rate = None
    m = re.search(r"(\d+)%\s*response", text, re.I)
    if m:
        response_rate = int(m.group(1))

    certifications = []
    cert_pattern = re.compile(r"\b(CE|RoHS|ISO\s*\d+|FDA|REACH|ASTM|UL|GS|BV)\b")
    for c in cert_pattern.findall(text):
        if c not in certifications:
            certifications.append(c)

    return {
        "sku_id": sku,
        "name": name,
        "category_id": cat_id,
        "supplier_id": supplier_id,
        "supplier_name": supplier_name,
        "fob_price_usd": price_min,
        "price_min": price_min,
        "price_max": price_max,
        "price_unit": price_unit,
        "moq": moq,
        "transaction_level": transaction_level,
        "response_rate": response_rate,
        "certifications": certifications,
        "url": url,
    }


# ── Парсинг категории ─────────────────────────────────────────────────────────
def fetch_products(cat: dict) -> list:
    slug = cat["category_id"]
    if slug.startswith("top_") or slug.startswith("sub_"):
        slug = cat["name_en"].replace(" ", "-")

    base_url = f"{BASE}/products-search/hot-china-products/{slug}.html"
    fallback_url = cat.get("url", "")

    html = _get(base_url)
    if not html or len(html) < 5000:
        if fallback_url:
            html = _get(fallback_url)
            base_url = fallback_url
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all(class_=re.compile(r"products-item|product-item", re.I))
    if not cards:
        return []

    last_page = _detect_last_page(soup)
    pages_limit = PAGES_PER_CAT if PAGES_PER_CAT > 0 else last_page
    pages_to_fetch = min(pages_limit, last_page)

    products = []
    seen: set = set()

    def _extract(page_soup):
        page_cards = page_soup.find_all(class_=re.compile(r"products-item|product-item", re.I))
        for card in page_cards:
            p = _parse_card(card, cat["category_id"])
            if p and p["sku_id"] not in seen:
                seen.add(p["sku_id"])
                products.append(p)

    _extract(soup)

    for page in range(2, pages_to_fetch + 1):
        purl = _page_url(base_url, page)
        _sleep()
        html2 = _get(purl)
        if not html2 or len(html2) < 3000:
            break
        soup2 = BeautifulSoup(html2, "html.parser")
        before = len(products)
        _extract(soup2)
        if len(products) == before:
            break

    return products


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    log("run_start", batch=BATCH_SIZE, pages_per_cat=PAGES_PER_CAT, max_min=MAX_RUNTIME_S // 60)

    # Checkpoint: S3 (основной) или GHA artifact (резерв при S3 недоступен)
    cp = s3_get_json("checkpoint.json")
    if cp is None:
        cp = load_artifact_checkpoint()
    if cp is None:
        cp = {"done_cats": [], "suppliers": {}}

    categories = s3_get_json("categories.json")
    if not categories:
        sys.exit("categories.json не найден в S3")

    done_set = set(cp["done_cats"])
    todo = [c for c in categories if c.get("is_leaf", True) and c["category_id"] not in done_set]

    total = len(categories)
    log("progress", done=len(done_set), total=total, remaining=len(todo))

    if not todo:
        log("run_complete", message="All categories scraped")
        _build_index(categories)
        return

    processed = 0
    for cat in todo[:BATCH_SIZE]:
        elapsed = time.time() - start_time
        if elapsed > MAX_RUNTIME_S - 120:
            log("time_limit", elapsed_min=round(elapsed / 60, 1))
            break

        cid = cat["category_id"]
        t0 = time.time()
        products = fetch_products(cat)
        n_with_price = sum(1 for p in products if p.get("fob_price_usd"))
        n_with_supplier = sum(1 for p in products if p.get("supplier_name"))

        log("category_done",
            name=cat["name_en"][:50],
            skus=len(products),
            priced=n_with_price,
            with_supplier=n_with_supplier,
            elapsed_ms=round((time.time() - t0) * 1000),
            batch_n=processed + 1)

        s3_put_json(f"products_{cid}.json", products)
        cp["done_cats"].append(cid)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        s3_put_json(f"checkpoint_{today}.json", cp)
        s3_put_json("checkpoint.json", cp)
        save_artifact_checkpoint(cp)  # локальная копия для GHA artifact
        processed += 1
        _sleep()

    elapsed = time.time() - start_time
    remaining = len(todo) - processed
    pct = round(len(cp["done_cats"]) / total * 100, 1)

    log("run_end",
        processed=processed,
        remaining=remaining,
        done_total=len(cp["done_cats"]),
        total=total,
        pct=pct,
        elapsed_min=round(elapsed / 60, 1))

    if remaining == 0:
        _build_index(categories)


def _build_index(categories: list):
    log("build_index_start", categories=len(categories))
    index = {}
    for cat in categories:
        raw = s3_get_json(f"products_{cat['category_id']}.json")
        if not raw:
            continue
        prices = sorted(p["fob_price_usd"] for p in raw if p.get("fob_price_usd"))
        if not prices:
            continue
        n = len(prices)
        n_suppliers = len({p["supplier_id"] for p in raw if p.get("supplier_id")})
        moqs = [p["moq"] for p in raw if p.get("moq")]
        certs = list({c for p in raw for c in p.get("certifications", [])})
        index[cat["category_id"]] = {
            "name": cat["name_en"],
            "n_skus": len(raw),
            "n_suppliers": n_suppliers,
            "n_priced": n,
            "fob_min": round(prices[0], 4),
            "fob_med": round(prices[n // 2], 4),
            "fob_max": round(prices[-1], 4),
            "fob_avg": round(sum(prices) / n, 4),
            "moq_min": min(moqs) if moqs else None,
            "moq_med": sorted(moqs)[len(moqs) // 2] if moqs else None,
            "certifications": certs,
        }
    s3_put_json("mic_index.json", index)
    log("build_index_done", indexed=len(index))


if __name__ == "__main__":
    main()
