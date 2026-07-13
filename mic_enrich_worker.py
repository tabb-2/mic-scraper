"""
mic_enrich_worker.py — обогащение MIC товаров через Playwright.

Берёт url из products_{cat_id}.json в S3, заходит на страницу товара,
извлекает MOQ и price_unit, обновляет файлы обратно в S3.

Env:
  YC_ACCESS_KEY_ID, YC_SECRET_ACCESS_KEY
  BATCH_CATS     — категорий за один run (default: 100)
  MAX_RUNTIME_MIN — лимит минут (default: 300)
"""

import json
import os
import re
import sys
import time
import random
from datetime import datetime
from typing import Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    sys.exit("pip install boto3")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit("pip install playwright && playwright install chromium")

# ── Конфиг ────────────────────────────────────────────────────────────────────
BUCKET        = os.environ.get("YC_BUCKET", "tebiz-data")
S3_PREFIX     = os.environ.get("S3_PREFIX", "parsed/mic").rstrip("/")
BATCH_CATS    = int(os.environ.get("BATCH_CATS", "100"))
MAX_RUNTIME_S = int(os.environ.get("MAX_RUNTIME_MIN", "300")) * 60
PAGE_TIMEOUT  = 20_000  # ms
NAV_TIMEOUT   = 30_000  # ms
DELAY_MIN     = 2.0
DELAY_MAX     = 4.0


# ── S3 ────────────────────────────────────────────────────────────────────────
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=os.environ["YC_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["YC_SECRET_ACCESS_KEY"],
        region_name="ru-central1",
    )


def s3_get_json(s3, key):
    try:
        r = s3.get_object(Bucket=BUCKET, Key=f"{S3_PREFIX}/{key}")
        return json.loads(r["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def s3_put_json(s3, key, obj):
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{S3_PREFIX}/{key}",
        Body=json.dumps(obj, ensure_ascii=False, indent=2).encode()
    )


# ── Playwright парсинг страницы товара ───────────────────────────────────────
def parse_product_page(page, url: str) -> dict:
    """Возвращает {moq, price_unit, price_min, price_max} с реальной страницы товара."""
    result = {}
    try:
        page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)
        text = page.inner_text("body")

        # MOQ
        moq_patterns = [
            r"Min\.?\s*Order[:\s]+(\d[\d,]*)\s*(Piece|Set|Bag|Box|Roll|Meter|Kg|Ton|Unit|Pack)s?",
            r"Minimum\s+Order[:\s]+(\d[\d,]*)",
            r"MOQ[:\s]+(\d[\d,]*)",
        ]
        for pat in moq_patterns:
            m = re.search(pat, text, re.I)
            if m:
                try:
                    result["moq"] = int(m.group(1).replace(",", ""))
                except ValueError:
                    pass
                # Единица из MOQ строки
                if len(m.groups()) > 1 and m.group(2):
                    result["price_unit"] = m.group(2).lower()
                break

        # price_unit из строки цены
        if "price_unit" not in result:
            unit_patterns = [
                (r"\$[\d.,]+\s*/\s*(Piece|Set|Bag|Box|Roll|Meter|Metre|Kg|Ton|Unit|Pack)", 1),
                (r"per\s+(Piece|Set|Bag|Box|Roll|Meter|Metre|Kg|Ton|Unit|Pack)", 1),
            ]
            for pat, grp in unit_patterns:
                m = re.search(pat, text, re.I)
                if m:
                    result["price_unit"] = m.group(grp).lower()
                    break

        # Цена с реальной страницы (точнее чем из списка)
        price_m = re.search(r"US\s*\$\s*([\d.]+)\s*[-–]\s*([\d.]+)", text)
        if price_m:
            result["price_min"] = float(price_m.group(1))
            result["price_max"] = float(price_m.group(2))
        else:
            price_m = re.search(r"US\s*\$\s*([\d.]+)", text)
            if price_m:
                result["price_min"] = float(price_m.group(1))

    except PWTimeout:
        pass
    except Exception as e:
        print(f"  parse error {url}: {e}")

    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    print(f"=== MIC Enrich Worker {datetime.utcnow().isoformat()} ===")
    print(f"batch_cats={BATCH_CATS}  max={MAX_RUNTIME_S//60}min")

    s3 = s3_client()

    # Checkpoint обогащения (отдельный от основного)
    enrich_cp = s3_get_json(s3, "enrich_checkpoint.json") or {"done_cats": []}
    done_set = set(enrich_cp["done_cats"])

    categories = s3_get_json(s3, "categories.json")
    if not categories:
        sys.exit("categories.json не найден")

    todo = [c for c in categories if c.get("is_leaf", True) and c["category_id"] not in done_set]
    print(f"Осталось обогатить: {len(todo)}/{len(categories)} категорий")

    if not todo:
        print("Все категории обогащены!")
        return

    total_enriched = 0
    total_skipped = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )
        ctx.set_default_timeout(PAGE_TIMEOUT)
        page = ctx.new_page()

        for cat in todo[:BATCH_CATS]:
            if time.time() - start > MAX_RUNTIME_S - 120:
                print("Лимит времени, сохраняю...")
                break

            cid = cat["category_id"]
            products = s3_get_json(s3, f"products_{cid}.json")
            if not products:
                enrich_cp["done_cats"].append(cid)
                continue

            # Обогащаем только товары без MOQ или с unit=piece (неизвестно)
            to_enrich = [p for p in products if not p.get("moq") or p.get("price_unit") == "piece"]
            if not to_enrich:
                enrich_cp["done_cats"].append(cid)
                total_skipped += 1
                continue

            print(f"[{cat['name_en'][:40]}] {len(to_enrich)}/{len(products)} товаров к обогащению")
            cat_enriched = 0

            for p in to_enrich:
                url = p.get("url")
                if not url:
                    continue
                enriched = parse_product_page(page, url)
                if enriched:
                    if "moq" in enriched and enriched["moq"]:
                        p["moq"] = enriched["moq"]
                    if "price_unit" in enriched:
                        p["price_unit"] = enriched["price_unit"]
                    if "price_min" in enriched:
                        p["price_min"] = enriched["price_min"]
                        p["fob_price_usd"] = enriched["price_min"]
                    if "price_max" in enriched:
                        p["price_max"] = enriched["price_max"]
                    cat_enriched += 1
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            # Сохраняем обогащённые данные
            s3_put_json(s3, f"products_{cid}.json", products)
            enrich_cp["done_cats"].append(cid)
            s3_put_json(s3, "enrich_checkpoint.json", enrich_cp)
            total_enriched += cat_enriched
            print(f"  → обогащено {cat_enriched} товаров")

        browser.close()

    elapsed = time.time() - start
    remaining = len(todo) - len(enrich_cp["done_cats"])
    print(f"\n✓ Готово!")
    print(f"  Обогащено SKU: {total_enriched}")
    print(f"  Пропущено (уже ок): {total_skipped}")
    print(f"  Осталось категорий: {remaining}")
    print(f"  Время: {elapsed/60:.1f} мин")


if __name__ == "__main__":
    main()
