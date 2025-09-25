#!/usr/bin/env python3

import asyncio
import logging
import random
import sqlite3
import time
import csv
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urljoin, unquote
import re
from typing import List, Dict
import aiohttp
from playwright.async_api import async_playwright
from pypdf import PdfReader
import aiosmtplib
from email.message import EmailMessage
import psutil
import os

# ========== CONFIG ==========
END_DATE = date.today()  # Current date: 2025-09-26
START_DATE = END_DATE - timedelta(days=21)  # Last 21 days: 2025-09-05
DOWNLOAD_FOLDER = Path("downloaded_pdfs/pdfs")
ERROR_FOLDER = Path("downloaded_pdfs/errors")
DB_FILE = "downloads2025.db"
CSV_FILE = "downloads2025.csv"
BASE_URL = "https://epd.punjab.gov.pk"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 45
DELAY_RANGE = (0.2, 0.5)
HEADLESS = True
DATE_FORMAT = "%m/%d/%Y"
MAX_CONCURRENT = 5
EMAIL_NOTIFICATIONS = False
EMAIL_SENDER = "your_email@example.com"
EMAIL_RECEIVER = "your_email@example.com"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USERNAME = "your_username"
SMTP_PASSWORD = "your_password"
BANDWIDTH_THRESHOLD = 20
# ============================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
ERROR_FOLDER.mkdir(parents=True, exist_ok=True)

def sanitize_filename(filename: str) -> str:
    invalid_chars = r'[<>:"/\\|?*=&]'
    filename = re.sub(invalid_chars, '_', filename).strip()
    return re.sub(r'\s+', '_', filename)

def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            date TEXT,
            filename TEXT,
            url TEXT,
            status TEXT,
            file_size INTEGER,
            timestamp TEXT,
            PRIMARY KEY (date, filename)
        )
    """)
    conn.commit()
    conn.close()

def init_csv():
    if not Path(CSV_FILE).exists():
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['date', 'filename', 'url', 'timestamp'])

def log_to_db(date_str: str, filename: str, url: str, status: str, file_size: int, timestamp: str):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO downloads VALUES (?, ?, ?, ?, ?, ?)",
            (date_str, filename, url, status, file_size, timestamp)
        )
        conn.commit()
        if status in ['SUCCESS', 'EXISTS'] and filename:
            with open(CSV_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([date_str, filename, url, timestamp])
    except Exception as e:
        logger.error(f"DB/CSV log failed for {date_str}/{filename}: {e}")
    finally:
        conn.close()

def get_processed_dates() -> set:
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT date FROM downloads")
        dates = {row[0] for row in cursor.fetchall()}
        return dates
    except Exception as e:
        logger.error(f"Failed to get processed dates: {e}")
        return set()
    finally:
        conn.close()

async def send_email_alert(subject: str, body: str):
    if not EMAIL_NOTIFICATIONS:
        return
    try:
        email = EmailMessage()
        email['Subject'] = subject
        email['From'] = EMAIL_SENDER
        email['To'] = EMAIL_RECEIVER
        email.set_content(body)
        await aiosmtplib.send(
            email,
            hostname=SMTP_SERVER,
            port=SMTP_PORT,
            username=SMTP_USERNAME,
            password=SMTP_PASSWORD,
            use_tls=True
        )
        logger.info("Email alert sent")
    except Exception as e:
        logger.error(f"Email alert failed: {e}")

async def check_bandwidth() -> float:
    try:
        net_io = psutil.net_io_counters()
        bytes_start = net_io.bytes_recv
        await asyncio.sleep(1)
        net_io = psutil.net_io_counters()
        bytes_end = net_io.bytes_recv
        mbps = ((bytes_end - bytes_start) * 8 / 1_000_000)
        return mbps
    except Exception:
        return float('inf')

async def download_file(session: aiohttp.ClientSession, url: str, filename: str, date_str: str) -> bool:
    sanitized_filename = sanitize_filename(filename)
    filepath = DOWNLOAD_FOLDER / sanitized_filename
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    if filepath.exists() and filepath.stat().st_size > 0:
        try:
            PdfReader(filepath)
            log_to_db(date_str, sanitized_filename, url, 'EXISTS', filepath.stat().st_size, timestamp)
            return True
        except:
            filepath.unlink()
            logger.warning(f"Removed corrupt file: {sanitized_filename}")

    for attempt in range(MAX_RETRIES):
        try:
            bandwidth = await check_bandwidth()
            if bandwidth < BANDWIDTH_THRESHOLD:
                logger.warning(f"Low bandwidth ({bandwidth:.2f} Mbps), throttling...")
                await asyncio.sleep(2)
            headers = {
                'User-Agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
                ]),
                'Accept': 'application/pdf',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': f'{BASE_URL}/aqi'
            }
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    filepath.write_bytes(content)
                    size = filepath.stat().st_size
                    try:
                        PdfReader(filepath)
                        log_to_db(date_str, sanitized_filename, url, 'SUCCESS', size, timestamp)
                        return True
                    except:
                        filepath.unlink()
                        log_to_db(date_str, sanitized_filename, url, 'INVALID_PDF', 0, timestamp)
                        return False
                elif resp.status == 429:
                    logger.warning(f"Rate limit for {sanitized_filename}, retrying...")
                    await asyncio.sleep(20 + random.uniform(0, 5))
                    continue
                else:
                    raise Exception(f"HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Download attempt {attempt+1} failed for {sanitized_filename}: {e}")
            if attempt == MAX_RETRIES - 1:
                log_to_db(date_str, sanitized_filename, url, f'FAILED: {e}', 0, timestamp)
                await send_email_alert(
                    f"Download Failure: {date_str}",
                    f"Failed to download {sanitized_filename} for {date_str}: {e}"
                )
                return False
            await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
    return False

def find_pdf_links(html: str) -> List[Dict[str, str]]:
    pdf_links = []
    patterns = [
        r'<a[^>]*href="(/system/files[^"]*\.pdf)"[^>]*>',
        r'<a[^>]*href="([^"]*\.pdf)"[^>]*>'
    ]
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for match in matches:
            filename = unquote(match.split('file=')[-1] if 'file=' in match else match.split('/')[-1])
            filename = sanitize_filename(filename)
            full_url = urljoin(BASE_URL, match)
            if full_url not in [link['url'] for link in pdf_links]:
                pdf_links.append({'url': full_url, 'filename': filename})
    return pdf_links[:1]

async def process_date(page, date_str: str, session: aiohttp.ClientSession, processed_dates: set) -> int:
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    if date_str in processed_dates:
        logger.info(f"Skipping {date_str} (already processed)")
        return 0

    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"Attempting to fill date {date_str}")
            await page.fill("#edit-field-aqr-date-value", date_str)
            logger.debug("Clicking submit button")
            await page.click("#edit-submit-air-quality-reports-block-")
            logger.debug("Waiting for PDF links")
            await page.wait_for_selector("a[href*='.pdf']", timeout=10000)
            html = await page.content()
            pdf_links = find_pdf_links(html)
            downloaded = 0
            for link in pdf_links:
                if await download_file(session, link['url'], link['filename'], date_str):
                    downloaded += 1
            if not pdf_links:
                log_to_db(date_str, '', '', 'NO_PDF', 0, timestamp)
            processed_dates.add(date_str)
            logger.info(f"Processed {date_str}: {downloaded} PDFs")
            return downloaded
        except Exception as e:
            logger.error(f"Attempt {attempt+1} failed for {date_str}: {e}")
            if attempt == MAX_RETRIES - 1:
                log_to_db(date_str, '', '', f'ERROR: {e}', 0, timestamp)
                await page.screenshot(path=ERROR_FOLDER / f"error_{date_str.replace('/', '_')}.png")
                with open(ERROR_FOLDER / f"debug_{date_str.replace('/', '_')}.html", 'w', encoding='utf-8') as f:
                    f.write(html if 'html' in locals() else 'No content')
                await send_email_alert(
                    f"Scraper Error: {date_str}",
                    f"Failed to process {date_str}: {e}"
                )
                return 0
            await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
    return 0

async def scrape_historical(start_date: date = START_DATE, end_date: date = END_DATE, max_attempts: int = 3):
    init_db()
    init_csv()
    all_dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    async def process_batch(batch: List[date], processed_dates: set):
        total_downloaded = 0
        async with async_playwright() as p:
            logger.debug("Launching browser")
            browser = await p.chromium.launch(headless=HEADLESS, args=['--no-sandbox', '--disable-blink-features=AutomationControlled'])
            context = await browser.new_context(
                user_agent=random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
                ]),
                viewport={'width': 1280, 'height': 720},
                bypass_csp=True
            )
            async with aiohttp.ClientSession() as session:
                for current_date in batch:
                    date_str = current_date.strftime(DATE_FORMAT)
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    page = await context.new_page()
                    try:
                        for attempt in range(MAX_RETRIES):
                            try:
                                logger.debug(f"Navigating to {BASE_URL}/aqi for {date_str}, attempt {attempt+1}")
                                await page.goto(BASE_URL + "/aqi", wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)
                                break
                            except Exception as e:
                                logger.error(f"Navigation failed for {date_str}: {e}")
                                if attempt == MAX_RETRIES - 1:
                                    log_to_db(date_str, '', '', f'NAVIGATION_ERROR: {e}', 0, timestamp)
                                    await page.screenshot(path=ERROR_FOLDER / f"nav_error_{date_str.replace('/', '_')}.png")
                                    await send_email_alert(
                                        f"Navigation Error: {date_str}",
                                        f"Failed to navigate to {BASE_URL}/aqi for {date_str}: {e}"
                                    )
                                    break
                                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
                        else:
                            continue

                        downloaded = await process_date(page, date_str, session, processed_dates)
                        total_downloaded += downloaded
                    except Exception as e:
                        logger.error(f"Unexpected error for {date_str}: {e}")
                        log_to_db(date_str, '', '', f'UNEXPECTED_ERROR: {e}', 0, timestamp)
                    finally:
                        await page.close()
                        await asyncio.sleep(random.uniform(*DELAY_RANGE))
            await browser.close()
        return total_downloaded

    total_downloaded = 0
    processed_dates = get_processed_dates()
    for attempt in range(max_attempts):
        dates = [d for d in all_dates if d.strftime(DATE_FORMAT) not in processed_dates]
        if not dates:
            logger.info("All dates processed")
            break

        logger.info(f"Attempt {attempt + 1}/{max_attempts}: Processing {len(dates)} dates")
        batch_size = MAX_CONCURRENT
        for i in range(0, len(dates), batch_size):
            batch = dates[i:i + batch_size]
            tasks = [process_batch([d], processed_dates) for d in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Batch failed for {batch[idx].strftime(DATE_FORMAT)}: {result}")
                    log_to_db(batch[idx].strftime(DATE_FORMAT), '', '', f'BATCH_ERROR: {result}', 0, time.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    total_downloaded += result
            await asyncio.sleep(0.5)
        processed_dates = get_processed_dates()

    logger.info(f"Scrape complete: {total_downloaded} PDFs downloaded")
    return total_downloaded

async def main():
    start_time = time.time()
    total = await scrape_historical()
    conn = sqlite3.connect(DB_FILE, timeout=10)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE status IN ('SUCCESS', 'EXISTS')")
    successful = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE status = 'NO_PDF'")
    no_pdf = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE status LIKE 'ERROR%' OR status LIKE 'FAILED%' OR status LIKE 'NAVIGATION_ERROR%' OR status LIKE 'UNEXPECTED_ERROR%' OR status LIKE 'BATCH_ERROR%'")
    errors = cursor.fetchone()[0]
    conn.close()
    all_dates = [START_DATE + timedelta(days=x) for x in range((END_DATE - START_DATE).days + 1)]
    processed_dates = get_processed_dates()
    completion_rate = round(len(processed_dates) / len(all_dates) * 100, 2)
    missing_dates = [d.strftime(DATE_FORMAT) for d in all_dates if d.strftime(DATE_FORMAT) not in processed_dates]
    summary = (
        f"Summary: {total} new PDFs downloaded\n"
        f"Successful downloads: {successful}\n"
        f"No PDFs found: {no_pdf}\n"
        f"Errors: {errors}\n"
        f"Completion rate: {completion_rate:.2f}%\n"
        f"Missing dates: {len(missing_dates)}\n"
        f"Missing dates list: {', '.join(missing_dates) if missing_dates else 'None'}\n"
        f"Time taken: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}"
    )
    print(summary)
    with open("scraper_summary.txt", "w") as f:
        f.write(summary)
    if errors > 0 and EMAIL_NOTIFICATIONS:
        await send_email_alert("Scraper Summary", summary)

if __name__ == "__main__":
    asyncio.run(main())