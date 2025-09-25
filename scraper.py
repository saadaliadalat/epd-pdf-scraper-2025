#!/usr/bin/env python3

import asyncio
import logging
import random
import sqlite3
import time
import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, unquote
import re
from typing import List, Dict, Set
import aiohttp
from playwright.async_api import async_playwright, Page
from pypdf import PdfReader
import aiosmtplib
from email.message import EmailMessage
import psutil

class Config:
    # Calculate dates dynamically
    END_DATE = date.today()
    START_DATE = END_DATE - timedelta(days=21)
    
    # Paths
    DOWNLOAD_FOLDER = Path("downloads/pdf")
    ERROR_FOLDER = Path("downloads/errors")
    DB_FILE = "downloads.db"
    CSV_FILE = "downloads.csv"
    
    # URLs and API
    BASE_URL = "https://epd.punjab.gov.pk"
    
    # Scraping settings
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 45
    DELAY_RANGE = (0.2, 0.5)
    HEADLESS = True
    DATE_FORMAT = "%m/%d/%Y"
    MAX_CONCURRENT = 5
    BANDWIDTH_THRESHOLD = 20
    
    # Email settings
    EMAIL_NOTIFICATIONS = False
    EMAIL_SENDER = "your_email@example.com"
    EMAIL_RECEIVER = "your_email@example.com"
    SMTP_SERVER = "smtp.example.com"
    SMTP_PORT = 587
    SMTP_USERNAME = "your_username"
    SMTP_PASSWORD = "your_password"

class PDFScraper:
    def __init__(self):
        self._setup_logging()
        self._create_directories()
        self._init_db()
        self._init_csv()
        self.processed_dates = self._get_processed_dates()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _create_directories(self):
        Config.DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
        Config.ERROR_FOLDER.mkdir(parents=True, exist_ok=True)

    def _init_db(self):
        with sqlite3.connect(Config.DB_FILE) as conn:
            conn.execute("""
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

    def _init_csv(self):
        if not Path(Config.CSV_FILE).exists():
            with open(Config.CSV_FILE, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['date', 'filename', 'url', 'timestamp'])

    def _get_processed_dates(self) -> Set[str]:
        try:
            with sqlite3.connect(Config.DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT date FROM downloads")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"Failed to get processed dates: {e}")
            return set()

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        invalid_chars = r'[<>:"/\\|?*=&]'
        filename = re.sub(invalid_chars, '_', filename).strip()
        return re.sub(r'\s+', '_', filename)

    async def check_bandwidth(self) -> float:
        try:
            net_io = psutil.net_io_counters()
            bytes_start = net_io.bytes_recv
            await asyncio.sleep(1)
            net_io = psutil.net_io_counters()
            bytes_end = net_io.bytes_recv
            return ((bytes_end - bytes_start) * 8 / 1_000_000)
        except Exception:
            return float('inf')

    async def send_email_alert(self, subject: str, body: str):
        if not Config.EMAIL_NOTIFICATIONS:
            return
        
        try:
            email = EmailMessage()
            email['Subject'] = subject
            email['From'] = Config.EMAIL_SENDER
            email['To'] = Config.EMAIL_RECEIVER
            email.set_content(body)
            
            await aiosmtplib.send(
                email,
                hostname=Config.SMTP_SERVER,
                port=Config.SMTP_PORT,
                username=Config.SMTP_USERNAME,
                password=Config.SMTP_PASSWORD,
                use_tls=True
            )
            self.logger.info("Email alert sent")
        except Exception as e:
            self.logger.error(f"Email alert failed: {e}")

    def log_to_db(self, date_str: str, filename: str, url: str, 
                  status: str, file_size: int, timestamp: str):
        try:
            with sqlite3.connect(Config.DB_FILE) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO downloads VALUES (?, ?, ?, ?, ?, ?)",
                    (date_str, filename, url, status, file_size, timestamp)
                )
                
                if status in ['SUCCESS', 'EXISTS'] and filename:
                    with open(Config.CSV_FILE, 'a', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([date_str, filename, url, timestamp])
        except Exception as e:
            self.logger.error(f"DB/CSV log failed for {date_str}/{filename}: {e}")

    async def download_file(self, session: aiohttp.ClientSession, 
                          url: str, filename: str, date_str: str) -> bool:
        sanitized_filename = self.sanitize_filename(filename)
        filepath = Config.DOWNLOAD_FOLDER / sanitized_filename
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if filepath.exists() and filepath.stat().st_size > 0:
            try:
                PdfReader(filepath)
                self.log_to_db(date_str, sanitized_filename, url, 'EXISTS', 
                              filepath.stat().st_size, timestamp)
                return True
            except:
                filepath.unlink()
                self.logger.warning(f"Removed corrupt file: {sanitized_filename}")

        for attempt in range(Config.MAX_RETRIES):
            try:
                bandwidth = await self.check_bandwidth()
                if bandwidth < Config.BANDWIDTH_THRESHOLD:
                    self.logger.warning(f"Low bandwidth ({bandwidth:.2f} Mbps)")
                    await asyncio.sleep(2)

                headers = {
                    'User-Agent': random.choice([
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
                    ]),
                    'Accept': 'application/pdf',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': f'{Config.BASE_URL}/aqi'
                }

                async with session.get(url, headers=headers, 
                                     timeout=Config.REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        filepath.write_bytes(content)
                        
                        try:
                            PdfReader(filepath)
                            self.log_to_db(date_str, sanitized_filename, url, 
                                         'SUCCESS', filepath.stat().st_size, 
                                         timestamp)
                            return True
                        except:
                            filepath.unlink()
                            self.log_to_db(date_str, sanitized_filename, url, 
                                         'INVALID_PDF', 0, timestamp)
                            return False
                    
                    elif resp.status == 429:
                        self.logger.warning(f"Rate limit hit, retrying...")
                        await asyncio.sleep(20 + random.uniform(0, 5))
                        continue
                    else:
                        raise Exception(f"HTTP {resp.status}")

            except Exception as e:
                self.logger.error(
                    f"Download attempt {attempt+1} failed for {sanitized_filename}: {e}"
                )
                if attempt == Config.MAX_RETRIES - 1:
                    self.log_to_db(date_str, sanitized_filename, url, 
                                 f'FAILED: {e}', 0, timestamp)
                    await self.send_email_alert(
                        f"Download Failure: {date_str}",
                        f"Failed to download {sanitized_filename}: {e}"
                    )
                    return False
                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
        
        return False

    @staticmethod
    def find_pdf_links(html: str) -> List[Dict[str, str]]:
        pdf_links = []
        patterns = [
            r'<a[^>]*href="(/system/files[^"]*\.pdf)"[^>]*>',
            r'<a[^>]*href="([^"]*\.pdf)"[^>]*>'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                filename = unquote(match.split('file=')[-1] 
                                 if 'file=' in match else match.split('/')[-1])
                filename = PDFScraper.sanitize_filename(filename)
                full_url = urljoin(Config.BASE_URL, match)
                if full_url not in [link['url'] for link in pdf_links]:
                    pdf_links.append({'url': full_url, 'filename': filename})
        
        return pdf_links[:1]  # Return only first PDF link

    async def process_date(self, page: Page, date_str: str, 
                          session: aiohttp.ClientSession) -> int:
        if date_str in self.processed_dates:
            self.logger.info(f"Skipping {date_str} (already processed)")
            return 0

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for attempt in range(Config.MAX_RETRIES):
            try:
                await page.fill("#edit-field-aqr-date-value", date_str)
                await page.click("#edit-submit-air-quality-reports-block-")
                await page.wait_for_selector("a[href*='.pdf']", timeout=10000)
                
                html = await page.content()
                pdf_links = self.find_pdf_links(html)
                downloaded = 0
                
                for link in pdf_links:
                    if await self.download_file(session, link['url'], 
                                             link['filename'], date_str):
                        downloaded += 1
                
                if not pdf_links:
                    self.log_to_db(date_str, '', '', 'NO_PDF', 0, timestamp)
                
                self.processed_dates.add(date_str)
                self.logger.info(f"Processed {date_str}: {downloaded} PDFs")
                return downloaded

            except Exception as e:
                self.logger.error(f"Attempt {attempt+1} failed for {date_str}: {e}")
                if attempt == Config.MAX_RETRIES - 1:
                    self.log_to_db(date_str, '', '', f'ERROR: {e}', 0, timestamp)
                    await page.screenshot(
                        path=Config.ERROR_FOLDER / 
                        f"error_{date_str.replace('/', '_')}.png"
                    )
                    with open(Config.ERROR_FOLDER / 
                             f"debug_{date_str.replace('/', '_')}.html", 
                             'w', encoding='utf-8') as f:
                        f.write(html if 'html' in locals() else 'No content')
                    await self.send_email_alert(
                        f"Scraper Error: {date_str}",
                        f"Failed to process {date_str}: {e}"
                    )
                    return 0
                await asyncio.sleep(2 ** attempt + random.uniform(0, 0.5))
        
        return 0

    async def scrape(self):
        start_time = time.time()
        total_downloaded = 0
        dates_to_process = [
            Config.START_DATE + timedelta(days=x) 
            for x in range((Config.END_DATE - Config.START_DATE).days + 1)
        ]

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=Config.HEADLESS,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720},
                bypass_csp=True
            )

            async with aiohttp.ClientSession() as session:
                for current_date in dates_to_process:
                    date_str = current_date.strftime(Config.DATE_FORMAT)
                    page = await context.new_page()
                    
                    try:
                        await page.goto(
                            f"{Config.BASE_URL}/aqi",
                            wait_until="domcontentloaded",
                            timeout=Config.REQUEST_TIMEOUT * 1000
                        )
                        downloaded = await self.process_date(
                            page, date_str, session
                        )
                        total_downloaded += downloaded
                    
                    except Exception as e:
                        self.logger.error(f"Error processing {date_str}: {e}")
                    
                    finally:
                        await page.close()
                        await asyncio.sleep(
                            random.uniform(*Config.DELAY_RANGE)
                        )

            await browser.close()

        # Generate summary
        self._generate_summary(total_downloaded, start_time)
        return total_downloaded

    def _generate_summary(self, total_downloaded: int, start_time: float):
        with sqlite3.connect(Config.DB_FILE) as conn:
            cursor = conn.cursor()
            successful = cursor.execute(
                "SELECT COUNT(*) FROM downloads WHERE status IN ('SUCCESS', 'EXISTS')"
            ).fetchone()[0]
            no_pdf = cursor.execute(
                "SELECT COUNT(*) FROM downloads WHERE status = 'NO_PDF'"
            ).fetchone()[0]
            errors = cursor.execute(
                """SELECT COUNT(*) FROM downloads 
                   WHERE status LIKE 'ERROR%' 
                   OR status LIKE 'FAILED%'"""
            ).fetchone()[0]

        all_dates = [
            Config.START_DATE + timedelta(days=x) 
            for x in range((Config.END_DATE - Config.START_DATE).days + 1)
        ]
        completion_rate = round(
            len(self.processed_dates) / len(all_dates) * 100, 2
        )
        missing_dates = [
            d.strftime(Config.DATE_FORMAT) 
            for d in all_dates 
            if d.strftime(Config.DATE_FORMAT) not in self.processed_dates
        ]

        summary = (
            f"Summary:\n"
            f"New PDFs downloaded: {total_downloaded}\n"
            f"Successful downloads: {successful}\n"
            f"No PDFs found: {no_pdf}\n"
            f"Errors: {errors}\n"
            f"Completion rate: {completion_rate}%\n"
            f"Missing dates: {len(missing_dates)}\n"
            f"Missing dates list: {', '.join(missing_dates) if missing_dates else 'None'}\n"
            f"Time taken: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}"
        )

        print(summary)
        with open("scraper_summary.txt", "w") as f:
            f.write(summary)

async def main():
    scraper = PDFScraper()
    await scraper.scrape()

if __name__ == "__main__":
    asyncio.run(main())