#!/usr/bin/env python3
"""
EPD Punjab Air Quality Reports PDF Scraper
Downloads daily AQI reports from https://epd.punjab.gov.pk/aqi

Strategy: Process ALL dates every run, but skip only already downloaded PDFs
"""

import asyncio
import csv
import logging
import re
import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urljoin, unquote

import aiohttp
import aiosmtplib
from email.message import EmailMessage
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from pypdf import PdfReader

# ==================== CONFIGURATION ====================
class Config:
    # Date range
    END_DATE = date.today()
    START_DATE = END_DATE - timedelta(days=21)  # Last 21 days
    
    # Directories
    DOWNLOAD_FOLDER = Path("downloaded_pdfs/pdfs")
    ERROR_FOLDER = Path("downloaded_pdfs/errors")
    
    # Database and logging
    DB_FILE = "downloads2025.db"
    CSV_FILE = "downloads2025.csv"
    LOG_FILE = "scraper.log"
    SUMMARY_FILE = "scraper_summary.txt"
    
    # Scraping settings
    BASE_URL = "https://epd.punjab.gov.pk"
    DATE_FORMAT = "%m/%d/%Y"
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 60
    PAGE_TIMEOUT = 60000  # milliseconds
    DELAY_BETWEEN_REQUESTS = 1.0  # seconds
    MAX_CONCURRENT = 3
    
    # Browser settings
    HEADLESS = True  # Set to True for production
    BROWSER_ARGS = [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled'
    ]
    
    # Email notifications (set EMAIL_ENABLED=True to activate)
    EMAIL_ENABLED = False
    EMAIL_SENDER = "your_email@example.com"
    EMAIL_RECEIVER = "your_email@example.com"
    SMTP_SERVER = "smtp.example.com"
    SMTP_PORT = 587
    SMTP_USERNAME = "your_username"
    SMTP_PASSWORD = "your_password"
    
    # User agents for rotation
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]

# ==================== SETUP ====================
def setup_logging():
    """Configure logging with file and console handlers."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def setup_directories():
    """Create necessary directories if they don't exist."""
    Config.DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
    Config.ERROR_FOLDER.mkdir(parents=True, exist_ok=True)
    logger.info(f"Directories ready: {Config.DOWNLOAD_FOLDER}, {Config.ERROR_FOLDER}")

# ==================== DATABASE ====================
class Database:
    """Handle all database operations."""
    
    @staticmethod
    def init():
        """Initialize SQLite database with downloads table."""
        conn = sqlite3.connect(Config.DB_FILE, timeout=10)
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
        logger.info(f"Database initialized: {Config.DB_FILE}")
    
    @staticmethod
    def log_download(date_str: str, filename: str, url: str, status: str, 
                     file_size: int = 0, timestamp: str = None):
        """Log a download attempt to database and CSV."""
        if timestamp is None:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            conn = sqlite3.connect(Config.DB_FILE, timeout=10)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO downloads VALUES (?, ?, ?, ?, ?, ?)",
                (date_str, filename, url, status, file_size, timestamp)
            )
            conn.commit()
            conn.close()
            
            # Log to CSV for successful downloads
            if status in ['SUCCESS', 'EXISTS'] and filename:
                # Check if already in CSV to avoid duplicates
                csv_path = Path(Config.CSV_FILE)
                if csv_path.exists():
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        existing = set((row[0], row[1]) for row in reader if len(row) >= 2)
                    
                    if (date_str, filename) not in existing:
                        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow([date_str, filename, url, timestamp])
                else:
                    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow([date_str, filename, url, timestamp])
            
            logger.debug(f"DB log: {date_str} | {filename} | {status}")
        except Exception as e:
            logger.error(f"Failed to log to DB: {date_str}/{filename} - {e}")
    
    @staticmethod
    def get_successful_downloads(date_str: str) -> Set[str]:
        """Get filenames of successfully downloaded PDFs for a specific date.
        
        Returns set of filenames that have status SUCCESS or EXISTS.
        """
        try:
            conn = sqlite3.connect(Config.DB_FILE, timeout=10)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT filename FROM downloads 
                WHERE date = ? AND status IN ('SUCCESS', 'EXISTS')
                AND filename != ''
            """, (date_str,))
            filenames = {row[0] for row in cursor.fetchall()}
            conn.close()
            return filenames
        except Exception as e:
            logger.error(f"Failed to get successful downloads for {date_str}: {e}")
            return set()
    
    @staticmethod
    def get_all_successful_downloads() -> Dict[str, Set[str]]:
        """Get all successfully downloaded PDFs grouped by date.
        
        Returns dict mapping date_str -> set of filenames.
        """
        try:
            conn = sqlite3.connect(Config.DB_FILE, timeout=10)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, filename FROM downloads 
                WHERE status IN ('SUCCESS', 'EXISTS')
                AND filename != ''
            """)
            
            downloads_by_date = {}
            for row in cursor.fetchall():
                date_str, filename = row
                if date_str not in downloads_by_date:
                    downloads_by_date[date_str] = set()
                downloads_by_date[date_str].add(filename)
            
            conn.close()
            return downloads_by_date
        except Exception as e:
            logger.error(f"Failed to get all successful downloads: {e}")
            return {}
    
    @staticmethod
    def get_statistics() -> Dict[str, int]:
        """Get download statistics from database."""
        try:
            conn = sqlite3.connect(Config.DB_FILE, timeout=10)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM downloads WHERE status IN ('SUCCESS', 'EXISTS')")
            successful = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM downloads WHERE status = 'NO_PDF'")
            no_pdf = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM downloads 
                WHERE status LIKE 'ERROR%' OR status LIKE 'FAILED%'
            """)
            errors = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT date) FROM downloads")
            dates_processed = cursor.fetchone()[0]
            
            conn.close()
            return {
                'successful': successful, 
                'no_pdf': no_pdf, 
                'errors': errors,
                'dates_processed': dates_processed
            }
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {'successful': 0, 'no_pdf': 0, 'errors': 0, 'dates_processed': 0}

def init_csv():
    """Initialize CSV file with headers if it doesn't exist."""
    if not Path(Config.CSV_FILE).exists():
        with open(Config.CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['date', 'filename', 'url', 'timestamp'])
        logger.info(f"CSV file initialized: {Config.CSV_FILE}")

# ==================== UTILITIES ====================
def sanitize_filename(filename: str) -> str:
    """Clean filename by removing invalid characters."""
    invalid_chars = r'[<>:"/\\|?*=&]'
    filename = re.sub(invalid_chars, '_', filename).strip()
    filename = re.sub(r'\s+', '_', filename)
    # Ensure .pdf extension
    if not filename.lower().endswith('.pdf'):
        filename += '.pdf'
    return filename

def is_valid_pdf(filepath: Path) -> bool:
    """Check if a file is a valid PDF."""
    try:
        PdfReader(filepath)
        return True
    except Exception as e:
        logger.warning(f"Invalid PDF {filepath.name}: {e}")
        return False

def find_pdf_links(html: str) -> List[Dict[str, str]]:
    """Extract PDF links from HTML content."""
    pdf_links = []
    patterns = [
        r'<a[^>]*href="(/system/files[^"]*\.pdf)"[^>]*>',
        r'<a[^>]*href="([^"]*\.pdf)"[^>]*>'
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for match in matches:
            # Extract filename
            if 'file=' in match:
                filename = unquote(match.split('file=')[-1])
            else:
                filename = unquote(match.split('/')[-1])
            
            filename = sanitize_filename(filename)
            full_url = urljoin(Config.BASE_URL, match)
            
            # Avoid duplicates
            if full_url not in [link['url'] for link in pdf_links]:
                pdf_links.append({'url': full_url, 'filename': filename})
    
    logger.debug(f"Found {len(pdf_links)} PDF links")
    return pdf_links

# ==================== EMAIL NOTIFICATIONS ====================
async def send_email_alert(subject: str, body: str):
    """Send email notification for errors or completion."""
    if not Config.EMAIL_ENABLED:
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
            start_tls=True
        )
        logger.info(f"Email alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")

# ==================== DOWNLOAD ====================
async def download_file(session: aiohttp.ClientSession, url: str, 
                       filename: str, date_str: str, 
                       already_downloaded: Set[str]) -> Tuple[bool, str]:
    """Download a PDF file with validation and retry logic.
    
    Args:
        session: aiohttp session
        url: PDF URL
        filename: Filename to save as
        date_str: Date string for logging
        already_downloaded: Set of filenames already successfully downloaded for this date
    
    Returns:
        Tuple of (success: bool, status: str)
        status can be: 'DOWNLOADED', 'SKIPPED', 'FAILED', 'INVALID_PDF'
    """
    sanitized_filename = sanitize_filename(filename)
    filepath = Config.DOWNLOAD_FOLDER / sanitized_filename
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Check if already successfully downloaded
    if sanitized_filename in already_downloaded:
        # Verify file still exists and is valid
        if filepath.exists() and filepath.stat().st_size > 0 and is_valid_pdf(filepath):
            logger.debug(f"Skipping already downloaded: {sanitized_filename}")
            return True, 'SKIPPED'
        else:
            # File is missing or corrupt, remove from skip set and re-download
            logger.warning(f"Previously downloaded file missing/corrupt: {sanitized_filename}")
            already_downloaded.discard(sanitized_filename)
    
    # Check if file exists on disk and is valid (handles external downloads)
    if filepath.exists() and filepath.stat().st_size > 0:
        if is_valid_pdf(filepath):
            logger.info(f"File exists and valid: {sanitized_filename}")
            Database.log_download(date_str, sanitized_filename, url, 'EXISTS', 
                                filepath.stat().st_size, timestamp)
            return True, 'EXISTS'
        else:
            # Remove corrupt file
            filepath.unlink()
            logger.warning(f"Removed corrupt file: {sanitized_filename}")
    
    # Attempt download with retries
    for attempt in range(Config.MAX_RETRIES):
        try:
            headers = {
                'User-Agent': Config.USER_AGENTS[attempt % len(Config.USER_AGENTS)],
                'Accept': 'application/pdf',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': f'{Config.BASE_URL}/aqi'
            }
            
            async with session.get(url, headers=headers, 
                                  timeout=Config.REQUEST_TIMEOUT) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    filepath.write_bytes(content)
                    size = filepath.stat().st_size
                    
                    # Validate PDF
                    if is_valid_pdf(filepath):
                        logger.info(f"✓ Downloaded: {sanitized_filename} ({size} bytes)")
                        Database.log_download(date_str, sanitized_filename, url, 
                                            'SUCCESS', size, timestamp)
                        return True, 'DOWNLOADED'
                    else:
                        filepath.unlink()
                        Database.log_download(date_str, sanitized_filename, url, 
                                            'INVALID_PDF', 0, timestamp)
                        logger.error(f"✗ Invalid PDF: {sanitized_filename}")
                        return False, 'INVALID_PDF'
                
                elif resp.status == 429:
                    wait_time = 20 + (attempt * 5)
                    logger.warning(f"Rate limited. Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                else:
                    raise Exception(f"HTTP {resp.status}")
        
        except Exception as e:
            logger.error(f"Download attempt {attempt+1}/{Config.MAX_RETRIES} failed "
                        f"for {sanitized_filename}: {e}")
            
            if attempt == Config.MAX_RETRIES - 1:
                Database.log_download(date_str, sanitized_filename, url, 
                                    f'FAILED: {str(e)[:100]}', 0, timestamp)
                await send_email_alert(
                    f"Download Failure: {date_str}",
                    f"Failed to download {sanitized_filename} for {date_str}\nError: {e}"
                )
                return False, 'FAILED'
            
            # Exponential backoff
            await asyncio.sleep(2 ** attempt)
    
    return False, 'FAILED'

# ==================== BROWSER AUTOMATION ====================
class BrowserManager:
    """Manage Playwright browser instance."""
    
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
    
    async def __aenter__(self):
        """Initialize browser on context entry."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=Config.HEADLESS,
            args=Config.BROWSER_ARGS
        )
        self.context = await self.browser.new_context(
            user_agent=Config.USER_AGENTS[0],
            viewport={'width': 1920, 'height': 1080}
        )
        # Set timeouts
        self.context.set_default_timeout(Config.PAGE_TIMEOUT)
        self.context.set_default_navigation_timeout(Config.PAGE_TIMEOUT)
        
        logger.info("Browser initialized")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up browser on context exit."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")
    
    async def new_page(self) -> Page:
        """Create a new page in the browser context."""
        return await self.context.new_page()

async def process_date(browser_manager: BrowserManager, date_str: str, 
                      session: aiohttp.ClientSession,
                      already_downloaded: Set[str]) -> Dict[str, int]:
    """Process a single date: search for PDFs and download new ones.
    
    Args:
        browser_manager: Browser manager instance
        date_str: Date in MM/DD/YYYY format
        session: aiohttp session
        already_downloaded: Set of filenames already downloaded for this date
    
    Returns:
        Dict with counts: {'downloaded': int, 'skipped': int, 'failed': int}
    """
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    page = None
    counts = {'downloaded': 0, 'skipped': 0, 'failed': 0}
    
    for attempt in range(Config.MAX_RETRIES):
        try:
            logger.info(f"Processing {date_str} (attempt {attempt+1}/{Config.MAX_RETRIES})")
            
            # Create new page
            page = await browser_manager.new_page()
            
            # Navigate to AQI page
            logger.debug(f"Navigating to {Config.BASE_URL}/aqi")
            await page.goto(f"{Config.BASE_URL}/aqi", wait_until="networkidle")
            
            # Wait for date input field
            await page.wait_for_selector("#edit-field-aqr-date-value", 
                                        state="visible", timeout=30000)
            
            # Fill date
            logger.debug(f"Filling date: {date_str}")
            await page.fill("#edit-field-aqr-date-value", date_str)
            
            # Click submit button
            logger.debug("Clicking submit button")
            await page.click("#edit-submit-air-quality-reports-block-")
            
            # Wait for results to load
            await page.wait_for_load_state("networkidle", timeout=30000)
            
            # Try to find PDF links (with timeout handling)
            try:
                await page.wait_for_selector("a[href*='.pdf']", timeout=10000)
            except PlaywrightTimeoutError:
                logger.warning(f"No PDF links found for {date_str}")
                Database.log_download(date_str, '', '', 'NO_PDF', 0, timestamp)
                await page.close()
                return counts
            
            # Extract HTML and find PDF links
            html = await page.content()
            pdf_links = find_pdf_links(html)
            
            if not pdf_links:
                logger.warning(f"No PDF links extracted for {date_str}")
                Database.log_download(date_str, '', '', 'NO_PDF', 0, timestamp)
                await page.close()
                return counts
            
            logger.info(f"Found {len(pdf_links)} PDF(s) for {date_str}")
            
            # Download PDFs (skip already downloaded ones)
            for link in pdf_links:
                success, status = await download_file(
                    session, link['url'], link['filename'], 
                    date_str, already_downloaded
                )
                
                if status == 'DOWNLOADED':
                    counts['downloaded'] += 1
                    already_downloaded.add(sanitize_filename(link['filename']))
                elif status == 'SKIPPED' or status == 'EXISTS':
                    counts['skipped'] += 1
                else:
                    counts['failed'] += 1
                
                await asyncio.sleep(Config.DELAY_BETWEEN_REQUESTS)
            
            logger.info(f"Completed {date_str}: "
                       f"{counts['downloaded']} new, "
                       f"{counts['skipped']} skipped, "
                       f"{counts['failed']} failed")
            
            await page.close()
            return counts
        
        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout processing {date_str} (attempt {attempt+1}): {e}")
            
            if page:
                # Save error screenshot and HTML
                error_path = Config.ERROR_FOLDER / f"error_{date_str.replace('/', '_')}"
                try:
                    await page.screenshot(path=f"{error_path}.png")
                    html = await page.content()
                    with open(f"{error_path}.html", 'w', encoding='utf-8') as f:
                        f.write(html)
                except:
                    pass
                await page.close()
            
            if attempt == Config.MAX_RETRIES - 1:
                Database.log_download(date_str, '', '', f'ERROR: Timeout', 0, timestamp)
                await send_email_alert(
                    f"Scraper Timeout: {date_str}",
                    f"Timeout processing {date_str} after {Config.MAX_RETRIES} attempts"
                )
                return counts
            
            # Exponential backoff
            await asyncio.sleep(2 ** attempt)
        
        except Exception as e:
            logger.error(f"Error processing {date_str} (attempt {attempt+1}): {e}")
            
            if page:
                await page.close()
            
            if attempt == Config.MAX_RETRIES - 1:
                Database.log_download(date_str, '', '', f'ERROR: {str(e)[:100]}', 0, timestamp)
                await send_email_alert(
                    f"Scraper Error: {date_str}",
                    f"Failed to process {date_str}\nError: {e}"
                )
                return counts
            
            await asyncio.sleep(2 ** attempt)
    
    return counts

# ==================== MAIN SCRAPER ====================
async def scrape_historical(start_date: date = None, end_date: date = None):
    """Main scraper function to process date range.
    
    Strategy: Process ALL dates every run, but skip only PDFs already downloaded.
    """
    if start_date is None:
        start_date = Config.START_DATE
    if end_date is None:
        end_date = Config.END_DATE
    
    logger.info("="*60)
    logger.info(f"Starting scraper: {start_date} to {end_date}")
    logger.info("Strategy: Process ALL dates, skip only downloaded PDFs")
    logger.info("="*60)
    
    # Setup
    setup_directories()
    Database.init()
    init_csv()
    
    # Generate complete date list (process ALL dates)
    all_dates = [start_date + timedelta(days=x) 
                 for x in range((end_date - start_date).days + 1)]
    
    logger.info(f"Will process {len(all_dates)} dates")
    
    # Get already downloaded PDFs by date
    downloads_by_date = Database.get_all_successful_downloads()
    
    # Log what we're skipping
    total_to_skip = sum(len(pdfs) for pdfs in downloads_by_date.values())
    logger.info(f"Will skip {total_to_skip} already downloaded PDFs across {len(downloads_by_date)} dates")
    
    # Track overall statistics
    total_stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}
    
    # Process dates with browser manager
    async with BrowserManager() as browser_manager:
        async with aiohttp.ClientSession() as session:
            # Process each date
            for i, current_date in enumerate(all_dates, 1):
                date_str = current_date.strftime(Config.DATE_FORMAT)
                
                # Get already downloaded PDFs for this date
                already_downloaded = downloads_by_date.get(date_str, set())
                
                if already_downloaded:
                    logger.info(f"[{i}/{len(all_dates)}] Processing {date_str} "
                              f"(will skip {len(already_downloaded)} already downloaded)")
                else:
                    logger.info(f"[{i}/{len(all_dates)}] Processing {date_str} "
                              f"(no previous downloads)")
                
                try:
                    counts = await process_date(
                        browser_manager, date_str, session, already_downloaded
                    )
                    
                    # Update overall stats
                    total_stats['downloaded'] += counts['downloaded']
                    total_stats['skipped'] += counts['skipped']
                    total_stats['failed'] += counts['failed']
                    
                except Exception as e:
                    logger.error(f"Unexpected error for {date_str}: {e}")
                    Database.log_download(date_str, '', '', f'UNEXPECTED: {str(e)[:100]}', 
                                        0, time.strftime('%Y-%m-%d %H:%M:%S'))
                    total_stats['failed'] += 1
                
                # Small delay between dates
                if i < len(all_dates):
                    await asyncio.sleep(Config.DELAY_BETWEEN_REQUESTS)
    
    logger.info("="*60)
    logger.info(f"Scraping complete!")
    logger.info(f"New PDFs downloaded: {total_stats['downloaded']}")
    logger.info(f"PDFs skipped (already downloaded): {total_stats['skipped']}")
    logger.info(f"Failed downloads: {total_stats['failed']}")
    logger.info("="*60)
    
    return total_stats

# ==================== REPORTING ====================
def generate_summary(start_time: float, run_stats: Dict[str, int]) -> str:
    """Generate summary report of scraping session."""
    db_stats = Database.get_statistics()
    
    all_dates = [Config.START_DATE + timedelta(days=x) 
                 for x in range((Config.END_DATE - Config.START_DATE).days + 1)]
    
    elapsed_time = time.time() - start_time
    
    summary = f"""
EPD Punjab AQI Scraper - Summary Report
{'='*60}
Execution Strategy: Process ALL dates, skip downloaded PDFs only

Date Range: {Config.START_DATE} to {Config.END_DATE}
Total Dates in Range: {len(all_dates)}

This Run Statistics:
  ✓ New PDFs Downloaded: {run_stats['downloaded']}
  ↷ PDFs Skipped (already downloaded): {run_stats['skipped']}
  ✗ Failed Downloads: {run_stats['failed']}

Database Totals (All Time):
  - Total Successful Downloads: {db_stats['successful']}
  - Dates with No PDFs: {db_stats['no_pdf']}
  - Total Errors: {db_stats['errors']}
  - Dates Processed: {db_stats['dates_processed']}

Execution Time: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}
Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
{'='*60}

Note: Every date is processed on each run. PDFs marked as SUCCESS or 
EXISTS in the database are automatically skipped to avoid duplicates.
Failed downloads are retried automatically.
"""
    
    return summary.strip()

# ==================== ENTRY POINT ====================
async def main():
    """Main entry point."""
    start_time = time.time()
    
    logger.info("="*60)
    logger.info("EPD Punjab AQI PDF Scraper Started")
    logger.info("="*60)
    
    try:
        run_stats = await scrape_historical()
        summary = generate_summary(start_time, run_stats)
        
        print("\n" + summary + "\n")
        
        # Save summary to file
        with open(Config.SUMMARY_FILE, 'w', encoding='utf-8') as f:
            f.write(summary)
        
        # Send email notification if errors occurred
        if run_stats['failed'] > 0 and Config.EMAIL_ENABLED:
            await send_email_alert("EPD Scraper - Completion with Errors", summary)
        
        logger.info("Scraper finished successfully")
    
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        await send_email_alert("EPD Scraper - Fatal Error", f"Scraper crashed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())