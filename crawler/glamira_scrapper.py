import os
import logging
import sqlite3
import threading
import re
import json
import time
from typing import Optional, Tuple
from bs4 import BeautifulSoup
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))
from logs.config_logs import setup_logging

setup_logging()

class GlamiraScrapper:
    def __init__(self, domains_file: str, products_file: str, output_dir: str):
        try:
            self.domains_file = domains_file
            self.products_file = products_file
            self.output_dir = output_dir
            self.db_path = "checkpoint.db"

            os.makedirs(self.output_dir, exist_ok=True)
            self.init_database()
            logging.info("Loading CSV data...")
            self.domains_df = pd.read_csv(self.domains_file)
            self.products_df = pd.read_csv(self.products_file)
            logging.info("Loading proxy configurations...")
            self.proxy_configs = self.load_proxy_configs()
            self.thread_local = threading.local()

            logging.info(f"Loaded {len(self.domains_df)} domains and {len(self.products_df)} products")
            logging.info(f"Loaded {len(self.proxy_configs)} proxy configurations")

        except Exception as e:
            logging.error(f"Error initializing scraper: {e}")
            raise

    def load_proxy_configs(self):
        proxy_configs = []
        for i in range(1, 4):
            ip = os.getenv(f'ip{i}')
            port = os.getenv(f'port{i}')
            user = os.getenv(f'user{i}')
            passwd = os.getenv(f'passwd{i}')

            if ip and port:
                config = {
                    'ip': ip,
                    'port': int(port),
                    'user': user,
                    'passwd': passwd,
                    'proxy_id': i
                }
                proxy_configs.append(config)
                logging.info(f"Loaded proxy {i}: {ip}:{port} (user: {user})")
            else:
                logging.warning(f"Proxy {i} configuration incomplete or missing")

        if not proxy_configs:
            logging.warning("No proxy configurations found. Running without proxies.")
        print("Proxy configs loaded:", proxy_configs)
        return proxy_configs

    def get_worker_proxy(self, worker_id: int):
        if not self.proxy_configs:
            return None

        # Assign proxy based on worker ID (cyclically if needed)
        proxy_index = (worker_id - 1) % len(self.proxy_configs)
        return self.proxy_configs[proxy_index]

    def create_proxy_session(self, proxy_config):
        """Create a requests session with SOCKS5 proxy configuration"""
        session = requests.Session()

        if proxy_config:
            proxy_url = f"socks5://{proxy_config['user']}:{proxy_config['passwd']}@{proxy_config['ip']}:{proxy_config['port']}"
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            session.proxies.update(proxies)
            logging.debug(f"Using proxy {proxy_config['proxy_id']}: {proxy_config['ip']}:{proxy_config['port']}")

        return session

    def init_database(self):
        """Initialize SQLite database for checkpoints"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                product_id TEXT NOT NULL,
                status TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(domain, product_id)
            )
        ''')
        conn.commit()
        conn.close()

    def clear_checkpoint_database(self):
        """Clear all checkpoints from the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('DELETE FROM checkpoints')
            conn.commit()
            conn.close()
            logging.info("Checkpoint database cleared successfully")
        except Exception as e:
            logging.error(f"Error clearing checkpoint database: {e}")

    def get_checkpoint_stats(self):
        """Get statistics about the checkpoint database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get total counts by status
            cursor.execute('SELECT status, COUNT(*) FROM checkpoints GROUP BY status')
            stats = dict(cursor.fetchall())

            # Get total count
            cursor.execute('SELECT COUNT(*) FROM checkpoints')
            total = cursor.fetchone()[0]

            conn.close()
            return stats, total
        except Exception as e:
            logging.error(f"Error getting checkpoint stats: {e}")
            return {}, 0

    def save_checkpoint(self, domain: str, product_id: str, status: str):
        """Save checkpoint to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO checkpoints (domain, product_id, status)
            VALUES (?, ?, ?)
        ''', (domain, product_id, status))
        conn.commit()
        conn.close()

    def is_processed(self, domain: str, product_id: str) -> bool:
        """Check if combination has been successfully processed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT status FROM checkpoints 
            WHERE domain = ? AND product_id = ? AND status = 'success'
        ''', (domain, product_id))
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def extract_react_data(self, html_content: str) -> Optional[dict]:
        """Extract react_data from HTML content and filter for specific fields"""

        # Define the specific fields we want to extract
        desired_fields = [
            "product_id", "name", "sku", "attribute_set_id", "attribute_set",
            "type_id", "price", "min_price", "max_price", "min_price_format",
            "max_price_format", "gold_weight", "none_metal_weight", "fixed_silver_weight",
            "material_design", "qty", "collection", "collection_id", "product_type",
            "product_type_value", "category", "category_name", "store_code",
            "platinum_palladium_info_in_alloy", "bracelet_without_chain",
            "show_popup_quantity_eternity", "visible_contents", "gender"
        ]

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Look for script tags containing react_data or product data
            scripts = soup.find_all('script', type='text/javascript')

            for script in scripts:
                script_content = script.string if script.string else ""
                if not script_content:
                    continue

                # Multiple patterns to look for based on the screenshot
                patterns = [
                    # Standard react_data pattern
                    r'var\s+react_data\s*=\s*({.*?});',
                    r'window\.react_data\s*=\s*({.*?});',

                    # Product data patterns from the screenshot
                    r'"product_id":\s*"?\d+"?.*?({[^}]*"product_id"[^}]*})',
                    r'({[^}]*"product_id"[^}]*"name"[^}]*})',

                    # Look for large JSON objects that might contain product data
                    r'var\s+\w+\s*=\s*({.*?"product_id".*?});',
                    r'window\.\w+\s*=\s*({.*?"product_id".*?});',

                    # Generic large JSON objects
                    r'=\s*({.*?"attribute_set_id".*?});',
                    r'=\s*({.*?"price".*?"sku".*?});'
                ]

                for pattern in patterns:
                    matches = re.finditer(pattern, script_content, re.DOTALL)
                    for match in matches:
                        try:
                            json_str = match.group(1)
                            data = json.loads(json_str)

                            # Validate that this looks like product data
                            if isinstance(data, dict):
                                # Check for common product fields
                                product_indicators = ['product_id', 'sku', 'name', 'price', 'attribute_set_id']
                                if any(indicator in str(data).lower() for indicator in product_indicators):
                                    logging.info("Found product data in script")

                                    # Filter data to only include desired fields
                                    filtered_data = {}
                                    for field in desired_fields:
                                        if field in data:
                                            filtered_data[field] = data[field]

                                    # Add metadata for tracking
                                    filtered_data['_metadata'] = {
                                        'extraction_timestamp': time.time(),
                                        'fields_extracted': len(filtered_data) - 1,  # -1 for metadata
                                        'total_fields_available': len(data)
                                    }

                                    logging.info(
                                        f"Filtered data: {len(filtered_data) - 1}/{len(desired_fields)} desired fields found")
                                    return filtered_data
                        except json.JSONDecodeError:
                            continue

            # If no direct patterns found, look for any large JSON structures
            script_text = ' '.join(script.string or '' for script in scripts)

            # Try to find any JSON object that contains product-related keywords
            json_pattern = r'{[^{}]*(?:{[^{}]*}[^{}]*)*}'
            potential_jsons = re.findall(json_pattern, script_text)

            for json_candidate in potential_jsons:
                if len(json_candidate) > 100:  # Only consider substantial JSON objects
                    try:
                        data = json.loads(json_candidate)
                        if isinstance(data, dict):
                            # Check if it contains product-related data
                            data_str = json.dumps(data).lower()
                            if any(keyword in data_str for keyword in ['product', 'sku', 'price', 'name']):
                                logging.info("Found potential product data")

                                # Filter data to only include desired fields
                                filtered_data = {}
                                for field in desired_fields:
                                    if field in data:
                                        filtered_data[field] = data[field]

                                if filtered_data:  # Only return if we found some desired fields
                                    # Add metadata for tracking
                                    filtered_data['_metadata'] = {
                                        'extraction_timestamp': time.time(),
                                        'fields_extracted': len(filtered_data) - 1,
                                        'total_fields_available': len(data)
                                    }

                                    logging.info(
                                        f"Filtered data: {len(filtered_data) - 1}/{len(desired_fields)} desired fields found")
                                    return filtered_data
                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            logging.error(f"Error extracting react_data: {e}")
        return None

    def fetch_product_data(self, domain: str, product_id: str) -> Tuple[bool, Optional[dict]]:
        """Fetch product data with retries for 403 errors and SOCKS5 proxy support"""
        # Get current worker ID from thread-local storage
        worker_id = getattr(self.thread_local, 'worker_id', 1)
        proxy_config = self.get_worker_proxy(worker_id)

        url = f"https://{domain}/catalog/product/view/id/{product_id}"

        # Rotate user agents - Updated with modern, less detectable agents
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]

        for attempt in range(5):  # Retry up to 5 times
            try:
                # Create session with proxy configuration
                session = self.create_proxy_session(proxy_config)

                # Rotate user agent
                ua = user_agents[attempt % len(user_agents)]

                session.headers.update({
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Cache-Control': 'max-age=0'
                })

                # Add referrer for non-first attempts
                if attempt > 0:
                    session.headers['Referer'] = f"https://{domain}/"

                proxy_info = f" (proxy {proxy_config['proxy_id']}: {proxy_config['ip']}:{proxy_config['port']})" if proxy_config else " (no proxy)"
                logging.info(
                    f"Worker {worker_id}: Fetching {url} (attempt {attempt + 1}/5) with UA: {ua[:50]}...{proxy_info}")

                # Add random delay between attempts
                if attempt > 0:
                    delay = (2 ** attempt) + (attempt * 2)  # Progressive delay
                    logging.info(f"Worker {worker_id}: Waiting {delay} seconds before retry...")
                    time.sleep(delay)

                # Use a reasonable timeout
                response = session.get(url, timeout=20, allow_redirects=True)

                logging.info(f"Worker {worker_id}: Response: {response.status_code} for {url}")

                if response.status_code == 200:
                    logging.info(f"Worker {worker_id}: Successfully fetched {url}")

                    # Wait 3 seconds for page to load
                    time.sleep(3)

                    react_data = self.extract_react_data(response.text)
                    if react_data:
                        logging.info(f"Worker {worker_id}: Found react_data for {url}")
                        return True, react_data
                    else:
                        logging.warning(f"Worker {worker_id}: No react_data found for {url}")
                        # Still save the page content for debugging if needed
                        return True, {
                            "url": url,
                            "domain": domain,
                            "product_id": product_id,
                            "status": "no_react_data",
                            "page_title": self.extract_page_title(response.text),
                            "page_size": len(response.text),
                            "timestamp": time.time()
                        }

                elif response.status_code == 403:
                    logging.warning(f"Worker {worker_id}: Got 403 for {url}, attempt {attempt + 1}/5")
                    # Don't give up immediately on 403, continue with next attempt
                    continue

                elif response.status_code == 404:
                    logging.warning(f"Worker {worker_id}: Product not found (404) for {url}")
                    return True, {
                        "url": url,
                        "domain": domain,
                        "product_id": product_id,
                        "status": "not_found",
                        "timestamp": time.time()
                    }

                elif response.status_code in [429, 503]:  # Rate limited or service unavailable
                    logging.warning(f"Worker {worker_id}: Rate limited ({response.status_code}) for {url}")
                    if attempt < 4:
                        delay = (2 ** attempt) * 5  # Longer delay for rate limiting
                        logging.info(f"Worker {worker_id}: Rate limited, waiting {delay} seconds...")
                        time.sleep(delay)
                    continue

                else:
                    logging.error(f"Worker {worker_id}: HTTP {response.status_code} for {url}")
                    if attempt < 4:
                        time.sleep(3)
                    continue

            except requests.exceptions.Timeout:
                logging.warning(f"Worker {worker_id}: Timeout for {url}, attempt {attempt + 1}/5")
                if attempt < 4:
                    time.sleep(5)
                continue

            except requests.exceptions.RequestException as e:
                logging.error(f"Worker {worker_id}: Request error for {url}: {e}")
                if attempt < 4:
                    time.sleep(3)
                continue

        logging.error(f"Worker {worker_id}: Failed to fetch {url} after 5 attempts")
        return False, {
            "url": url,
            "domain": domain,
            "product_id": product_id,
            "status": "failed_all_attempts",
            "timestamp": time.time()
        }

    def extract_page_title(self, html_content: str) -> str:
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.find('title')
            return title.text.strip() if title else "No title"
        except:
            return "Error extracting title"

    def save_product_data(self, domain: str, product_id: str, data: dict):
        filename = f"{domain}_{product_id}.json"
        filepath = os.path.join(self.output_dir, filename)

        try:
            os.makedirs(self.output_dir, exist_ok=True)

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Verify file was created
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                logging.info(f"✓ Saved data to {filepath} ({file_size} bytes)")
            else:
                logging.error(f"File was not created: {filepath}")

        except Exception as e:
            logging.error(f"Error saving {filepath}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def process_product_with_worker_id(self, domain: str, product_id: str, worker_id: int) -> bool:
        self.thread_local.worker_id = worker_id

        try:
            # Check if already processed
            if self.is_processed(domain, product_id):
                logging.info(f"Worker {worker_id}: Skipping {domain}/{product_id} - already processed")
                return True

            logging.info(f"Worker {worker_id}: Processing {domain}/{product_id}")

            # Fetch data
            success, data = self.fetch_product_data(domain, product_id)

            logging.info(
                f"Worker {worker_id}: Fetch result for {domain}/{product_id} - Success: {success}, Data: {type(data)}")

            if success and data:
                logging.info(f"Worker {worker_id}: Saving data for {domain}/{product_id}")

                # Save data
                self.save_product_data(domain, product_id, data)

                # Save checkpoint
                self.save_checkpoint(domain, product_id, 'success')
                logging.info(f"Worker {worker_id}: Successfully processed {domain}/{product_id}")
                return True
            else:
                # Save failed checkpoint
                self.save_checkpoint(domain, product_id, 'failed')
                logging.error(f"Worker {worker_id}: Failed to process {domain}/{product_id}")
                return False

        except Exception as e:
            logging.error(f"Worker {worker_id}: Error processing {domain}/{product_id}: {e}")
            import traceback
            traceback.print_exc()
            self.save_checkpoint(domain, product_id, 'error')
            return False

    def process_product(self, domain: str, product_id: str) -> bool:
        try:
            # Check if already processed
            if self.is_processed(domain, product_id):
                logging.info(f"Skipping {domain}/{product_id} - already processed")
                return True

            # Fetch data
            success, data = self.fetch_product_data(domain, product_id)

            if success and data:
                # Save data
                self.save_product_data(domain, product_id, data)

                # Save checkpoint
                self.save_checkpoint(domain, product_id, 'success')
                logging.info(f"Successfully processed {domain}/{product_id}")
                return True
            else:
                # Save failed checkpoint
                self.save_checkpoint(domain, product_id, 'failed')
                logging.error(f"Failed to process {domain}/{product_id}")
                return False

        except Exception as e:
            logging.error(f"Error processing {domain}/{product_id}: {e}")
            self.save_checkpoint(domain, product_id, 'error')
            return False

    def run_scraping(self, max_workers: int = 3):
        tasks = []

        # Create all domain-product combinations
        for _, domain_row in self.domains_df.iterrows():
            domain = domain_row['domain']
            for _, product_row in self.products_df.iterrows():
                product_id = str(product_row['product_id'])
                tasks.append((domain, product_id))

        logging.info(f"Starting scraping with {max_workers} workers for {len(tasks)} tasks")
        logging.info(f"Proxy configurations available: {len(self.proxy_configs)}")

        completed = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {}
            worker_id = 1

            for domain, product_id in tasks:
                future = executor.submit(self.process_product_with_worker_id, domain, product_id, worker_id)
                future_to_task[future] = (domain, product_id, worker_id)

                worker_id = (worker_id % max_workers) + 1
            for future in as_completed(future_to_task):
                domain, product_id, assigned_worker_id = future_to_task[future]
                try:
                    success = future.result()
                    if success:
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    logging.error(f"Worker {assigned_worker_id}: Task {domain}/{product_id} generated an exception: {e}")
                    failed += 1

                if (completed + failed) % 100 == 0:
                    logging.info(
                        f"Progress: {completed} completed, {failed} failed, {len(tasks) - completed - failed} remaining")

        logging.info(f"Scraping completed: {completed} successful, {failed} failed")

        # Show final statistics
        stats, total = self.get_checkpoint_stats()
        logging.info(f"Final checkpoint stats: {stats}")

        # Clear checkpoint database if scraping is fully completed
        if total > 0:
            logging.info("Clearing checkpoint database...")
            self.clear_checkpoint_database()
            logging.info("Checkpoint database cleared. Ready for next run.")

        return completed, failed
