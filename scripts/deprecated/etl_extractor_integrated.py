'''Depriciated file used to extract data using selenim and api But facing blockage errors
Code works but barely get 1-2 featuures in the list before failing multiple time, also takes 30 mins to go through
the entire list'''

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import datetime
import logging
import random
import time
import json
import os
import sys
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from urllib.parse import urljoin

# Load environment variables from .env file
load_dotenv()

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AgriExtractor:
    """
    Enhanced ETL Extractor for Agricultural Market Data.
    
    This class combines:
    1. Real Weather API data (WeatherAPI.com)
    2. Selenium-based web scraping (AGMARKNET portal)
    3. Mock data fallbacks for resilience
    
    Data Flow:
    - Selenium scrapes modal price & arrival quantity from AGMARKNET
    - Weather API enriches with real temperature, humidity, rainfall
    - All data is deduplicated and persisted to SQLite + Parquet
    """

    DISTRICTS = ['Pune', 'Nashik', 'Nagpur', 'Satara', 'Aurangabad', 'Solapur']
    COMMODITIES = ['Onion', 'Wheat', 'Rice', 'Soybean', 'Cotton', 'Tomato']
    AGMARKNET_URL = "https://agmarknet.gov.in/home"

    def __init__(self):
        """Initialize database connection and Selenium driver."""
        self.conn = sqlite3.connect(Config.DB_PATH)
        self.driver = None
        self.wait = None
        self.weather_api_key = os.getenv("WEATHER_API_KEY", "").strip()
        self._init_db()

    def _init_db(self):
        """Create extraction log table for deduplication."""
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_log (
                date_scraped TEXT,
                district TEXT,
                apmc TEXT,
                commodity TEXT,
                PRIMARY KEY (date_scraped, district, apmc, commodity)
            )
        ''')
        self.conn.commit()
        logger.info("Database initialized.")

    def _init_selenium(self):
        """
        Initialize Selenium WebDriver.
        Uses Chrome in headless mode for production.
        """
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            self.driver = webdriver.Chrome(options=options)
            self.wait = WebDriverWait(self.driver, 15)
            logger.info("Selenium driver initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize Selenium: {e}")
            raise

    def _cleanup_selenium(self):
        """Close Selenium driver gracefully."""
        if self.driver:
            self.driver.quit()
            logger.info("Selenium driver closed.")

    def get_weather(self, district):
        """
        Fetch real-time weather data from WeatherAPI.
        
        Args:
            district (str): District name (e.g., "Pune")
        
        Returns:
            dict: {
                'temperature': float (°C),
                'humidity': float (%),
                'rainfall': float (mm)
            }
        
        Fallback: Generates synthetic data if API fails or key is missing.
        
        Error Handling:
        - Missing API key → Use mock data
        - Network timeout → Use mock data
        - Rate limit (429) → Use mock data
        - Invalid response → Use mock data
        """
        if not self.weather_api_key or "YOUR_" in self.weather_api_key:
            logger.debug(
                f"Weather API key not configured. Using synthetic data for {district}."
            )
            return self._get_synthetic_weather()

        try:
            # WeatherAPI.com endpoint
            url = "https://api.weatherapi.com/v1/current.json"
            params = {
                'q': f"{district}, India",
                'key': self.weather_api_key,
                'aqi': 'no'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                current = data.get('current', {})
                
                weather = {
                    'temperature': float(current.get('temp_c', 25)),
                    'humidity': float(current.get('humidity', 50)),
                    'rainfall': float(current.get('precip_mm', 0))
                }
                logger.info(
                    f"Weather fetched for {district}: "
                    f"Temp={weather['temperature']}°C, "
                    f"Humidity={weather['humidity']}%"
                )
                return weather
            
            elif response.status_code == 429:
                logger.warning(
                    f"Weather API rate limit exceeded for {district}. "
                    "Using synthetic data."
                )
                return self._get_synthetic_weather()
            
            else:
                logger.warning(
                    f"Weather API error ({response.status_code}) for {district}. "
                    "Using synthetic data."
                )
                return self._get_synthetic_weather()

        except requests.exceptions.Timeout:
            logger.warning(
                f"Weather API timeout for {district}. Using synthetic data."
            )
            return self._get_synthetic_weather()
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Weather API request failed for {district}: {e}. "
                "Using synthetic data."
            )
            return self._get_synthetic_weather()
        except (KeyError, ValueError) as e:
            logger.warning(
                f"Failed to parse Weather API response for {district}: {e}. "
                "Using synthetic data."
            )
            return self._get_synthetic_weather()

    def _get_synthetic_weather(self):
        """
        Generate realistic synthetic weather data as fallback.
        
        Returns:
            dict: {
                'temperature': float (°C, range 20-40),
                'humidity': float (%, range 30-90),
                'rainfall': float (mm, range 0-15)
            }
        """
        return {
            'temperature': round(random.uniform(20, 40), 1),
            'humidity': round(random.uniform(30, 90), 1),
            'rainfall': round(random.uniform(0, 15), 1)
        }

    def select_from_dropdown(self, element_id, search_text, uncheck_label):
        """
        Helper function to select dropdown items (reusable pattern from Sel_extractor.py).
        
        Args:
            element_id (str): HTML element ID of the dropdown
            search_text (str): Text to search for in dropdown
            uncheck_label (str): Label of the "All" option to uncheck first
        
        Raises:
            Exception: If dropdown selection fails
        """
        try:
            # Open dropdown
            self.driver.find_element(By.ID, element_id).click()
            time.sleep(1)

            # Uncheck "All..." option
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//span[text()='{uncheck_label}']")
            )).click()
            time.sleep(0.5)

            # Type in search bar
            search_input = self.wait.until(EC.visibility_of_element_located(
                (By.XPATH, "//input[@placeholder='Search...']")
            ))
            search_input.clear()
            search_input.send_keys(search_text)
            time.sleep(1)

            # Click matching item
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//span[contains(text(),'{search_text}')]")
            )).click()
            time.sleep(1)

        except Exception as e:
            logger.error(
                f"Error selecting '{search_text}' from dropdown '{element_id}': {e}"
            )
            raise

    def scrape_agmarknet_row(self, district, commodity):
        """
        Scrape a single row from AGMARKNET for a given district + commodity.
        
        This function replicates the extraction logic from Sel_extractor.py,
        iterating through the AGMARKNET portal UI and extracting:
        - modal_price_rs_qtl (column 3)
        - arrival_quantity_qtl (column 6)
        
        Args:
            district (str): District name
            commodity (str): Commodity name
        
        Returns:
            dict: {
                'district': str,
                'commodity': str,
                'modal_price_rs_qtl': str or None,
                'arrival_quantity_qtl': str or None
            }
        """
        try:
            logger.info(f"Scraping {district} - {commodity}...")
            
            # Step 1: Select State (default: Maharashtra)
            self.select_from_dropdown("state", "Maharashtra", "All States")
            
            # Step 2: Select District
            self.select_from_dropdown("district", district, "All Districts")
            
            # Step 3: Select Commodity
            self.select_from_dropdown("commodity", commodity, "All Commodities")
            
            # Step 4: Click GO button
            go_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[span[contains(text(),'Go')]]")
            ))
            self.driver.execute_script("arguments[0].click();", go_button)
            time.sleep(3)
            
            # Step 5: Check for "No Data Available"
            no_data_xpath = "//h3[contains(text(),'No Data Available')]"
            if len(self.driver.find_elements(By.XPATH, no_data_xpath)) > 0:
                logger.warning(f"No data available for {district} - {commodity}")
                return {
                    'district': district,
                    'commodity': commodity,
                    'modal_price_rs_qtl': None,
                    'arrival_quantity_qtl': None
                }
            
            # Step 6: Extract data from first row
            table = self.wait.until(
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
            rows = table.find_elements(By.XPATH, ".//tbody/tr")
            
            if not rows:
                logger.warning(f"No table rows found for {district} - {commodity}")
                return {
                    'district': district,
                    'commodity': commodity,
                    'modal_price_rs_qtl': None,
                    'arrival_quantity_qtl': None
                }
            
            latest_row = rows[0]
            columns = latest_row.find_elements(By.TAG_NAME, "td")
            
            if len(columns) >= 7:
                modal_price = columns[3].text.strip()
                arrival = columns[6].text.strip()
                
                print(f"✓ {district} - {commodity}")
                print(f"  Modal_Price_Rs_Qtl   : {modal_price}")
                print(f"  Arrival_Quantity_Qtl : {arrival}")
                
                return {
                    'district': district,
                    'commodity': commodity,
                    'modal_price_rs_qtl': modal_price,
                    'arrival_quantity_qtl': arrival
                }
            else:
                logger.warning(
                    f"Unexpected table format for {district} - {commodity}"
                )
                return {
                    'district': district,
                    'commodity': commodity,
                    'modal_price_rs_qtl': None,
                    'arrival_quantity_qtl': None
                }

        except Exception as e:
            logger.error(
                f"Error scraping {district} - {commodity}: {e}"
            )
            return {
                'district': district,
                'commodity': commodity,
                'modal_price_rs_qtl': None,
                'arrival_quantity_qtl': None
            }

    def scrape_agmarknet(self, district):
        """
        Orchestrate Selenium scraping for all commodities in a district.
        
        This function:
        1. Initializes Selenium once per district
        2. Iterates through all commodities
        3. Scrapes modal price & arrival quantity
        4. Enriches with real weather data
        5. Deduplicates before inserting to DB
        6. Returns structured data for ETL
        
        Args:
            district (str): District name
        
        Returns:
            list: List of dicts containing scraped + weather data
        """
        scraped_data = []
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        try:
            # Initialize Selenium once per district
            if not self.driver:
                self._init_selenium()
                self.driver.get(self.AGMARKNET_URL)
                time.sleep(5)
            
            # Fetch weather once per district
            weather = self.get_weather(district)
            
            for commodity in self.COMMODITIES:
                try:
                    # Check deduplication
                    cursor = self.conn.cursor()
                    cursor.execute(
                        """SELECT 1 FROM extraction_log 
                           WHERE date_scraped=? AND district=? AND commodity=?""",
                        (current_date, district, commodity)
                    )
                    
                    if cursor.fetchone():
                        logger.info(
                            f"Skipping {district}-{commodity} (Already extracted today)"
                        )
                        continue
                    
                    # Scrape AGMARKNET
                    scraped_row = self.scrape_agmarknet_row(district, commodity)
                    
                    # Merge with weather data
                    row = {
                        'Date': current_date,
                        'District': district,
                        'APMC_Name': f"{district}-Market-Yard",
                        'Commodity': commodity,
                        'Modal_Price_Rs_Qtl': scraped_row['modal_price_rs_qtl'],
                        'Arrival_Quantity_Qtl': scraped_row['arrival_quantity_qtl'],
                        'Temperature_C': weather['temperature'],
                        'Humidity_Pct': weather['humidity'],
                        'Rainfall_Mm': weather['rainfall']
                    }
                    
                    scraped_data.append(row)
                    
                    # Log deduplication
                    cursor.execute(
                        """INSERT INTO extraction_log 
                           VALUES (?, ?, ?, ?)""",
                        (current_date, district, row['APMC_Name'], commodity)
                    )
                    self.conn.commit()
                    
                    # Respectful scraping delay
                    time.sleep(2)

                except Exception as e:
                    logger.error(
                        f"Error processing {district}-{commodity}: {e}. Continuing..."
                    )
                    continue

        except Exception as e:
            logger.error(f"Error scraping district {district}: {e}")
        
        return scraped_data

    def run_extraction(self):
        """
        Main ETL pipeline orchestrator.
        
        Flow:
        1. Iterate through all districts
        2. For each district:
           - Initialize Selenium
           - Scrape AGMARKNET data
           - Enrich with real weather
        3. Combine all data
        4. Deduplicate using extraction_log
        5. Export to Parquet (for Spark processing)
        
        Returns:
            str or None: Path to output Parquet file, or None if no data
        """
        all_data = []
        
        try:
            for district in self.DISTRICTS:
                logger.info(f"Starting extraction for {district}...")
                data = self.scrape_agmarknet(district)
                all_data.extend(data)
                
                # Delay between districts for respectful scraping
                time.sleep(3)
            
            if all_data:
                df = pd.DataFrame(all_data)
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"agri_raw_{timestamp}.parquet"
                output_path = os.path.join(Config.RAW_DIR, filename)
                
                # Ensure output directory exists
                os.makedirs(Config.RAW_DIR, exist_ok=True)
                
                # Save to Parquet for Spark processing
                df.to_parquet(output_path, index=False)
                logger.info(
                    f"Extracted {len(df)} rows to {output_path}"
                )
                print(f"\n✓ Parquet file saved: {output_path}")
                return output_path
            else:
                logger.info("No new data extracted.")
                return None

        finally:
            # Cleanup: Close database and Selenium
            self._cleanup_selenium()
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed.")


if __name__ == "__main__":
    try:
        extractor = AgriExtractor()
        output_file = extractor.run_extraction()
        if output_file:
            logger.info("ETL pipeline completed successfully.")
        else:
            logger.info("ETL pipeline completed with no new data.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)