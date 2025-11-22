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

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AgriExtractor:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH)
        self._init_db()

    def _init_db(self):
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

    def get_weather(self, district):
        """Fetch real-time weather or fallback to mock if no API key"""
        try:
            if "YOUR_" in Config.WEATHER_API_KEY:
                raise ValueError("No API Key provided")
                
            params = {'q': f"{district},IN", 'appid': Config.WEATHER_API_KEY, 'units': 'metric'}
            response = requests.get(Config.WEATHER_URL, params=params, timeout=10)
            data = response.json()
            
            if response.status_code == 200:
                return {
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'rainfall': data.get('rain', {}).get('1h', 0.0) # Rainfall in last hour
                }
        except Exception as e:
            logger.warning(f"Weather API failed for {district}: {e}. Using synthetic data.")
        
        # Synthetic Fallback (for demonstration/resilience)
        return {
            'temperature': round(random.uniform(25, 35), 1),
            'humidity': round(random.uniform(40, 80), 1),
            'rainfall': round(random.uniform(0, 10), 1)
        }

    def scrape_agmarknet(self, district):
        """
        Simulates scraping Agmarknet. 
        Note: Real Agmarknet requires POST requests with ViewState. 
        This function mocks the HTML parsing logic to ensure the pipeline runs.
        """
        logger.info(f"Scraping AGMARKNET for {district}...")
        
        # In a real scenario, you would use requests.post(url, data=form_data)
        # Here we generate a data structure identical to what BS4 would extract
        
        commodities = ['Onion', 'Wheat', 'Rice', 'Soybean', 'Tomato']
        scraped_data = []
        
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        for comm in commodities:
            # Check deduplication
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT 1 FROM extraction_log WHERE date_scraped=? AND district=? AND commodity=?", 
                (current_date, district, comm)
            )
            if cursor.fetchone():
                logger.info(f"Skipping {district}-{comm} (Already extracted)")
                continue

            # Generate Row (Simulating scraped table row)
            row = {
                'Date': current_date,
                'District': district,
                'APMC_Name': f"{district}-Market-Yard",
                'Commodity': comm,
                'Arrival_Quantity_Qtl': random.randint(100, 5000),
                'Min_Price_Rs_Qtl': random.randint(1000, 2000),
                'Max_Price_Rs_Qtl': random.randint(2500, 4000),
                'Modal_Price_Rs_Qtl': random.randint(2100, 3000)
            }
            
            # Fetch Weather match for this row
            weather = self.get_weather(district)
            row.update(weather)
            
            scraped_data.append(row)
            
            # Log success
            cursor.execute(
                "INSERT INTO extraction_log VALUES (?, ?, ?, ?)", 
                (current_date, district, row['APMC_Name'], comm)
            )
        
        self.conn.commit()
        return scraped_data

    def run_extraction(self):
        all_data = []
        for dist in Config.DISTRICTS:
            data = self.scrape_agmarknet(dist)
            all_data.extend(data)
            time.sleep(1) # Respectful scraping delay
            
        if all_data:
            df = pd.DataFrame(all_data)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"agri_raw_{timestamp}.parquet"
            output_path = os.path.join(Config.RAW_DIR, filename)
            
            # Save as Parquet for Spark
            df.to_parquet(output_path, index=False)
            logger.info(f"Extracted {len(df)} rows to {output_path}")
            return output_path
        else:
            logger.info("No new data to extract.")
            return None

if __name__ == "__main__":
    extractor = AgriExtractor()
    extractor.run_extraction()