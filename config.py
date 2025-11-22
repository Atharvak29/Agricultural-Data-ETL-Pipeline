import os

class Config:
    # --- Paths ---
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    RAW_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
    DB_PATH = os.path.join(DATA_DIR, 'metadata', 'ingestion_state.db')
    
    # --- Lookup Files ---
    SOIL_LOOKUP_PATH = os.path.join(DATA_DIR, 'static', 'soil_lookup.csv')
    CROP_META_PATH = os.path.join(DATA_DIR, 'static', 'crop_metadata.csv')

    # --- API Configs ---
    # In production, use os.getenv('WEATHER_API_KEY')
    WEATHER_API_KEY = "YOUR_OPENWEATHER_API_KEY" 
    WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
    
    # --- Target Districts (Maharashtra) ---
    DISTRICTS = ['Pune', 'Nashik', 'Nagpur', 'Satara', 'Aurangabad', 'Solapur']
    
    # --- Column Schema (Strict) ---
    FINAL_COLUMNS = [
        "N", "P", "K", "temperature", "humidity", "ph", "rainfall", 
        "Commodity", "Season", "District", "APMC_Name", "Date", 
        "Soil_Type", "Water_Requirement", "Modal_Price_Rs_Qtl", 
        "Min_Price_Rs_Qtl", "Max_Price_Rs_Qtl", "Arrival_Quantity_Qtl", 
        "Typical_Costs_Rs_per_Ha", "Export_Potential", "Average_Yield_Tons_per_Ha", 
        "NPK_Ratio", "Temp_Humidity_Index", "Rainfall_pH_Interaction", 
        "Price_to_Cost_Ratio", "Nutrient_Balance", 
        "Soil_Type_Encoded", "Water_Requirement_Encoded", "Season_Encoded", 
        "Export_Potential_Encoded", "District_Encoded", "Commodity_Encoded"
    ]

    CATEGORICAL_COLS = [
        "Soil_Type", "Water_Requirement", "Season", 
        "Export_Potential", "District", "Commodity"
    ]

    # --- Spark Settings ---
    SPARK_APP_NAME = "Maharashtra_Agri_ETL"
    
    @staticmethod
    def ensure_dirs():
        for d in [Config.RAW_DIR, Config.PROCESSED_DIR, 
                  os.path.dirname(Config.DB_PATH), 
                  os.path.join(Config.DATA_DIR, 'static')]:
            os.makedirs(d, exist_ok=True)

# Initialize structure
Config.ensure_dirs()