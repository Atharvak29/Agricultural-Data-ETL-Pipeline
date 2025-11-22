import pandas as pd
import os
import sys

# Add parent dir to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def generate_static_data():
    print("Generating Static Lookup Tables...")

    # 1. Soil Health Lookup (Mock data based on Maharashtra averages)
    soil_data = {
        'District': ['Pune', 'Nashik', 'Nagpur', 'Satara', 'Aurangabad', 'Solapur'],
        'Soil_Type': ['Black Cotton', 'Red Laterite', 'Deep Black', 'Medium Black', 'Clay Loam', 'Sandy Loam'],
        'N': [250, 180, 280, 220, 200, 190], # kg/ha
        'P': [40, 35, 45, 38, 42, 30],
        'K': [300, 280, 350, 310, 320, 290],
        'ph': [7.2, 6.5, 7.5, 7.0, 7.8, 7.1],
        'Nutrient_Balance': ['Balanced', 'Deficient', 'Surplus', 'Balanced', 'Surplus', 'Deficient']
    }
    df_soil = pd.DataFrame(soil_data)
    df_soil.to_csv(Config.SOIL_LOOKUP_PATH, index=False)
    print(f"Soil lookup saved to {Config.SOIL_LOOKUP_PATH}")

    # 2. Crop Metadata (Ministry Stats)
    crop_data = {
        'Commodity': ['Onion', 'Wheat', 'Rice', 'Soybean', 'Cotton', 'Tomato'],
        'Season': ['Rabi', 'Rabi', 'Kharif', 'Kharif', 'Kharif', 'Kharif'],
        'Water_Requirement': ['Medium', 'Medium', 'High', 'Low', 'Low', 'High'],
        'Typical_Costs_Rs_per_Ha': [45000, 30000, 40000, 25000, 35000, 60000],
        'Average_Yield_Tons_per_Ha': [15, 3.5, 4.0, 2.5, 2.0, 20],
        'Export_Potential': ['High', 'Medium', 'High', 'High', 'High', 'Medium']
    }
    df_crop = pd.DataFrame(crop_data)
    df_crop.to_csv(Config.CROP_META_PATH, index=False)
    print(f"Crop metadata saved to {Config.CROP_META_PATH}")

if __name__ == "__main__":
    Config.ensure_dirs()
    generate_static_data()