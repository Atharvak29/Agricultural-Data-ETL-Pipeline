from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
import sys
import os
import pandas as pd

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def get_latest_file(directory):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
    if not files:
        return None
    return max(files, key=os.path.getctime)

def process_data():
    spark = SparkSession.builder \
        .appName(Config.SPARK_APP_NAME) \
        .config("spark.sql.caseSensitive", "false") \
        .getOrCreate()

    raw_file = get_latest_file(Config.RAW_DIR)
    if not raw_file:
        print("No raw data found to process.")
        return

    print(f"Processing file: {raw_file}")

    # 1. Load Data
    df_raw = spark.read.parquet(raw_file)
    df_soil = spark.createDataFrame(pd.read_csv(Config.SOIL_LOOKUP_PATH))
    df_crop = spark.createDataFrame(pd.read_csv(Config.CROP_META_PATH))

    # 2. Joins (Enrichment)
    # Join with Soil (on District)
    df_enriched = df_raw.join(df_soil, on="District", how="left")
    
    # Join with Crop Meta (on Commodity)
    df_enriched = df_enriched.join(df_crop, on="Commodity", how="left")

    # Handle Missing Values (Imputation)
    df_enriched = df_enriched.fillna({
        'rainfall': 0.0,
        'Typical_Costs_Rs_per_Ha': 0,
        'Average_Yield_Tons_per_Ha': 0
    })

    # 3. Feature Engineering
    
    # NPK Ratio (Avoid division by zero)
    df_feat = df_enriched.withColumn(
        "NPK_Ratio", 
        F.col("N") / (F.col("P") + F.col("K") + 0.01)
    )

    # THI (Temperature Humidity Index) for livestock/stress
    # Formula: (1.8 * T + 32) - (0.55 - 0.0055 * RH) * (1.8 * T - 26)
    df_feat = df_feat.withColumn(
        "Temp_Humidity_Index",
        (1.8 * F.col("temperature") + 32) - 
        ((0.55 - 0.0055 * F.col("humidity")) * (1.8 * F.col("temperature") - 26))
    )

    # Rainfall * pH Interaction
    df_feat = df_feat.withColumn(
        "Rainfall_pH_Interaction", 
        F.col("rainfall") * F.col("ph")
    )

    # Price to Cost Ratio (Profitability)
    # Assuming Yield * Modal Price = Revenue per Ha
    df_feat = df_feat.withColumn(
        "Revenue_Per_Ha",
        F.col("Average_Yield_Tons_per_Ha") * 10 * F.col("Modal_Price_Rs_Qtl") # 1 Ton = 10 Qtls
    )
    
    df_feat = df_feat.withColumn(
        "Price_to_Cost_Ratio",
        F.when(F.col("Typical_Costs_Rs_per_Ha") > 0, 
               F.col("Revenue_Per_Ha") / F.col("Typical_Costs_Rs_per_Ha"))
        .otherwise(0)
    )

    # 4. String Encoding (ML Preparation)
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_Encoded", handleInvalid="keep")
        for c in Config.CATEGORICAL_COLS
    ]
    
    pipeline = Pipeline(stages=indexers)
    model = pipeline.fit(df_feat)
    df_final = model.transform(df_feat)

    # 5. Final Selection & Reordering
    # Ensure all columns exist
    for col_name in Config.FINAL_COLUMNS:
        if col_name not in df_final.columns:
            df_final = df_final.withColumn(col_name, F.lit(None))

    df_final = df_final.select(Config.FINAL_COLUMNS)

    # 6. Output
    # Parquet Lake
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_out = os.path.join(Config.PROCESSED_DIR, f"ml_ready_{timestamp}.parquet")
    df_final.write.mode("overwrite").parquet(parquet_out)
    
    # Excel (Pandas)
    excel_out = os.path.join(Config.PROCESSED_DIR, f"Maharashtra_Agri_ML_Ready_{timestamp}.xlsx")
    pd_df = df_final.toPandas()
    pd_df.to_excel(excel_out, index=False)

    print(f"Success! Files generated:\n{parquet_out}\n{excel_out}")
    spark.stop()

if __name__ == "__main__":
    import datetime
    process_data()