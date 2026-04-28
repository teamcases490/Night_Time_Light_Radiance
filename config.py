# =========================================================
# CONFIG MANAGER
# File: config.py
# =========================================================

import os
import math
import pandas as pd


# =========================================================
# DEFAULTS
# =========================================================
DEFAULT_FILE = "location.csv"
DEFAULT_LAT = "Latitude"
DEFAULT_LON = "Longitude"
DEFAULT_BATCH_SIZE = 1000


# =========================================================
# SAFE USER INPUT
# =========================================================
def get_input(prompt_text, default_value):

    user_val = input(
        f"{prompt_text} [Default: {default_value}] : "
    ).strip()

    return user_val if user_val else default_value


# =========================================================
# VALIDATE CSV + COLUMNS
# =========================================================
def validate_csv(file_path, lat_col, lon_col):

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"❌ File not found: {file_path}"
        )

    df = pd.read_csv(file_path)

    if lat_col not in df.columns:
        raise ValueError(
            f"❌ Latitude column '{lat_col}' not found in CSV"
        )

    if lon_col not in df.columns:
        raise ValueError(
            f"❌ Longitude column '{lon_col}' not found in CSV"
        )

    return df


# =========================================================
# GET USER CONFIG
# =========================================================
def load_user_config():

    print("\n==============================")
    print("🚀 NTLR PIPELINE CONFIGURATION")
    print("==============================\n")

    file_path = get_input(
        "Enter CSV file name",
        DEFAULT_FILE
    )

    lon_col = get_input(
        "Enter Longitude column name",
        DEFAULT_LON
    )

    lat_col = get_input(
        "Enter Latitude column name",
        DEFAULT_LAT
    )

    while True:

        try:
            batch_size = int(
                get_input(
                    "Enter Batch Size",
                    DEFAULT_BATCH_SIZE
                )
            )

            if batch_size <= 0:
                raise ValueError

            break

        except:
            print("⚠️ Batch size must be a positive integer")

    # Validate file + columns
    df = validate_csv(
        file_path,
        lat_col,
        lon_col
    )

    total_rows = len(df)

    total_batches = math.ceil(
        total_rows / batch_size
    )

    print("\n==============================")
    print("✅ CONFIG VALIDATED")
    print("==============================")
    print(f"📂 File: {file_path}")
    print(f"📍 Latitude Column: {lat_col}")
    print(f"📍 Longitude Column: {lon_col}")
    print(f"📦 Batch Size: {batch_size}")
    print(f"📊 Total Rows: {total_rows}")
    print(f"🧩 Total Batches: {total_batches}")
    print("==============================\n")

    return {
        "file_path": file_path,
        "lat_col": lat_col,
        "lon_col": lon_col,
        "batch_size": batch_size,
        "total_rows": total_rows,
        "total_batches": total_batches,
        "dataframe": df
    }


# =========================================================
# GET SPECIFIC BATCH
# =========================================================
def get_batch(df, batch_num, batch_size):

    start_idx = (batch_num - 1) * batch_size
    end_idx = start_idx + batch_size

    return df.iloc[start_idx:end_idx].copy()


# =========================================================
# MAIN TEST
# =========================================================
if __name__ == "__main__":

    config = load_user_config()

    print("🎯 Sample Batch Preview:")
    sample_batch = get_batch(
        config["dataframe"],
        1,
        config["batch_size"]
    )

    print(sample_batch.head())