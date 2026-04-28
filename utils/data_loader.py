import os
import glob
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
MASTER_FILE = "final_ntlr_enriched.csv"
DATA_FOLDER = "data"
KEYWORD = "enriched"


# =========================================================
# FIND LATEST ENRICHED BATCH FILE
# =========================================================
def get_latest_data_file(
    data_folder=DATA_FOLDER,
    keyword=KEYWORD
):
    """
    Finds latest enriched CSV in data folder
    """

    pattern = os.path.join(
        data_folder,
        f"*{keyword}*.csv"
    )

    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            "❌ No enriched CSV files found in data/"
        )

    latest_file = max(
        files,
        key=os.path.getmtime
    )

    print(
        f"📂 Latest batch file detected: {latest_file}"
    )

    return latest_file


# =========================================================
# LOAD BEST AVAILABLE DATAFRAME
# =========================================================
def load_latest_dataframe():
    """
    Priority:
    1. final_ntlr_enriched.csv
    2. Latest batch enriched CSV
    """

    # ---------------------------------------------
    # MASTER FILE
    # ---------------------------------------------
    if os.path.exists(
        MASTER_FILE
    ):

        print(
            f"📂 Master file detected: {MASTER_FILE}"
        )

        df = pd.read_csv(
            MASTER_FILE
        )

        print(
            f"✅ Loaded master dataframe: {df.shape}"
        )

        return df, MASTER_FILE

    # ---------------------------------------------
    # FALLBACK → BATCH FILE
    # ---------------------------------------------
    latest_file = get_latest_data_file()

    df = pd.read_csv(
        latest_file
    )

    print(
        f"✅ Loaded batch dataframe: {df.shape}"
    )

    return df, latest_file