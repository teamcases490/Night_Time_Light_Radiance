# =========================================================
# NTLR POSTPROCESS + REALTIME MASTER MERGER
# File: ntlr_postprocess.py
# =========================================================

import os
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
BUFFERS = [500, 1000, 1500, 2000]

MASTER_OUTPUT = "final_ntlr_enriched.csv"


# =========================================================
# SAFE CV
# =========================================================
def safe_divide(a, b):

    if pd.isna(a) or pd.isna(b) or b == 0:
        return None

    return a / b


# =========================================================
# ENRICH SINGLE BATCH
# =========================================================
def enrich_features(
    input_path,
    output_path=None
):

    print("\n==============================")
    print(f"⚙️ ENRICHING: {input_path}")
    print("==============================")

    df = pd.read_csv(input_path)

    # ---------------------------------------------
    # BUFFER DERIVED FEATURES
    # ---------------------------------------------
    for b in BUFFERS:

        # Variance
        df[f"variance_{b}"] = (
            df[f"stdDev_{b}"] ** 2
        )

        # Range
        df[f"range_{b}"] = (
            df[f"max_{b}"] -
            df[f"min_{b}"]
        )

        # IQR
        df[f"iqr_{b}"] = (
            df[f"p75_{b}"] -
            df[f"p25_{b}"]
        )

        # CV
        df[f"cv_{b}"] = df.apply(
            lambda row: safe_divide(
                row[f"stdDev_{b}"],
                row[f"mean_{b}"]
            ),
            axis=1
        )

    # ---------------------------------------------
    # OPTIONAL OUTPUT
    # ---------------------------------------------
    if output_path:

        df.to_csv(
            output_path,
            index=False
        )

        print(
            f"💾 Saved Enriched Batch → {output_path}"
        )

    print(
        f"✅ Enriched Rows: {len(df)}"
    )

    return df


# =========================================================
# APPEND TO MASTER FILE
# =========================================================
def append_to_master(
    enriched_df,
    master_output=MASTER_OUTPUT
):

    # ---------------------------------------------
    # If master exists, prevent duplicates
    # ---------------------------------------------
    if os.path.exists(master_output):

        existing_df = pd.read_csv(
            master_output,
            usecols=["id"]
        )

        existing_ids = set(
            existing_df["id"].tolist()
        )

        enriched_df = enriched_df[
            ~enriched_df["id"].isin(
                existing_ids
            )
        ]

        if enriched_df.empty:

            print(
                "⚠️ No new rows to append"
            )

            return

        enriched_df.to_csv(
            master_output,
            mode="a",
            header=False,
            index=False
        )

    else:

        enriched_df.to_csv(
            master_output,
            index=False
        )

    print(
        f"📈 Master Updated → {master_output}"
    )
    print(
        f"➕ Appended Rows: {len(enriched_df)}"
    )


# =========================================================
# FULL BATCH PROCESS
# =========================================================
def process_batch(
    batch_csv_path,
    batch_id,
    master_output=MASTER_OUTPUT,
    keep_batch_enriched=True
):

    # ---------------------------------------------
    # Individual enriched batch path
    # ---------------------------------------------
    batch_output = None

    if keep_batch_enriched:

        batch_output = batch_csv_path.replace(
            ".csv",
            "_enriched.csv"
        )

    # ---------------------------------------------
    # Enrich
    # ---------------------------------------------
    enriched_df = enrich_features(
        batch_csv_path,
        batch_output
    )

    # ---------------------------------------------
    # Merge to Master
    # ---------------------------------------------
    append_to_master(
        enriched_df,
        master_output
    )

    print("\n==============================")
    print(
        f"✅ BATCH {batch_id} PROCESSED"
    )
    print("==============================")

    return master_output


# =========================================================
# VERIFY MASTER FILE
# =========================================================
def get_master_status(
    master_output=MASTER_OUTPUT
):

    if not os.path.exists(
        master_output
    ):

        return {
            "exists": False,
            "rows": 0
        }

    df = pd.read_csv(
        master_output
    )

    return {
        "exists": True,
        "rows": len(df),
        "columns": len(df.columns)
    }


# =========================================================
# RESET MASTER
# =========================================================
def reset_master(
    master_output=MASTER_OUTPUT
):

    if os.path.exists(
        master_output
    ):

        os.remove(
            master_output
        )

        print(
            f"🗑️ Removed {master_output}"
        )


# =========================================================
# MAIN TEST
# =========================================================
if __name__ == "__main__":

    sample = "data/ntlr_batch_1.csv"

    if os.path.exists(sample):

        process_batch(
            sample,
            batch_id=1
        )

        print(
            get_master_status()
        )

    else:

        print(
            f"⚠️ Sample file missing: {sample}"
        )