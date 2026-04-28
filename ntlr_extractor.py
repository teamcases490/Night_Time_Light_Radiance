# =========================================================
# NTLR V5 BATCHED EXTRACTOR
# File: ntlr_extractor.py
# =========================================================

import time
import ee
import pandas as pd


# =========================================================
# CONFIG
# =========================================================
PROJECT_ID = "incomeestimationcase-468413"

VIIRS_COLLECTION = (
    "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"
)

BUFFERS = [500, 1000, 1500, 2000]

MAX_RETRIES = 3
RETRY_WAIT = 15


# =========================================================
# INITIALIZE GEE
# =========================================================
def initialize_gee():

    try:
        ee.Initialize(project=PROJECT_ID)

    except Exception:

        print("🌐 Authenticating Google Earth Engine...")
        ee.Authenticate()
        ee.Initialize(project=PROJECT_ID)

    print("✅ Google Earth Engine Ready")


initialize_gee()


# =========================================================
# DATASET
# =========================================================
viirs = ee.ImageCollection(
    VIIRS_COLLECTION
).select("avg_rad")

img_2025 = viirs.filterDate(
    "2025-03-01",
    "2025-04-01"
).mean()


# =========================================================
# BUILD POINT FEATURE COLLECTION
# =========================================================
def build_points(
    batch_df,
    lat_col="Latitude",
    lon_col="Longitude"
):

    features = []

    for i, row in batch_df.iterrows():

        point = ee.Feature(
            ee.Geometry.Point([
                row[lon_col],
                row[lat_col]
            ]),
            {
                "id": int(i),
                "source_row": int(i)
            }
        )

        features.append(point)

    return ee.FeatureCollection(features)


# =========================================================
# FEATURE ENGINEERING FUNCTION
# =========================================================
def compute_features(feature):

    geom = feature.geometry()

    props = ee.Dictionary(
        feature.toDictionary()
    )

    # Preserve exact coordinates
    coords = geom.coordinates()

    props = props.set(
        "Latitude",
        coords.get(1)
    )

    props = props.set(
        "Longitude",
        coords.get(0)
    )

    # =====================================================
    # CURRENT YEAR MULTI-BUFFER FEATURES
    # =====================================================
    for b in BUFFERS:

        buffer_geom = geom.buffer(b)

        stats = img_2025.reduceRegion(
            reducer=(
                ee.Reducer.mean()
                .combine(
                    ee.Reducer.median(),
                    "",
                    True
                )
                .combine(
                    ee.Reducer.stdDev(),
                    "",
                    True
                )
                .combine(
                    ee.Reducer.minMax(),
                    "",
                    True
                )
                .combine(
                    ee.Reducer.percentile(
                        [25, 75]
                    ),
                    "",
                    True
                )
            ),
            geometry=buffer_geom,
            scale=500,
            bestEffort=True,
            maxPixels=1e13
        )

        stats_dict = ee.Dictionary({

            f"mean_{b}":
                stats.get("avg_rad_mean"),

            f"median_{b}":
                stats.get("avg_rad_median"),

            f"stdDev_{b}":
                stats.get("avg_rad_stdDev"),

            f"min_{b}":
                stats.get("avg_rad_min"),

            f"max_{b}":
                stats.get("avg_rad_max"),

            f"p25_{b}":
                stats.get("avg_rad_p25"),

            f"p75_{b}":
                stats.get("avg_rad_p75"),
        })

        props = props.combine(stats_dict)

    # =====================================================
    # HISTORICAL (2020–2025)
    # =====================================================
    for year in range(2020, 2026):

        hist_img = viirs.filterDate(
            f"{year}-03-01",
            f"{year}-04-01"
        ).mean()

        hist_val = hist_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom.buffer(500),
            scale=500,
            bestEffort=True,
            maxPixels=1e13
        ).get("avg_rad")

        props = props.set(
            f"hist_{year}",
            hist_val
        )

    return ee.Feature(
        geom,
        props
    )


# =========================================================
# CREATE EXPORT TASK
# =========================================================
def create_export_task(
    result_fc,
    batch_id,
    folder="EarthEngine"
):

    task = ee.batch.Export.table.toDrive(
        collection=result_fc,
        description=f"NTLR_BATCH_{batch_id}",
        folder=folder,
        fileNamePrefix=f"ntlr_batch_{batch_id}",
        fileFormat="CSV"
    )

    return task


# =========================================================
# RUN EXTRACTION
# =========================================================
def run_extraction(
    batch_df,
    batch_id,
    lat_col="Latitude",
    lon_col="Longitude"
):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print("\n==============================")
            print(f"🚀 STARTING BATCH {batch_id}")
            print("==============================")
            print(
                f"📦 Rows: {len(batch_df)}"
            )
            print(
                f"🔁 Attempt: {attempt}/{MAX_RETRIES}"
            )

            # Build points
            points = build_points(
                batch_df,
                lat_col,
                lon_col
            )

            # Compute features
            result = points.map(
                compute_features
            )

            # Export task
            task = create_export_task(
                result,
                batch_id
            )

            task.start()

            print(
                f"✅ Batch {batch_id} Export Submitted"
            )

            return task

        except Exception as e:

            print(
                f"⚠️ Batch {batch_id} failed on attempt "
                f"{attempt}: {str(e)}"
            )

            if attempt < MAX_RETRIES:

                print(
                    f"⏳ Retrying in "
                    f"{RETRY_WAIT} sec..."
                )

                time.sleep(RETRY_WAIT)

            else:

                raise Exception(
                    f"❌ Batch {batch_id} failed permanently "
                    f"after {MAX_RETRIES} attempts"
                )


# =========================================================
# CHECK TASK STATUS
# =========================================================
def get_task_state(task):

    try:
        status = task.status()

        return status.get(
            "state",
            "UNKNOWN"
        )

    except:
        return "UNKNOWN"


# =========================================================
# MAIN TEST
# =========================================================
if __name__ == "__main__":

    sample_df = pd.read_csv(
        "location.csv"
    ).head(10)

    task = run_extraction(
        batch_df=sample_df,
        batch_id=1,
        lat_col="Latitude",
        lon_col="Longitude"
    )

    print(
        f"📡 Task State: "
        f"{get_task_state(task)}"
    )