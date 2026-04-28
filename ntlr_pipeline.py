# =========================================================
# NTLR MASTER PIPELINE
# File: ntlr_pipeline.py
# =========================================================

import os
import time
from tqdm import tqdm

from config import (
    load_user_config,
    get_batch
)

from pipeline_state import (
    initialize_pipeline,
    mark_batch_completed,
    mark_batch_downloaded,
    mark_batch_processed,
    is_batch_downloaded,
    is_batch_processed,
    print_pipeline_status
)

from ntlr_extractor import (
    run_extraction
)

from mount_extracted_data_from_drive import (
    wait_for_task,
    download_batch_file
)

from ntlr_postprocess import (
    process_batch,
    get_master_status
)


# =========================================================
# PIPELINE CONFIG
# =========================================================
FINAL_OUTPUT = "final_ntlr_enriched.csv"

POST_GEE_WAIT = 10


# =========================================================
# PROCESS DOWNSTREAM BATCH
# (Download + Postprocess)
# =========================================================
def process_completed_batch(
    batch_id
):

    print("\n==============================")
    print(
        f"📥 PROCESSING COMPLETED BATCH {batch_id}"
    )
    print("==============================")

    # ---------------------------------------------
    # DOWNLOAD
    # ---------------------------------------------
    if not is_batch_downloaded(
        batch_id
    ):

        batch_file = download_batch_file(
            batch_id
        )

        mark_batch_downloaded(
            batch_id
        )

    else:

        batch_file = os.path.join(
            "data",
            f"ntlr_batch_{batch_id}.csv"
        )

        print(
            f"📂 Using local batch file: {batch_file}"
        )

    # ---------------------------------------------
    # POSTPROCESS
    # ---------------------------------------------
    if not is_batch_processed(
        batch_id
    ):

        process_batch(
            batch_file,
            batch_id,
            FINAL_OUTPUT
        )

        mark_batch_processed(
            batch_id
        )

    else:

        print(
            f"⚙️ Batch {batch_id} already processed"
        )


# =========================================================
# MAIN PIPELINE
# =========================================================
def run_pipeline():

    # ---------------------------------------------
    # USER CONFIG
    # ---------------------------------------------
    config = load_user_config()

    df = config["dataframe"]

    total_batches = config[
        "total_batches"
    ]

    batch_size = config[
        "batch_size"
    ]

    lat_col = config[
        "lat_col"
    ]

    lon_col = config[
        "lon_col"
    ]

    # ---------------------------------------------
    # INIT / RESUME STATE
    # ---------------------------------------------
    state = initialize_pipeline(
        total_batches=total_batches,
        input_file=config[
            "file_path"
        ],
        final_output=FINAL_OUTPUT
    )

    start_batch = (
        state[
            "last_completed_batch"
        ] + 1
    )

    print_pipeline_status()

    # ---------------------------------------------
    # PREVIOUS TASK HOLDER
    # ---------------------------------------------
    previous_task = None
    previous_batch_id = None

    # =====================================================
    # MAIN LOOP WITH PROGRESS BAR
    # =====================================================
    with tqdm(
        range(start_batch, total_batches + 1),
        desc="🚀 Overall Pipeline Progress",
        unit="batch",
        dynamic_ncols=True
    ) as progress_bar:

        for batch_id in progress_bar:

            progress_bar.set_postfix({
                "Current": f"{batch_id}/{total_batches}",
                "Remaining": total_batches - batch_id
            })

            print("\n################################")
            print(
                f"🚀 STARTING BATCH {batch_id}/{total_batches}"
            )
            print("################################")

            # -----------------------------------------
            # CREATE CURRENT BATCH
            # -----------------------------------------
            batch_df = get_batch(
                df,
                batch_id,
                batch_size
            )

            # -----------------------------------------
            # START CURRENT GEE EXPORT
            # -----------------------------------------
            current_task = run_extraction(
                batch_df=batch_df,
                batch_id=batch_id,
                lat_col=lat_col,
                lon_col=lon_col
            )

            # -----------------------------------------
            # WHILE CURRENT RUNS:
            # PROCESS PREVIOUS
            # -----------------------------------------
            if (
                previous_task is not None
                and previous_batch_id is not None
            ):

                print(
                    f"⏳ Waiting previous Batch "
                    f"{previous_batch_id}"
                )

                wait_for_task(
                    previous_task
                )

                mark_batch_completed(
                    previous_batch_id
                )

                process_completed_batch(
                    previous_batch_id
                )

            # -----------------------------------------
            # SHIFT TASK FOR NEXT ITERATION
            # -----------------------------------------
            previous_task = current_task
            previous_batch_id = batch_id

            time.sleep(
                POST_GEE_WAIT
            )

    # =====================================================
    # FINAL BATCH REMAINING
    # =====================================================
    if (
        previous_task is not None
        and previous_batch_id is not None
    ):

        print("\n==============================")
        print(
            f"🏁 FINALIZING LAST BATCH "
            f"{previous_batch_id}"
        )
        print("==============================")

        wait_for_task(
            previous_task
        )

        mark_batch_completed(
            previous_batch_id
        )

        process_completed_batch(
            previous_batch_id
        )

    # =====================================================
    # FINAL STATUS
    # =====================================================
    final_status = get_master_status(
        FINAL_OUTPUT
    )

    print("\n################################")
    print("🎉 PIPELINE COMPLETE")
    print("################################")

    print(
        f"📂 Final Output: {FINAL_OUTPUT}"
    )

    print(
        f"📊 Total Final Rows: "
        f"{final_status['rows']}"
    )

    print(
        f"🧠 Total Columns: "
        f"{final_status['columns']}"
    )

    print_pipeline_status()


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":

    try:

        run_pipeline()

    except KeyboardInterrupt:

        print(
            "\n⚠️ Pipeline interrupted manually."
        )

        print(
            "♻️ Resume supported from last successful batch."
        )

    except Exception as e:

        print(
            f"\n❌ PIPELINE FAILED: {str(e)}"
        )

        print(
            "♻️ Restart script to resume automatically."
        )