# =========================================================
# PIPELINE STATE MANAGER
# File: pipeline_state.py
# =========================================================

import os
import json
from datetime import datetime


# =========================================================
# CONFIG
# =========================================================
STATE_FILE = "pipeline_state.json"


# =========================================================
# DEFAULT STATE STRUCTURE
# =========================================================
def default_state():
    return {
        "last_completed_batch": 0,
        "downloaded_batches": [],
        "processed_batches": [],
        "total_batches": 0,
        "input_file": None,
        "final_output": "final_ntlr_enriched.csv",
        "last_updated": None
    }


# =========================================================
# LOAD STATE
# =========================================================
def load_state():

    # No state file yet
    if not os.path.exists(STATE_FILE):
        return default_state()

    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)

        # Ensure missing keys are added
        base = default_state()
        for key, value in base.items():
            if key not in state:
                state[key] = value

        return state

    except Exception:
        print("⚠️ Corrupt pipeline state detected. Resetting...")
        backup_corrupt_state()
        return default_state()


# =========================================================
# SAVE STATE
# =========================================================
def save_state(state):

    state["last_updated"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


# =========================================================
# BACKUP CORRUPT STATE
# =========================================================
def backup_corrupt_state():

    if os.path.exists(STATE_FILE):

        backup_name = (
            f"pipeline_state_corrupt_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        try:
            os.rename(STATE_FILE, backup_name)
            print(f"🛟 Corrupt state backed up → {backup_name}")
        except:
            pass


# =========================================================
# INITIALIZE NEW RUN
# =========================================================
def initialize_pipeline(
    total_batches,
    input_file,
    final_output="final_ntlr_enriched.csv"
):

    # Existing state
    state = load_state()

    # If previous run exists for same file, resume
    if (
        state["input_file"] == input_file
        and state["total_batches"] == total_batches
        and state["last_completed_batch"] < total_batches
    ):

        print("\n==============================")
        print("♻️ RESUME DETECTED")
        print("==============================")
        print(
            f"📌 Resuming from Batch "
            f"{state['last_completed_batch'] + 1}"
        )
        print("==============================\n")

        return state

    # Otherwise fresh state
    state = default_state()

    state["total_batches"] = total_batches
    state["input_file"] = input_file
    state["final_output"] = final_output

    save_state(state)

    print("\n==============================")
    print("🆕 NEW PIPELINE INITIALIZED")
    print("==============================")
    print(f"📂 Input File: {input_file}")
    print(f"🧩 Total Batches: {total_batches}")
    print("==============================\n")

    return state


# =========================================================
# MARK BATCH COMPLETED
# =========================================================
def mark_batch_completed(batch_num):

    state = load_state()

    if batch_num > state["last_completed_batch"]:
        state["last_completed_batch"] = batch_num

    save_state(state)


# =========================================================
# MARK BATCH DOWNLOADED
# =========================================================
def mark_batch_downloaded(batch_num):

    state = load_state()

    if batch_num not in state["downloaded_batches"]:
        state["downloaded_batches"].append(batch_num)

    save_state(state)


# =========================================================
# MARK BATCH PROCESSED
# =========================================================
def mark_batch_processed(batch_num):

    state = load_state()

    if batch_num not in state["processed_batches"]:
        state["processed_batches"].append(batch_num)

    save_state(state)


# =========================================================
# CHECKERS
# =========================================================
def is_batch_downloaded(batch_num):

    state = load_state()

    return batch_num in state["downloaded_batches"]


def is_batch_processed(batch_num):

    state = load_state()

    return batch_num in state["processed_batches"]


# =========================================================
# GET NEXT BATCH
# =========================================================
def get_next_batch():

    state = load_state()

    next_batch = state["last_completed_batch"] + 1

    if next_batch > state["total_batches"]:
        return None

    return next_batch


# =========================================================
# RESET PIPELINE
# =========================================================
def reset_pipeline():

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    print("🗑️ Pipeline state reset complete")


# =========================================================
# STATUS DISPLAY
# =========================================================
def print_pipeline_status():

    state = load_state()

    print("\n==============================")
    print("📊 PIPELINE STATUS")
    print("==============================")
    print(f"📂 Input File: {state['input_file']}")
    print(f"🧩 Total Batches: {state['total_batches']}")
    print(
        f"✅ Last Completed Batch: "
        f"{state['last_completed_batch']}"
    )
    print(
        f"📥 Downloaded: "
        f"{len(state['downloaded_batches'])}"
    )
    print(
        f"⚙️ Processed: "
        f"{len(state['processed_batches'])}"
    )
    print(f"🕒 Last Updated: {state['last_updated']}")
    print("==============================\n")


# =========================================================
# MAIN TEST
# =========================================================
if __name__ == "__main__":

    state = initialize_pipeline(
        total_batches=20,
        input_file="location.csv"
    )

    print_pipeline_status()

    mark_batch_completed(1)
    mark_batch_downloaded(1)
    mark_batch_processed(1)

    print_pipeline_status()