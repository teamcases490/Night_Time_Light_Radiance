# =========================================================
# GOOGLE DRIVE MOUNT + BATCH DOWNLOAD MANAGER
# File: mount_extracted_data_from_drive.py
# =========================================================

import os
import time
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


# =========================================================
# CONFIG
# =========================================================
CLIENT_SECRET_FILE = "client_secrets.json"
TOKEN_FILE = "token.json"

DOWNLOAD_FOLDER = "data"

MAX_RETRIES = 5
RETRY_WAIT = 15


# =========================================================
# AUTHENTICATE GOOGLE DRIVE
# =========================================================
def authenticate_drive():

    gauth = GoogleAuth()

    # ---------------------------------------------
    # Client Secret
    # ---------------------------------------------
    if not os.path.exists(CLIENT_SECRET_FILE):

        raise FileNotFoundError(
            f"❌ Missing {CLIENT_SECRET_FILE}"
        )

    gauth.LoadClientConfigFile(
        CLIENT_SECRET_FILE
    )

    # ---------------------------------------------
    # Existing Token
    # ---------------------------------------------
    if os.path.exists(TOKEN_FILE):

        try:
            gauth.LoadCredentialsFile(
                TOKEN_FILE
            )

        except Exception:

            print(
                "⚠️ Corrupt token detected. Resetting..."
            )

            os.remove(TOKEN_FILE)

    # ---------------------------------------------
    # First Login
    # ---------------------------------------------
    if gauth.credentials is None:

        print(
            "🌐 First-time Google Drive login..."
        )

        gauth.LocalWebserverAuth()

        gauth.SaveCredentialsFile(
            TOKEN_FILE
        )

        print(
            "✅ Token saved"
        )

    # ---------------------------------------------
    # Expired Token
    # ---------------------------------------------
    elif gauth.access_token_expired:

        if getattr(
            gauth.credentials,
            "refresh_token",
            None
        ):

            print(
                "🔄 Refreshing token..."
            )

            gauth.Refresh()

        else:

            print(
                "⚠️ Re-authentication required..."
            )

            if os.path.exists(
                TOKEN_FILE
            ):
                os.remove(TOKEN_FILE)

            gauth.LocalWebserverAuth()

        gauth.SaveCredentialsFile(
            TOKEN_FILE
        )

        print(
            "✅ Token refreshed"
        )

    # ---------------------------------------------
    # Valid Token
    # ---------------------------------------------
    else:

        print(
            "✅ Using saved token"
        )

        gauth.Authorize()

    return GoogleDrive(gauth)


# =========================================================
# WAIT FOR GEE TASK
# =========================================================
def wait_for_task(
    task,
    poll_interval=20
):

    print("⏳ Waiting for GEE task...")

    while True:

        try:
            status = task.status()
            state = status["state"]

            print(
                f"📡 Task State: {state}"
            )

            # -------------------------------------
            # SUCCESS
            # -------------------------------------
            if state == "COMPLETED":

                print(
                    "✅ Task completed"
                )

                return True

            # -------------------------------------
            # FAILURE
            # -------------------------------------
            elif state == "FAILED":

                raise Exception(
                    f"❌ Task failed: {status}"
                )

            # -------------------------------------
            # RUNNING
            # -------------------------------------
            time.sleep(
                poll_interval
            )

        except Exception as e:

            print(
                f"⚠️ Task status error: {str(e)}"
            )

            time.sleep(
                poll_interval
            )


# =========================================================
# SEARCH SPECIFIC BATCH FILE
# =========================================================
def find_batch_file(
    drive,
    batch_id,
    prefix="ntlr_batch"
):

    query = (
        f"title contains '{prefix}_{batch_id}' "
        f"and trashed=false"
    )

    file_list = drive.ListFile({
        "q": query
    }).GetList()

    if not file_list:
        return None

    # Newest first
    file_list.sort(
        key=lambda x: x["modifiedDate"],
        reverse=True
    )

    return file_list[0]


# =========================================================
# DOWNLOAD SPECIFIC BATCH
# =========================================================
def download_batch_file(
    batch_id,
    output_folder=DOWNLOAD_FOLDER
):

    os.makedirs(
        output_folder,
        exist_ok=True
    )

    output_file = os.path.join(
        output_folder,
        f"ntlr_batch_{batch_id}.csv"
    )

    # Already downloaded
    if os.path.exists(output_file):

        print(
            f"📂 Batch {batch_id} already exists locally"
        )

        return output_file

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            print("\n==============================")
            print(
                f"🔐 Connecting to Google Drive "
                f"(Attempt {attempt}/{MAX_RETRIES})"
            )
            print("==============================")

            drive = authenticate_drive()

            print(
                f"🔍 Searching Batch {batch_id}..."
            )

            batch_file = find_batch_file(
                drive,
                batch_id
            )

            if not batch_file:

                raise Exception(
                    f"Batch {batch_id} not found yet"
                )

            print(
                f"📁 Found: {batch_file['title']}"
            )

            # Download
            batch_file.GetContentFile(
                output_file
            )

            print(
                f"📥 Downloaded → {output_file}"
            )

            return output_file

        except Exception as e:

            print(
                f"⚠️ Download failed: {str(e)}"
            )

            if attempt < MAX_RETRIES:

                print(
                    f"⏳ Retrying in "
                    f"{RETRY_WAIT} sec..."
                )

                time.sleep(
                    RETRY_WAIT
                )

            else:

                raise Exception(
                    f"❌ Failed to download "
                    f"Batch {batch_id}"
                )


# =========================================================
# WAIT + DOWNLOAD
# =========================================================
def wait_and_download(
    task,
    batch_id
):

    wait_for_task(task)

    return download_batch_file(
        batch_id
    )


# =========================================================
# CLEANUP DRIVE BATCH FILE (OPTIONAL)
# =========================================================
def delete_batch_from_drive(
    batch_id,
    prefix="ntlr_batch"
):

    try:

        drive = authenticate_drive()

        batch_file = find_batch_file(
            drive,
            batch_id,
            prefix
        )

        if batch_file:

            batch_file.Delete()

            print(
                f"🗑️ Deleted Drive batch {batch_id}"
            )

    except Exception as e:

        print(
            f"⚠️ Cleanup failed: {str(e)}"
        )


# =========================================================
# MAIN TEST
# =========================================================
if __name__ == "__main__":

    path = download_batch_file(
        batch_id=1
    )

    print(
        f"🚀 Ready → {path}"
    )