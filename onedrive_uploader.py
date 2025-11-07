import requests
import configparser
import msal
import pandas as pd
from io import BytesIO
from datetime import datetime
from log_manager.setup import get_logger
from typing import Any, Dict, List, Optional
import os
import sys



logger = get_logger(__name__)

_INVALID_SHEET_CHARS = set("[]:*?/\\")

def _config_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def get_onedrive_credentials():
    config = configparser.ConfigParser()
    config_path = os.path.join(_config_dir(), "config.ini")
    logger.info("Loading OneDrive configuration from %s", config_path)
    read_files = config.read(config_path)
    if not read_files:
        logger.warning("config.ini could not be read from %s", config_path)
    return config["OneDrive_Config"]


def get_access_token():
    credentials = get_onedrive_credentials()
    authority = f"https://login.microsoftonline.com/{credentials['tenant_id']}"
    client_id = credentials["client_id"]
    client_secret = credentials["client_secret"]

    app = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )

    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception("Unable to get access token.")


def generate_filename(filename):
    current_date = datetime.now()
    new_filename = current_date.strftime(
        f"{filename}_%B_%d_%y.xlsx"
    )  # Format: MedicareRate_MMMM_DD_YY.xlsx
    return new_filename


def validate_sheet_name(name: str) -> None:
    if not name:
        raise ValueError("sheet_name is required.")
    if len(name) > 31:
        raise ValueError("Excel sheet names must be 31 characters or fewer.")
    if any(ch in name for ch in _INVALID_SHEET_CHARS):
        raise ValueError(r"Excel sheet names cannot contain any of []:*?/\ characters.")
    if name.endswith("'"):
        raise ValueError("Excel sheet names cannot end with a single quote (').")


def upload_to_onedrive(
    data: List[Dict[str, Any]],
    *,
    sheet_name: str,
    filename: Optional[str] = None,
    folder_path: Optional[str] = None,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Uploads a single DataFrame (built from `data`) as an .xlsx file with a single worksheet.

    Args:
        data: list of dict rows
        sheet_name: Excel worksheet name (validated)
        filename: optional file name; defaults to generate_filename()
        folder_path: optional site-relative path; defaults to your current folder
        timeout: HTTP timeout in seconds

    Returns:
        The JSON response from Graph (e.g., file metadata).

    Raises:
        Exception on unrecoverable errors (HTTP or otherwise).
    """
    try:
        validate_sheet_name(sheet_name)

        # Build DataFrame
        df = pd.DataFrame(data or [])
        if df.empty:
            logger.warning(
                "upload_to_onedrive: data is empty; uploading an empty sheet."
            )

        # Resolve defaults
        resolved_filename = generate_filename(filename)
        
        resolved_folder = (
                folder_path
                or "General/SECUR - Central Operations Management Hub/Claims File Exchange & Audit Oversight/Audit Reports"
            )


        access_token = get_access_token()
        creds = get_onedrive_credentials()
        site_id = creds["site_id"]
        drive_id = creds["drive_id"]

        # Prepare Excel in-memory
        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name=sheet_name)
            buffer.seek(0)

            # Upload (PUT to /content will create or overwrite)
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{resolved_folder}/{resolved_filename}:/content"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }

            resp = requests.put(url, headers=headers, data=buffer, timeout=timeout)

        if resp.status_code in (200, 201):
            logger.info("Upload succeeded: %s", resolved_filename)
            return resp.json()

        # Try to surface Graph error details
        try:
            err = resp.json()
        except ValueError:
            err = {"text": resp.text}
        raise RuntimeError(f"Upload failed ({resp.status_code}): {err}")

    except Exception:
        logger.exception("upload_to_onedrive failed.")
        raise
