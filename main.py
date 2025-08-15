# main.py
from log_manager.setup import get_logger, setup_logging
import sys

from db_connector import (
    fetch_medicare_rate_mismatch_data,
    fetch_multiplan_terminated_providers,
)
from onedrive_uploader import upload_to_onedrive

from pipeline_utils import (
    safe_fetch,
    build_medicare_payload,
    build_terminated_payload,
    upload_with_retry,
)

setup_logging()
logger = get_logger(__name__)


def main():

    database_name = "SECUR_INSIGHTS"

    logger.info("Fetching Medicare rate mismatch data from %s...", database_name)
    medicare_rows = safe_fetch(
        fetch_medicare_rate_mismatch_data, database_name, "Medicare rate mismatch"
    )

    logger.info("Fetching Multiplan terminated providers from %s...", database_name)
    terminated_rows = safe_fetch(
        fetch_multiplan_terminated_providers,
        database_name,
        "Multiplan terminated providers",
    )

    # Upload each dataset separately
    if medicare_rows:
        logger.info("Preparing Medicare payload (%d rows)...", len(medicare_rows))
        medicare_payload = build_medicare_payload(medicare_rows)
        logger.info("Uploading Medicare payload to OneDrive...")

        if medicare_payload:
            upload_with_retry(
                medicare_payload,
                upload_fn=upload_to_onedrive,
                sheet_name="MedicareRateMismatch",
                filename="MedicareRate",
            )
        else:
            logger.warning("No Medicare rate mismatch rows to upload.")

    if terminated_rows:
        logger.info(
            "Preparing Terminated Providers payload (%d rows)...", len(terminated_rows)
        )
        terminated_payload = build_terminated_payload(terminated_rows)
        logger.info("Uploading Terminated Providers payload to OneDrive...")
        if terminated_payload:
            upload_with_retry(
                terminated_payload,
                upload_fn=upload_to_onedrive,
                sheet_name="TerminatedProviders",
                filename="MultiplanTerminatedProviders",
            )
    else:
        logger.warning("No terminated providers rows to upload.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        sys.exit(130)
    except Exception:
        logger.exception("Fatal error in main.")
        sys.exit(1)
