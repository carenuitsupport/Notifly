# pipeline_utils.py
import time
from collections.abc import Iterable
from typing import Any, Callable, Dict, List, Optional
from log_manager.setup import get_logger

logger = get_logger(__name__)


def as_value(row: Any, name: str) -> Optional[Any]:
    """Return row.name or row[name] if available; else None."""
    try:
        return getattr(row, name)
    except AttributeError:
        try:
            return row[name]
        except (KeyError, TypeError):
            return None


def build_medicare_payload(rows: Iterable) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for r in rows or []:
        payload.append(
            {
                "ProviderType": as_value(r, "ProviderType"),
                "NPI": as_value(r, "NPI"),
                "LocationTaxId": as_value(r, "LocationTaxId"),
                "Medicare_Allowable_Rate_From_MIRRA": as_value(
                    r, "Medicare_Allowable_Rate_From_MIRRA"
                ),
                "Medicare_Allowable_Rate_From_SECUR": as_value(
                    r, "Medicare_Allowable_Rate_From_SECUR"
                ),
                "Facility_Medicare_Allowable_Rate_From_SECUR": as_value(
                    r, "Facility_Medicare_Allowable_Rate_From_SECUR"
                ),
            }
        )
    return payload


def build_terminated_payload(rows: Iterable) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    for r in rows or []:
        payload.append(
            {
                "NPI": as_value(r, "NPI"),
                "FirstName": as_value(r, "FirstName"),
                "MiddleName": as_value(r, "MiddleName"),
                "LastName": as_value(r, "LastName"),
                "PrimaryAddress": as_value(r, "PrimaryAddress"),
                "AddressLine1": as_value(r, "AddressLine1"),
                "AddressLine2": as_value(r, "AddressLine2"),
                "City": as_value(r, "City"),
                "State": as_value(r, "State"),
                "ZipCode": as_value(r, "ZipCode"),
                "County": as_value(r, "County"),
                "Phone": as_value(r, "Phone"),
                "TIN": as_value(r, "TIN"),
                "TerminationDate": as_value(r, "TerminationDate"),
                "ExtractDate": as_value(r, "ExtractDate"),
                "ReceivedDate": as_value(r, "ReceivedDate"),
                "UpdateType": as_value(r, "UpdateType"),
                "Code": as_value(r, "Code"),
                "Description": as_value(r, "Description"),
                "Is_contracted_with_Secur": as_value(r, "Is_contracted_with_Secur"),
                "Have_any_members": as_value(r, "Have_any_members"),
            }
        )
    return payload


def safe_fetch(
    fetch_fn: Callable[[str], Iterable], db_name: str, label: str
) -> List[Any]:
    """
    Fetch with error handling; returns [] on error or non-iterable result.
    """
    try:
        rows = fetch_fn(db_name)
        if rows is None:
            logger.warning("%s fetch returned None; treating as empty.", label)
            return []
        if not isinstance(rows, Iterable):
            logger.error(
                "%s fetch returned non-iterable of type %r; treating as empty.",
                label,
                type(rows),
            )
            return []
        return list(rows)
    except Exception:
        logger.exception("Error fetching %s.", label)
        return []


def upload_with_retry(
    data, upload_fn, *, attempts: int = 3, base_sleep: float = 1.0, **kwargs
) -> None:
    """
    Retry uploads with exponential backoff. Any extra kwargs are forwarded to upload_fn.
    """
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            upload_fn(data, **kwargs)
            return
        except Exception as e:
            last_exc = e
            logger.exception("Upload attempt %d failed.", i)
            if i < attempts:
                time.sleep(base_sleep * (2 ** (i - 1)))
    raise last_exc if last_exc else RuntimeError("Upload failed with unknown error.")
