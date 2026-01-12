import re
from datetime import datetime

import pandas as pd
from pyathena.cursor import Cursor
from pyathena.pandas.util import as_pandas

# Ordered column output for final CSV upload
required_columns = [
    "# LLINE",
    "PIN* [PARID]",
    "Local Permit No.* [USER28]",
    "Issue Date* [PERMDT]",
    "Desc 1* [DESC1]",
    "Desc 2 Code 1 [USER6]",
    "Desc 2 Code 2 [USER7]",
    "Desc 2 Code 3 [USER8]",
    "Amount* [AMOUNT]",
    "Assessable [IS_ASSESS]",
    "Applicant Street Address* [ADDR1]",
    "Applicant Address 2 [ADDR2]",
    "Applicant City, State, Zip* [ADDR3]",
    "Contact Phone* [PHONE]",
    "Applicant* [USER21]",
    "Notes [NOTE1]",
    "Occupy Dt [UDATE1]",
    "Submit Dt* [CERTDATE]",
    "Est Comp Dt [UDATE2]",
]

filled_columns = [
    "PIN* [PARID]",
    "Local Permit No.* [USER28]",
    "Issue Date* [PERMDT]",
    "Amount* [AMOUNT]",
    "Applicant Street Address* [ADDR1]",
    "Applicant* [USER21]",
    "Applicant City, State, Zip* [ADDR3]",
    "Notes [NOTE1]",
]


def normalize_pin(pin_vec):
    # remove - from PIN
    pin_vec = re.sub("-", "", pin_vec)

    # If pin is 13 digits add leading 0
    if len(pin_vec) == 13:
        pin_vec = "0" + pin_vec

    # If PIN is 10 digits add 4 final digits
    if len(pin_vec) == 10:
        pin_vec = pin_vec + "0000"

    # If pin is 9 digits do both
    if len(pin_vec) == 9:
        pin_vec = "0" + pin_vec + "0000"

    return pin_vec


def year_from_date_string(date_str: str) -> str:
    """Parse a date string in YYYY-MM-DD format and return a string representing
    the year of the date"""

    return str(datetime.strptime(date_str, "%Y-%m-%d").year)


def get_pin_cache_filename(start_date: str, end_date: str) -> str:
    """Given start and end dates, return the name of a file that we can use to
    cache distinct PINs between the years represented by the two dates"""

    # Assume that dates are already validated for YYYY-MM-DD format
    start_year = year_from_date_string(start_date)
    end_year = year_from_date_string(end_date)

    return f"chicago_pin_universe-{start_year}-{end_year}.csv"


# Output data will be distinct by PIN and address. In the case where a PIN changes its address or has multiple addresses, it will appear twice.
def pull_existing_pins_from_athena(
    cursor: Cursor, start_date: str, end_date: str
) -> pd.DataFrame:
    """Connect to Athena and download all PINs in Chicago between the given
    start and end dates"""

    # Assume that dates are already validated for YYYY-MM-DD format
    start_year = year_from_date_string(start_date)
    end_year = year_from_date_string(end_date)

    SQL_QUERY = """
    SELECT DISTINCT
        CAST(u.pin AS varchar) AS pin,
        CAST(u.pin10 AS varchar) AS pin10,
        a.prop_address_full
    FROM default.vw_pin_universe u
    LEFT JOIN default.vw_pin_address a
        ON u.pin = a.pin
        AND u.year = a.year
    WHERE u.triad_name = 'City'
    AND u.year BETWEEN %(start_year)s AND %(end_year)s;
    """
    cursor.execute(SQL_QUERY, {"start_year": start_year, "end_year": end_year})
    chicago_pin_universe = as_pandas(cursor)
    pin_cache_filename = get_pin_cache_filename(start_date, end_date)
    chicago_pin_universe.to_csv(pin_cache_filename, index=False)

    return chicago_pin_universe


def finalize_columns(
    df: pd.DataFrame,
    filled_columns: list[str],
    chicago_pin_universe: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    df_flagged = df.copy()

    df_flagged["valid_filled"] = df_flagged[filled_columns].notna().all(axis=1)

    df_flagged["valid_pin"] = (
        df_flagged["PIN* [PARID]"].astype(str).str.len() == 14
    )
    df_flagged["valid_permit"] = (
        df_flagged["Local Permit No.* [USER28]"]
        .astype(str)
        .str.len()
        .isin([9, 10])
    )
    df_flagged["valid_addr_len"] = (
        df_flagged["Applicant Street Address* [ADDR1]"].astype(str).str.len()
        <= 40
    )
    df_flagged["valid_note_len"] = (
        df_flagged["Notes [NOTE1]"].astype(str).str.len() <= 2000
    )
    df_flagged["valid_name_len"] = (
        df_flagged["Applicant* [USER21]"].astype(str).str.len() <= 50
    )

    df_flagged["valid_amount"] = pd.to_numeric(
        df_flagged["Amount* [AMOUNT]"], errors="coerce"
    ).notnull() & (
        pd.to_numeric(df_flagged["Amount* [AMOUNT]"], errors="coerce")
        < 2147483647
    )

    df_flagged["pin14_in_data"] = df_flagged["PIN* [PARID]"].isin(
        chicago_pin_universe["pin"]
    )

    df_flagged["valid_row"] = (
        df_flagged["valid_filled"]
        & df_flagged["valid_pin"]
        & df_flagged["valid_permit"]
        & df_flagged["valid_addr_len"]
        & df_flagged["valid_note_len"]
        & df_flagged["valid_name_len"]
        & df_flagged["pin14_in_data"]
    )

    upload = df_flagged[df_flagged["valid_row"]].copy()
    upload["# LLINE"] = range(1, len(upload) + 1)
    upload = upload.loc[:, ~upload.columns.str.startswith("valid_")]

    need_review = df_flagged[~df_flagged["valid_row"]].copy()
    need_review["# LLINE"] = range(1, len(need_review) + 1)

    return {"upload": upload, "need_review": need_review}
