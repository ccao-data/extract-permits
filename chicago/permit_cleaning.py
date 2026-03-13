"""
Chicago Permit Ingest Process - Automation

This script automates the current process for cleaning permit data from the Chicago Data Portal's Building Permits table
and preparing it for upload to iasWorld via SmartFile. This involves fetching the data, cleaning up certain fields,
organizing columns to match the SmartFile template, and outputting all permits to a single Excel workbook. The workbook
contains a "Permits" tab with inline error formulas and validation for analyst review, and a "Universe of Valid PINs"
tab used for PIN validation.

The following optional environment variables can be set:
    AWS_ATHENA_S3_STAGING_DIR: S3 path where Athena query results are stored
    AWS_REGION: Region that AWS operations should be run in

The script also expects three positional arguments:
    * start_date (str, YYYY-MM-DD): The lower bound date to use for filtering permits
    * end_date (str, YYYY-MM-DD): The upper bound date to use for filtering
    * deduplicate (bool): Whether to filter out permits that already exist in iasworld
"""

import decimal
import os
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import xlsxwriter
from pyathena import connect
from pyathena.cursor import Cursor
from pyathena.pandas.util import as_pandas


def parse_args() -> tuple[str, str, bool]:
    """Helper function to parse and validate command line args to this script"""
    if len(sys.argv) < 4:
        print(
            "Usage: permit_cleaning.py <start_date> <end_date> <deduplicate>"
        )
        sys.exit(1)

    start_date_str, end_date_str, deduplicate = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
    )

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        print(
            f"Invalid start_date format: '{start_date_str}'. Expected YYYY-MM-DD."
        )
        sys.exit(1)

    try:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print(
            f"Invalid end_date format: '{end_date_str}'. Expected YYYY-MM-DD."
        )
        sys.exit(1)

    if end_date < start_date:
        print("Error: end_date must be later than or equal to start_date.")
        sys.exit(1)

    deduplicate = deduplicate.lower() == "true"

    return start_date_str, end_date_str, deduplicate


def get_current_assessment_year(cursor: Cursor) -> str:
    """Query Athena for the current assessment year"""
    # Use PARDAT as the source of truth for the current assessment year.
    # This choice is somewhat arbitrary, since we could theoretically use
    # any table as the source of truth for this info, but PARDAT feels
    # relevant given that it is the base for our PIN universe and the
    # main purpose of this year is to help us construct that universe
    cursor.execute("""
        SELECT MAX(taxyr)
        FROM iasworld.pardat
        WHERE cur = 'Y'
            AND deactivat IS NULL
    """)
    return cursor.fetchall()[0][0]


def get_pin_cache_filename(year: str) -> str:
    """Given a year, return the name of a file that we can use to cache
    distinct PINs in that year"""
    return f"chicago_pin_universe_{year}.csv"


def pull_existing_pins_from_athena(cursor: Cursor, year: str) -> pd.DataFrame:
    """Connect to Athena and download all PINs in Chicago for a given year,
    saving the resulting data to a cache file according to the year."""
    SQL_QUERY = """
    SELECT
        CAST(u.pin AS varchar) AS pin,
        CAST(u.pin10 AS varchar) AS pin10,
        a.prop_address_full
    FROM default.vw_pin_universe u
    LEFT JOIN default.vw_pin_address a
        ON u.pin = a.pin
        AND u.year = a.year
    WHERE u.triad_name = 'City'
    AND u.year = %(year)s
    """
    cursor.execute(SQL_QUERY, {"year": year})
    chicago_pin_universe = as_pandas(cursor)
    pin_cache_filename = get_pin_cache_filename(year)
    chicago_pin_universe.to_csv(pin_cache_filename, index=False)

    return chicago_pin_universe


def download_permits(start_date: str, end_date: str) -> pd.DataFrame:
    """Download permits from the Chicago open data portal in the dataframe
    with issue dates between `start_date` and `end_date`"""
    params = {
        # Assume we've already validated the start and end date strings for
        # YYYY-MM-DD format
        "$where": f"issue_date between '{start_date}' and '{end_date}'",
        "$order": "issue_date DESC",
        "$limit": 10000000,  # Artificial limit to override the default
    }
    url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"
    permits_response = requests.get(url, params=params)
    permits_response.raise_for_status()
    permits = permits_response.json()
    permits_df = pd.DataFrame(permits)
    return permits_df


def expand_multi_pin_permits(df):
    """
    Data from the Chicago open data permits table (data this script works with) has rows uniquely identified by permit number.
    Permits can apply to multiple PINs, in which case the PIN_LIST column will
    be a pipe-separated value representing multiple PINs.
    We want rows that are uniquely identified by PIN and permit number.
    This function creates new rows for each PIN in multi-PIN permits and saves the relevant PIN in pin_solo.
    """

    def parse_pin_list(pin_list_str):
        """Parse the PIN_LIST column. The column is formatted like so:

        * If no PINs are in the list, the value is NA, and we want to keep it
        * Otherwise, PINs are stored as pipe-separated values, and we want to
          parse them as lists so that we can pivot them out to one
          permit per PIN
        """
        if pd.isna(pin_list_str):
            return np.nan
        elif " | " in pin_list_str:
            pin_list = pin_list_str.split(" | ")
            # Remove duplicates while maintaining list order
            return list(dict.fromkeys(pin_list))
        else:
            return [pin_list_str]

    df["pin_list"] = df["pin_list"].apply(parse_pin_list)

    # Retain rows where pin_list is NA, since the pivot operation will
    # remove them but we want to keep them
    na_rows = df[df["pin_list"].isna()]
    df = df.dropna(subset=["pin_list"])

    # Pivot the dataframe longer by "pin_list" so that we have one row
    # per PIN with "solo_pin" being the PIN column
    df = (
        df.explode("pin_list")
        .reset_index(drop=True)
        .rename(columns={"pin_list": "solo_pin"})
    )

    # Add a column to track the position of the PIN in the PIN list for
    # ordering, starting at 1
    df["pin_type"] = df.groupby("permit_").cumcount() + 1
    df["pin_type"] = "pin" + df["pin_type"].astype(str)

    # Add back the NA rows
    df = pd.concat([df, na_rows], ignore_index=True)

    # Sort by the permit number, then the position of the PIN in the list,
    # so that PINs will be in order of their permit number and list position
    # in the output table
    df = df.sort_values(by=["permit_", "pin_type"]).reset_index(drop=True)

    return df


# update pin to match formatting of iasWorld
def format_pin(df):
    # iasWorld format doesn't include dashes
    df["pin_final"] = df["solo_pin"].astype("string").str.replace("-", "")

    # add zeros to 10-digit PINs to transform into 14-digits PINs
    def pad_pin(pin):
        if not pd.isna(pin):
            if len(pin) == 10:
                return pin + "0000"
            else:
                return pin
        else:
            return ""

    df["pin_final"] = df["pin_final"].apply(pad_pin)
    return df


# Eliminate columns not included in permit upload and rename and order to match Smartfile excel format
def organize_columns(df):
    address_columns = ["street_number", "street_direction", "street_name"]
    df[address_columns] = df[address_columns].fillna("")
    df["Address"] = df[address_columns].astype("string").agg(" ".join, axis=1)

    df["issue_date"] = pd.to_datetime(
        df["issue_date"], format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce"
    ).dt.strftime("%-m/%-d/%Y")

    column_renaming_dict = {
        "pin_final": "PIN* [PARID]",
        "permit_": "Local Permit No.* [USER28]",
        "issue_date": "Issue Date* [PERMDT]",
        "reported_cost": "Amount* [AMOUNT]",
        "Address": "Applicant Street Address* [ADDR1]",
        "city_state": "Applicant City, State, Zip* [ADDR3]",
        "contact_1_name": "Applicant* [USER21]",
        "work_description": "Notes [NOTE1]",
    }

    data_relevant = df[
        [col for col in df.columns if col in column_renaming_dict]
    ]
    data_renamed = data_relevant.rename(columns=column_renaming_dict)

    column_order = [
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

    data_all_cols = data_renamed.assign(
        **{col: None for col in column_order if col not in data_renamed}
    )
    data_ordered = data_all_cols[column_order]
    return data_ordered


def shorten_applicant_names(df):
    """Abbreviate common long words in the Applicant field to help stay
    within the 50-character iasWorld limit."""
    name_shortening_dict = {
        "ASSOCIATION": "ASSN",
        "COMPANY": "CO",
        "BUILDING": "BLDG",
        "FOUNDATION": "FNDN",
        "ILLINOIS": "IL",
        "STREET": "ST",
        "BOULEVARD": "BLVD",
        "AVENUE": "AVE",
        "APARTMENT": "APT",
        "APARTMENTS": "APTS",
        "MANAGEMENT": "MGMT",
        "CORPORATION": "CORP",
        "INCORPORATED": "INC",
        "LIMITED": "LTD",
        "PLAZA": "PLZ",
    }
    df["Applicant* [USER21]"] = df["Applicant* [USER21]"].replace(
        name_shortening_dict, regex=True
    )
    return df


def round_amount(df):
    """Round Amount to the nearest dollar — SmartFile does not accept
    decimal amounts."""
    df["Amount* [AMOUNT]"] = (
        pd.to_numeric(df["Amount* [AMOUNT]"], errors="coerce")
        .round()
        .astype("Int64")
    )
    return df


# List of keywords to identify likely assessable permits.
# This is produced via a document provided by Valuations
# and Data Integrity.
# Build was in the provided document
# but is a component of too many words (building)

keywords = [
    "Addition",
    "Elevator",
    "Window",
    "Construction",
    "Garage",
    "Roof",
    "Demolition",
    "HVAC",
    "Flatwork",
    "Expand",
    "Basement",
    "Alarm",
    "Fire",
    "Bathroom",
    "Solar",
    "New",
    "Attic",
    "Vacant",
    "Conversion",
    "Rehab",
    "Enclosed porch",
    "Alteration",
    "EFP",
    "ADU",
    "A.D.U.",
    "Coach",
    "Accessory",
    "Extension",
    "Dormer",
    "Erect",
    "Proposed",
    "Wreck",
    "Finish",
    "Rec Room",
    "Convert",
    "Recreation room",
    "Sun Room",
    "Season",
]


# Join addresses and format columns
def add_address_link_and_suggested_pins(df, chicago_pin_universe):
    # Collapse multiple pins per address into a single comma-separated string
    pin_map = (
        chicago_pin_universe.groupby(["prop_address_full"])["pin"]
        .apply(lambda pins: ", ".join(pins.astype(str).unique()))
        .reset_index()
    )

    # Merge using the collapsed mapping
    df = df.merge(
        pin_map,
        left_on=["Applicant Street Address* [ADDR1]"],
        right_on=["prop_address_full"],
        how="left",
    )

    # Insert Property Address column right after the Applicant Street Address column
    df.insert(
        df.columns.get_loc("Applicant Street Address* [ADDR1]") + 1,
        "Property Address",
        df["Applicant Street Address* [ADDR1]"],
    )

    # Suggested PINs (replace NA with empty string)
    df = df.rename(columns={"pin": "Suggested PINs"})
    df["Suggested PINs"] = df["Suggested PINs"].fillna("")

    # Drop the prop_address_full column (no longer needed)
    df = df.drop(columns=["prop_address_full"])

    # Add hyperlink for the Property Address
    df["Property Address"] = df["Property Address"].apply(
        lambda addr: (
            f'=HYPERLINK("https://maps.cookcountyil.gov/cookviewer/?search={addr}", "{addr}")'
            if pd.notna(addr)
            else ""
        )
    )

    # This uses three techniques to add a suggested PIN. If there is no PIN, it will say "NO PIN FOUND".
    # If there is a single 14-digit PIN, it will be a hyperlink.
    # If there are more than one PINs, it will be a comma-separated list of PINs. This is both the
    # result of joining based on pin10 and the fact that multiple pins may have the same address.
    def make_pin_hyperlink(pin_str):
        if pd.isna(pin_str):
            return "NO PIN FOUND"

        digits = re.sub(r"\D", "", pin_str)
        if len(digits) == 14:
            return f'=HYPERLINK("https://www.cookcountyassessoril.gov/pin/{digits}", "{pin_str}")'
        # This will be a list of comma separated pins
        return pin_str

    df["Suggested PINs"] = df["Suggested PINs"].apply(make_pin_hyperlink)

    # Create a comma separated list of matched keywords. This is derived from
    # the list called keywords.
    df = df.assign(
        **{
            "Matched Keywords": df["Notes [NOTE1]"].apply(
                lambda note: ", ".join(
                    [kw for kw in keywords if kw.lower() in str(note).lower()]
                )
            )
        }
    )
    return df


def deduplicate_permits(cursor, df, start_date, end_date):
    cursor.execute(
        """
            SELECT
                parid,
                permdt,
                amount,
                note2,
                user21,
                user28,
                user43
            FROM iasworld.permit
            WHERE permdt BETWEEN %(start_date)s AND %(end_date)s
        """,
        {"start_date": start_date, "end_date": end_date},
    )
    existing_permits = as_pandas(cursor)
    workbook_to_iasworld_col_map = {
        "PIN* [PARID]": "parid",
        "Issue Date* [PERMDT]": "permdt",
        "Amount* [AMOUNT]": "amount",
        "Applicant Street Address* [ADDR1]": "note2",
        "Local Permit No.* [USER28]": "user28",
        "Notes [NOTE1]": "user43",
    }
    new_permits = df.copy()
    for workbook_key, iasworld_key in workbook_to_iasworld_col_map.items():
        new_permits[iasworld_key] = new_permits[workbook_key]

    # Transform new columns to ensure they match the iasworld formatting
    new_permits["amount"] = new_permits["amount"].apply(
        lambda x: decimal.Decimal("{:.2f}".format(x))
        if not pd.isnull(x)
        else x
    )
    new_permits["permdt"] = (
        pd.to_datetime(new_permits["permdt"], dayfirst=False)
        .dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        .str[:-3]
    )
    new_permits["note2"] = new_permits["note2"] + ",,CHICAGO, IL"
    new_permits["user43"] = (
        new_permits["user43"]
        # Replace special characters that Smartfile removes
        .str.replace(r"""[():;+#*&'"@½]""", "", regex=True)
        # Truncate description to match Smartfile length limit
        .str.slice(0, 259)
    )

    # Antijoin new_permits to existing_permits to find permits that do
    # not exist in iasworld
    merged_permits = pd.merge(
        new_permits,
        existing_permits,
        how="left",
        on=list(workbook_to_iasworld_col_map.values()),
        indicator=True,
    )
    true_new_permits = merged_permits[merged_permits["_merge"] == "left_only"]
    true_new_permits = true_new_permits.drop("_merge", axis=1)
    for iasworld_key in workbook_to_iasworld_col_map.values():
        true_new_permits = true_new_permits.drop(iasworld_key, axis=1)

    return true_new_permits


def gen_file_base_name(start_date, end_date):
    return f"{start_date}_to_{end_date}_permits_"


def _build_textjoin_errors_formula(row: int) -> str:
    """Return the TEXTJOIN formula for the Errors column at a given row,
    matching the demo workbook logic."""
    return (
        f'=_xlfn.TEXTJOIN(", ", TRUE, '
        f'IF(LEN(TRIM(D{row}))=0, "Missing PIN14", ""), '
        f'IF(COUNTIF(\'Universe of Valid PINs\'!A:A, D{row}) > 0, "", "Provide Valid Pin"), '
        f'IF(LEN(TRIM(D{row}))<>14, "PIN is not 14 digits", ""), '
        f'IF(LEN(R{row})>50, "Applicant Name > 50 characters", ""), '
        f'IF(LEN(F{row})>40, "Address > 40 characters", ""), '
        f'IF(LEN(W{row})>2000, "Work Description > 2000 characters", ""), '
        f'IF(AND(ISNUMBER(M{row}), M{row}>2147483647), "Amount exceeds limit", ""), '
        f'IF(OR(NOT(ISNUMBER(H{row})), H{row}=""), "Missing or Invalid Issue Date", ""), '
        f'IF(OR(M{row}="", NOT(ISNUMBER(M{row}))), "Missing Amount", ""), '
        f'IF(LEN(TRIM(R{row}))=0, "Missing Applicant", ""), '
        f'IF(LEN(TRIM(F{row}))=0, "Missing Applicant Street Address", ""), '
        f'IF(LEN(TRIM(G{row}))=0, "Missing Permit Number", ""), '
        f'IF(LEN(TRIM(W{row}))=0, "Missing Work Description", "")'
        f")"
    )


# ---------------------------------------------------------------------------
# PERMITS_COLUMNS

# Key for every column on the "Permits" sheet.
# Keys are sequential column indices (0-based).
#
# ---------------------------------------------------------------------------
PERMITS_COLUMNS = {
    # col 0 — Row Number (computed)
    0: {
        "header": "Row Number",
        "src": None,
        "width": 12,
        "fmt_locked": "normal",
        "fmt_unlocked": "normal",
        "col_default": "normal",
        "hidden": False,
        "cell_type": "row_number",
        "error_check": None,
        "validation": None,
    },
    # col 1 — Errors (TEXTJOIN formula)
    1: {
        "header": "Errors",
        "src": None,
        "width": 67,
        "fmt_locked": "normal",
        "fmt_unlocked": "normal",
        "col_default": "normal",
        "hidden": False,
        "cell_type": "formula",
        "error_check": None,
        "validation": None,
    },
    # col 2 — Suggested PINs (always unlocked, wraps, warning-only validation)
    2: {
        "header": "Suggested PINs",
        "src": "Suggested PINs",
        "width": 50,
        "fmt_locked": "unlocked_wrap_col",
        "fmt_unlocked": "unlocked_wrap_col",
        "col_default": "unlocked_wrap_col",
        "hidden": False,
        "cell_type": "suggested_pins",
        "error_check": None,
        "validation": {
            "validate": "custom",
            "value": '=C2="343343434343"',
            "error_type": "warning",
            "show_error": True,
            "error_title": "Suggested PINs",
            "error_message": "Make sure that changes to PIN values are in PIN column.",
        },
    },
    # col 3 — PIN (text / zero-padded; unlocked when invalid)
    3: {
        "header": "PIN",
        "src": "PIN* [PARID]",
        "width": 25,
        "fmt_locked": "pin_fmt",
        "fmt_unlocked": "pin_unlocked_fmt",
        "col_default": "pin_unlocked_fmt",
        "hidden": False,
        "cell_type": "pin",
        "error_check": lambda row, valid_pins: (
            len(str(row.get("PIN* [PARID]", "") or "").strip()) == 0
            or len(str(row.get("PIN* [PARID]", "") or "").strip()) != 14
            or str(row.get("PIN* [PARID]", "") or "").strip() not in valid_pins
        ),
        "validation": {
            "validate": "custom",
            "value": "=AND(LEN(TRIM(D2))=14,COUNTIF('Universe of Valid PINs'!$A:$A,D2)>0)",
            "ignore_blank": False,
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid PIN",
            "error_message": "PIN must be 14 digits and exist in the Universe of Valid PINs.",
        },
    },
    # col 4 — Suggested Property Address (hyperlink, locked)
    4: {
        "header": "Suggested Property Address",
        "src": "Property Address",
        "width": 25,
        "fmt_locked": "hyperlink_fmt",
        "fmt_unlocked": "hyperlink_fmt",
        "col_default": "normal",
        "hidden": False,
        "cell_type": "hyperlink_locked",
        "error_check": None,
        "validation": None,
    },
    # col 5 — Applicant Street Address (unlocked when empty or > 40 chars)
    5: {
        "header": "Applicant Street Address",
        "src": "Applicant Street Address* [ADDR1]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "unlocked_wrap",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": lambda row, _: (
            len(
                str(
                    row.get("Applicant Street Address* [ADDR1]", "") or ""
                ).strip()
            )
            == 0
            or len(
                str(
                    row.get("Applicant Street Address* [ADDR1]", "") or ""
                ).strip()
            )
            > 40
        ),
        "validation": {
            "validate": "text length",
            "criteria": "between",
            "minimum": 1,
            "maximum": 40,
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Address",
            "error_message": "Address must be between 1 and 40 characters.",
        },
    },
    # col 6 — Local Permit No. (unlocked when empty)
    6: {
        "header": "Local Permit No.",
        "src": "Local Permit No.* [USER28]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "unlocked_wrap",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": lambda row, _: (
            len(str(row.get("Local Permit No.* [USER28]", "") or "").strip())
            == 0
        ),
        "validation": {
            "validate": "text length",
            "criteria": "greater than or equal to",
            "value": 1,
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Permit No.",
            "error_message": "Permit No. must not be empty.",
        },
    },
    # col 7 — Issue Date (date serial; unlocked when empty)
    7: {
        "header": "Issue Date",
        "src": "Issue Date* [PERMDT]",
        "width": 25,
        "fmt_locked": "date_fmt",
        "fmt_unlocked": "date_unlocked_fmt",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "date",
        "error_check": lambda row, _: (
            len(str(row.get("Issue Date* [PERMDT]", "") or "").strip()) == 0
        ),
        "validation": {
            "validate": "date",
            "criteria": "greater than or equal to",
            "value": "1900-01-01",
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Date",
            "error_message": "Issue Date must be a valid date.",
        },
    },
    # col 8 — Desc 1 (locked, hidden)
    8: {
        "header": "Desc 1* [DESC1]",
        "src": "Desc 1* [DESC1]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 9 — Desc 2 Code 1 (hidden)
    9: {
        "header": "Desc 2 Code 1 [USER6]",
        "src": "Desc 2 Code 1 [USER6]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 10 — Desc 2 Code 2 (hidden)
    10: {
        "header": "Desc 2 Code 2 [USER7]",
        "src": "Desc 2 Code 2 [USER7]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 11 — Desc 2 Code 3 (hidden)
    11: {
        "header": "Desc 2 Code 3 [USER8]",
        "src": "Desc 2 Code 3 [USER8]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 12 — Amount (hidden; unlocked when missing or over limit)
    12: {
        "header": "Amount",
        "src": "Amount* [AMOUNT]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "unlocked_wrap",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": lambda row, _: (
            (lambda v: v is None or pd.isna(v) or float(v) > 2147483647)(
                row.get("Amount* [AMOUNT]", None)
            )
        ),
        "validation": {
            "validate": "custom",
            "value": "=AND(ISNUMBER(M2),M2>=0,M2<=2147483647)",
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Amount",
            "error_message": "Amount must be a whole number between 0 and 2,147,483,647.",
        },
    },
    # col 13 — Assessable (locked, hidden)
    13: {
        "header": "Assessable [IS_ASSESS]",
        "src": "Assessable [IS_ASSESS]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 14 — Applicant Address 2 (hidden)
    14: {
        "header": "Applicant Address 2 [ADDR2]",
        "src": "Applicant Address 2 [ADDR2]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 15 — Applicant City, State, Zip
    15: {
        "header": "Applicant City, State, Zip* [ADDR3]",
        "src": "Applicant City, State, Zip* [ADDR3]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 16 — Contact Phone (locked, hidden)
    16: {
        "header": "Contact Phone* [PHONE]",
        "src": "Contact Phone* [PHONE]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 17 — Applicant (unlocked when empty or > 50 chars)
    17: {
        "header": "Applicant",
        "src": "Applicant* [USER21]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "unlocked_wrap",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": lambda row, _: (
            len(str(row.get("Applicant* [USER21]", "") or "").strip()) == 0
            or len(str(row.get("Applicant* [USER21]", "") or "").strip()) > 50
        ),
        "validation": {
            "validate": "text length",
            "criteria": "between",
            "minimum": 1,
            "maximum": 50,
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Applicant",
            "error_message": "Applicant must be between 1 and 50 characters.",
        },
    },
    # col 18 — Matched Keywords (locked, no validation)
    18: {
        "header": "Matched Keywords",
        "src": "Matched Keywords",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 19 — Occupy Dt (hidden)
    19: {
        "header": "Occupy Dt [UDATE1]",
        "src": "Occupy Dt [UDATE1]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 20 — Submit Dt (hidden)
    20: {
        "header": "Submit Dt* [CERTDATE]",
        "src": "Submit Dt* [CERTDATE]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 21 — Est Comp Dt (hidden)
    21: {
        "header": "Est Comp Dt [UDATE2]",
        "src": "Est Comp Dt [UDATE2]",
        "width": 25,
        "fmt_locked": "wrap",
        "fmt_unlocked": "wrap",
        "col_default": "normal",
        "hidden": True,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 22 — Work Description (unlocked when empty or > 2000 chars)
    22: {
        "header": "Work Description",
        "src": "Notes [NOTE1]",
        "width": 50,
        "fmt_locked": "wrap",
        "fmt_unlocked": "unlocked_wrap",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": lambda row, _: (
            len(str(row.get("Notes [NOTE1]", "") or "").strip()) == 0
            or len(str(row.get("Notes [NOTE1]", "") or "").strip()) > 2000
        ),
        "validation": {
            "validate": "text length",
            "criteria": "between",
            "minimum": 1,
            "maximum": 2000,
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Work Description",
            "error_message": "Work Description must be between 1 and 2000 characters.",
        },
    },
    # col 23 — Errors are Resolved (checkbox; validation blocks check when errors remain)
    23: {
        "header": "Errors are Resolved",
        "src": None,
        "width": 25,
        "fmt_locked": "checkbox_unlocked",
        "fmt_unlocked": "checkbox_unlocked",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "checkbox",
        "error_check": None,
        "validation": {
            "validate": "custom",
            "value": '=$B2=""',
            "show_error": True,
            "error_type": "stop",
            "error_title": "Errors not resolved",
            "error_message": "This row still has errors in column B. Fix them before marking resolved.",
        },
    },
    # col 24 — Reviewer Name (blank, always unlocked)
    24: {
        "header": "Reviewer Name",
        "src": None,
        "width": 25,
        "fmt_locked": "unlocked_normal",
        "fmt_unlocked": "unlocked_normal",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
    # col 25 — Reviewer Notes (blank, always unlocked)
    25: {
        "header": "Reviewer Notes",
        "src": None,
        "width": 25,
        "fmt_locked": "unlocked_normal",
        "fmt_unlocked": "unlocked_normal",
        "col_default": "unlocked_normal",
        "hidden": False,
        "cell_type": "normal",
        "error_check": None,
        "validation": None,
    },
}


def save_xlsx_files(df, file_base_name, chicago_pin_universe):
    df_all = df.reset_index(drop=True)

    print(f"# rows total: {len(df_all)}")

    output_folder = (
        datetime.today().date().strftime("files_for_review_%Y_%m_%d")
    )
    os.makedirs(output_folder, exist_ok=True)

    file_name = os.path.join(output_folder, file_base_name + "permits.xlsx")
    workbook = xlsxwriter.Workbook(file_name)

    # ------------------------------------------------------------------ #
    #  "Permits" sheet                                                     #
    # ------------------------------------------------------------------ #

    n_data_rows = len(df_all)

    # Pre-compute valid PIN set once for O(1) lookups per row
    valid_pins = set(chicago_pin_universe["pin"].values)

    # --- Format registry ---
    # Every xlsxwriter format object is defined here in full, keyed by the
    # same name strings used in PERMITS_COLUMNS (fmt_locked / fmt_unlocked /
    # col_default). Each entry is completely self-contained.
    formats = {
        key: workbook.add_format(value)
        for key, value in {
            "bold": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
                "bold": True,
            },
            "normal": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "wrap": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "hidden_col": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "hyperlink_fmt": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
                "font_color": "blue",
                "underline": True,
            },
            "unlocked_normal": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "unlocked_wrap": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "unlocked_wrap_col": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": True,
                "num_format": "0.##",
            },
            "checkbox_unlocked": {
                "font_name": "Arial",
                "locked": False,
                "align": "center",
                "text_wrap": False,
                "num_format": "0.##",
            },
            "pin_fmt": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "@",
            },
            "pin_unlocked_fmt": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": False,
                "num_format": "@",
            },
            "date_fmt": {
                "font_name": "Arial",
                "locked": True,
                "align": "left",
                "text_wrap": False,
                "num_format": "mm/dd/yyyy",
            },
            "date_unlocked_fmt": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": False,
                "num_format": "mm/dd/yyyy",
            },
            "hyperlink_unlocked_fmt": {
                "font_name": "Arial",
                "locked": False,
                "align": "left",
                "text_wrap": False,
                "num_format": "0.##",
                "font_color": "blue",
                "underline": True,
            },
        }.items()
    }

    ws = workbook.add_worksheet("Permits")
    ws.freeze_panes(1, 0)

    # --- Apply column widths, default formats, and hide flags from the dict ---
    for ci, col_def in PERMITS_COLUMNS.items():
        col_fmt = formats[col_def["col_default"]]
        if col_def["hidden"]:
            ws.set_column(
                ci,
                ci,
                col_def["width"],
                formats["hidden_col"],
                {"hidden": True},
            )
        else:
            ws.set_column(ci, ci, col_def["width"], col_fmt)

    # --- Header row ---
    for ci, col_def in PERMITS_COLUMNS.items():
        ws.write(0, ci, col_def["header"], formats["bold"])

    # --- Data rows ---
    for row_idx, (_, row_data) in enumerate(df_all.iterrows(), start=1):
        xl_row = row_idx

        for ci, col_def in PERMITS_COLUMNS.items():
            cell_type = col_def["cell_type"]
            error_check = col_def["error_check"]

            # Determine whether this cell is in error
            in_error = (
                bool(error_check(row_data, valid_pins))
                if error_check
                else False
            )

            locked_fmt = formats[col_def["fmt_locked"]]
            unlocked_fmt = formats[col_def["fmt_unlocked"]]
            cell_fmt = unlocked_fmt if in_error else locked_fmt

            # --- Computed / special cell types ---
            if cell_type == "row_number":
                ws.write(xl_row, ci, row_idx, locked_fmt)
                continue

            if cell_type == "formula":
                ws.write_formula(
                    xl_row,
                    ci,
                    _build_textjoin_errors_formula(xl_row + 1),
                    locked_fmt,
                )
                continue

            if cell_type == "checkbox":
                ws.insert_checkbox(
                    xl_row, ci, False, formats["checkbox_unlocked"]
                )
                continue

            # --- Source-value cells ---
            src = col_def["src"]
            if src is None:
                # Blank editable cells (Reviewer Name / Reviewer Notes)
                continue

            val = row_data.get(src)
            if not isinstance(val, str) and pd.isna(val):
                val = None

            if val is None:
                continue

            # Hyperlink cells (value is an Excel HYPERLINK formula string)
            if isinstance(val, str) and val.startswith("=HYPERLINK("):
                if cell_type == "suggested_pins":
                    ws.write_formula(
                        xl_row, ci, val, formats["hyperlink_unlocked_fmt"]
                    )
                else:
                    ws.write_formula(xl_row, ci, val, formats["hyperlink_fmt"])
                continue

            # Suggested PINs non-hyperlink (plain text / "NO PIN FOUND")
            if cell_type == "suggested_pins":
                ws.write(xl_row, ci, val, formats["unlocked_wrap_col"])
                continue

            # PIN: zero-pad to 14 digits, use text format
            if cell_type == "pin":
                val = str(val).zfill(14)
                ws.write(xl_row, ci, val, cell_fmt)
                continue

            # Date: parse to datetime object and write as Excel date serial
            if cell_type == "date":
                try:
                    parsed = pd.to_datetime(
                        str(val).strip(), dayfirst=False
                    ).to_pydatetime()
                    ws.write_datetime(xl_row, ci, parsed, cell_fmt)
                except (ValueError, pd.errors.ParserError):
                    ws.write(xl_row, ci, val, cell_fmt)
                continue

            # Default: plain write
            ws.write(xl_row, ci, val, cell_fmt)

        ws.set_row(xl_row, None)  # auto height

    # Conditional formatting to produce dynamic excel color changing
    if n_data_rows > 0:
        last_col = max(PERMITS_COLUMNS.keys())
        for criteria, color in [
            (
                '=AND($B2="",$X2=FALSE)',
                "#FFD5A8",
            ),  # no errors, unchecked → orange
            ('=$B2<>""', "#FFB3B3"),  # has errors → red
            (
                '=AND($X2=TRUE,$B2="")',
                "#B8D4E8",
            ),  # resolved → blue
        ]:
            ws.conditional_format(
                1,
                0,
                n_data_rows,
                last_col,
                {
                    "type": "formula",
                    "criteria": criteria,
                    "format": workbook.add_format({"bg_color": color}),
                },
            )

    # --- Data validation
    if n_data_rows > 0:
        for ci, col_def in PERMITS_COLUMNS.items():
            if col_def["validation"] is None:
                continue
            v = col_def["validation"].copy()
            # pop show_error / error_type so they can be passed as top-level
            # kwargs to ws.data_validation
            show_error = v.pop("show_error", True)
            error_type = v.pop("error_type", "stop")
            ws.data_validation(
                1,
                ci,
                n_data_rows,
                ci,
                {"show_error": show_error, "error_type": error_type, **v},
            )

        ws.autofilter(0, 0, n_data_rows, max(PERMITS_COLUMNS.keys()))

    # Protect sheet
    ws.protect(
        "",
        {
            "sheet": True,
            "select_locked_cells": True,
            "select_unlocked_cells": True,
            "format_cells": False,
            "sort": True,
            "autofilter": True,
        },
    )

    # ------------------------------------------------------------------ #
    #  "Universe of Valid PINs" sheet                                      #
    # ------------------------------------------------------------------ #
    pin_str_fmt = {
        "font_name": "Arial",
        "locked": True,
        "align": "left",
        "text_wrap": False,
        "num_format": "@",
    }
    ws_pins = workbook.add_worksheet("Universe of Valid PINs")
    ws_pins.set_column(0, 0, 16, workbook.add_format(pin_str_fmt))
    ws_pins.write(
        0, 0, "pin", workbook.add_format({**pin_str_fmt, "bold": True})
    )
    pin_fmt_u = workbook.add_format(pin_str_fmt)
    for i, pin in enumerate(chicago_pin_universe["pin"], start=1):
        ws_pins.write(i, 0, str(pin).zfill(14), pin_fmt_u)
    ws_pins.protect("")

    workbook.close()
    print(f"Saved workbook to {file_name}")


if __name__ == "__main__":
    # Parse command line arguments
    start_date, end_date, deduplicate = parse_args()

    # Set up database connection cursor to query Athena
    conn = connect(
        s3_staging_dir=os.getenv(
            "AWS_ATHENA_S3_STAGING_DIR", "s3://ccao-athena-results-us-east-1"
        ),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    cursor = conn.cursor()

    # Query for the current assessment year, which we will use to build a
    # universe of all current PINs to use for validating permit PINs. Smartfile
    # validates PINs against the current assessment year, not the date of
    # permit issue, so we need to match that logic
    print("Querying for current assessment year")
    year = get_current_assessment_year(cursor)

    pin_cache_filename = get_pin_cache_filename(year)
    if os.path.exists(pin_cache_filename):
        print(f"Loading Chicago PIN universe data from {pin_cache_filename}")
        chicago_pin_universe = pd.read_csv(
            pin_cache_filename,
            dtype={"pin": "string", "pin10": "string"},
        )
    else:
        print(f"Pulling {year} PINs from Athena")
        chicago_pin_universe = pull_existing_pins_from_athena(cursor, year)

    print(f"Downloading permits between {start_date} and {end_date}")
    permits = download_permits(start_date, end_date)
    print(f"Cleaning {len(permits)} permit{'' if len(permits) == 1 else 's'}")

    # Chicago permit data does not include city and state, but smartfile
    # expects it, so add it manually
    permits["city_state"] = "CHICAGO, IL"

    permits_expanded = expand_multi_pin_permits(permits)

    permits_formatted = format_pin(permits_expanded)

    permits_renamed = organize_columns(permits_formatted)

    permits_shortened = shorten_applicant_names(permits_renamed)

    permits_rounded = round_amount(permits_shortened)

    permits_with_links = add_address_link_and_suggested_pins(
        permits_rounded, chicago_pin_universe
    )

    if deduplicate:
        print(
            f"Number of permits prior to deduplication: {len(permits_with_links)}"
        )
        permits_final = deduplicate_permits(
            cursor, permits_with_links, start_date, end_date
        )
        print(f"Number of permits after deduplication: {len(permits_final)}")
    else:
        permits_final = permits_with_links

    file_base_name = gen_file_base_name(start_date, end_date)

    save_xlsx_files(permits_final, file_base_name, chicago_pin_universe)
