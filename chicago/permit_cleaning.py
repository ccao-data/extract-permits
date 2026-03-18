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


def _col_letter(col_name: str) -> str:
    """Return the Excel column letter for a named column in PERMITS_COLUMNS."""
    col_idx = PERMITS_COLUMNS[col_name]["col_idx"]
    # Convert 0-based index to Excel letter(s) (A, B, ..., Z, AA, ...)
    letters = ""
    n = col_idx + 1  # Excel columns are 1-based
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _build_textjoin_errors_formula(row: int) -> str:
    """Return the TEXTJOIN formula for the Errors column at a given row
    to catch common problems that require manual resolution.
    Column letters are derived dynamically from PERMITS_COLUMNS so that
    this function stays correct if columns are reordered or added.
    """
    pin = _col_letter("pin")
    addr = _col_letter("applicant_street_address")
    permit_no = _col_letter("local_permit_no")
    issue_date = _col_letter("issue_date")
    amount = _col_letter("amount")
    applicant = _col_letter("applicant")
    work_desc = _col_letter("work_description")

    return (
        f'=_xlfn.TEXTJOIN(", ", TRUE, '
        f'IF(LEN(TRIM({pin}{row}))=0, "Missing PIN14", ""), '
        f'IF(COUNTIF(\'Universe of Valid PINs\'!A:A, {pin}{row}) > 0, "", "Provide Valid Pin"), '
        f'IF(LEN(TRIM({pin}{row}))<>14, "PIN is not 14 digits", ""), '
        f'IF(LEN({applicant}{row})>50, "Applicant Name > 50 characters", ""), '
        f'IF(LEN({addr}{row})>40, "Address > 40 characters", ""), '
        f'IF(LEN({work_desc}{row})>2000, "Work Description > 2000 characters", ""), '
        f'IF(AND(ISNUMBER({amount}{row}), {amount}{row}>2147483647), "Amount exceeds limit", ""), '
        f'IF(OR(NOT(ISNUMBER({issue_date}{row})), {issue_date}{row}=""), "Missing or Invalid Issue Date", ""), '
        f'IF(OR({amount}{row}="", NOT(ISNUMBER({amount}{row}))), "Missing Amount", ""), '
        f'IF(LEN(TRIM({applicant}{row}))=0, "Missing Applicant", ""), '
        f'IF(LEN(TRIM({addr}{row}))=0, "Missing Applicant Street Address", ""), '
        f'IF(LEN(TRIM({permit_no}{row}))=0, "Missing Permit Number", ""), '
        f'IF(LEN(TRIM({work_desc}{row}))=0, "Missing Work Description", "")'
        f")"
    )


# ---------------------------------------------------------------------------
# PERMITS_COLUMNS
#
# Key for every column on the "Permits" sheet.
# Keys are human-readable string identifiers. Each entry carries a "col_idx"
# field (0-based) that drives the actual Excel column position.
# Hidden columns (Desc 1, Desc 2 Codes 1-3, Assessable, Applicant Address 2,
# Contact Phone, Occupy Dt, Submit Dt, Est Comp Dt) have been removed.
#
# "locked" controls whether the column is protected under sheet protection.
# Locked columns: Row Number, Errors, Suggested Property Address,
#                 Local Permit No., Matched Keywords.
# All other columns are unlocked (analyst-editable).
#
# Format names in use:
#   locked_normal          — locked, no wrap
#   unlocked_normal        — unlocked, no wrap
#   unlocked_wrap          — unlocked, text_wrap=True
#   pin_unlocked_fmt       — unlocked, num_format="@" (text, preserves leading zeros)
#   date_unlocked_fmt      — unlocked, mm/dd/yyyy
#   hyperlink_fmt          — locked, blue underline
#   checkbox               — unlocked, center-aligned
# ---------------------------------------------------------------------------
PERMITS_COLUMNS = {
    # col 0  A — Row Number (locked)
    "row_number": {
        "col_idx": 0,
        "header": "Row Number",
        "src": None,
        "width": 12,
        "locked": True,
        "fmt": "locked_normal",
        "cell_type": "row_number",
    },
    # col 1  B — Errors (locked)
    "errors": {
        "col_idx": 1,
        "header": "Errors",
        "src": None,
        "width": 67,
        "locked": True,
        "fmt": "locked_normal",
        "cell_type": "formula",
    },
    # col 2  C — Suggested PINs (unlocked)
    "suggested_pins": {
        "col_idx": 2,
        "header": "Suggested PINs",
        "src": "Suggested PINs",
        "width": 50,
        "locked": False,
        "fmt": "unlocked_wrap",
        "cell_type": "suggested_pins",
        "validation": {
            "validate": "custom",
            # We want C2 to trigger any time a change is coded to warn because
            # locking makes it hard to copy values into the PIN column.
            "value": '=C2="Impossible Match"',
            "error_type": "warning",
            "show_error": True,
            "error_title": "Suggested PINs",
            "error_message": "Make sure that changes to PIN values are in PIN column.",
        },
    },
    # col 3  D — PIN (unlocked)
    "pin": {
        "col_idx": 3,
        "header": "PIN",
        "src": "PIN* [PARID]",
        "width": 25,
        "locked": False,
        "fmt": "pin_unlocked_fmt",
        "cell_type": "pin",
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
    # col 4  E — Suggested Property Address (locked)
    "suggested_property_address": {
        "col_idx": 4,
        "header": "Suggested Property Address",
        "src": "Property Address",
        "width": 25,
        "locked": True,
        "fmt": "hyperlink_fmt",
        "cell_type": "hyperlink_locked",
    },
    # col 5  F — Applicant Street Address (unlocked)
    "applicant_street_address": {
        "col_idx": 5,
        "header": "Applicant Street Address",
        "src": "Applicant Street Address* [ADDR1]",
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
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
    # col 6  G — Local Permit No. (locked)
    "local_permit_no": {
        "col_idx": 6,
        "header": "Local Permit No.",
        "src": "Local Permit No.* [USER28]",
        "width": 25,
        "locked": True,
        "fmt": "locked_normal",
        "cell_type": "normal",
    },
    # col 7  H — Issue Date (unlocked)
    "issue_date": {
        "col_idx": 7,
        "header": "Issue Date",
        "src": "Issue Date* [PERMDT]",
        "width": 25,
        "locked": False,
        "fmt": "date_unlocked_fmt",
        "cell_type": "date",
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
    # col 8  I — Amount (unlocked)
    "amount": {
        "col_idx": 8,
        "header": "Amount",
        "src": "Amount* [AMOUNT]",
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
        "validation": {
            "validate": "custom",
            "value": "=AND(ISNUMBER(I2),I2>=0,I2<=2147483647)",
            "show_error": True,
            "error_type": "stop",
            "error_title": "Invalid Amount",
            "error_message": "Amount must be a whole number between 0 and 2,147,483,647.",
        },
    },
    # col 9  J — Applicant City, State, Zip (unlocked)
    "applicant_city_state_zip": {
        "col_idx": 9,
        "header": "Applicant City, State, Zip* [ADDR3]",
        "src": "Applicant City, State, Zip* [ADDR3]",
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
    },
    # col 10  K — Applicant (unlocked)
    "applicant": {
        "col_idx": 10,
        "header": "Applicant",
        "src": "Applicant* [USER21]",
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
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
    # col 11  L — Matched Keywords (locked)
    "matched_keywords": {
        "col_idx": 11,
        "header": "Matched Keywords",
        "src": "Matched Keywords",
        "width": 25,
        "locked": True,
        "fmt": "locked_normal",
        "cell_type": "normal",
    },
    # col 12  M — Work Description (unlocked)
    "work_description": {
        "col_idx": 12,
        "header": "Work Description",
        "src": "Notes [NOTE1]",
        "width": 50,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
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
    # col 13  N — Errors are Resolved (unlocked checkbox)
    "errors_resolved": {
        "col_idx": 13,
        "header": "Errors are Resolved",
        "src": None,
        "width": 25,
        "locked": False,
        "fmt": "checkbox",
        "cell_type": "checkbox",
        "validation": {
            "validate": "custom",
            "value": '=$B2=""',
            "show_error": True,
            "error_type": "stop",
            "error_title": "Errors not resolved",
            "error_message": "This row still has errors in column B. Fix them before marking resolved.",
        },
    },
    # col 14  O — Reviewer Name (unlocked)
    "reviewer_name": {
        "col_idx": 14,
        "header": "Reviewer Name",
        "src": None,
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
    },
    # col 15  P — Reviewer Notes (unlocked)
    "reviewer_notes": {
        "col_idx": 15,
        "header": "Reviewer Notes",
        "src": None,
        "width": 25,
        "locked": False,
        "fmt": "unlocked_normal",
        "cell_type": "normal",
    },
}

# Validate that col_idx values form a contiguous sequence starting at 0
# with no duplicates. This catches mistakes like skipped or repeated indices
# when columns are added or reordered.
assert sorted(cd["col_idx"] for cd in PERMITS_COLUMNS.values()) == list(
    range(len(PERMITS_COLUMNS))
), (
    "PERMITS_COLUMNS col_idx values must be unique and form a contiguous sequence starting at 0"
)

# Derived ordered sequence of column definitions, sorted by col_idx.
# Use this for any iteration that depends on column order.
PERMITS_COLUMNS_BY_IDX = sorted(
    PERMITS_COLUMNS.values(), key=lambda cd: cd["col_idx"]
)


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
    #  "Permits" sheet                                                   #
    # ------------------------------------------------------------------ #

    n_data_rows = len(df_all)

    # --- Format registry ---
    # _base holds the properties shared by nearly every format. Individual
    # formats override or extend only the keys that differ.
    _base = {
        "font_name": "Arial",
        "locked": True,
        "align": "left",
        "text_wrap": False,
        "num_format": "0.##",
    }
    _unlocked = {**_base, "locked": False}
    _hyperlink = {"font_color": "blue", "underline": True}
    _pin_num = {"num_format": "@"}
    _date_num = {"num_format": "mm/dd/yyyy"}

    formats = {
        key: workbook.add_format(value)
        for key, value in {
            "bold": {**_base, "bold": True},
            "locked_normal": _base,
            "hyperlink_fmt": {**_base, **_hyperlink},
            "unlocked_normal": _unlocked,
            "unlocked_wrap": {**_unlocked, "text_wrap": True},
            "checkbox": {**_unlocked, "align": "center"},
            "pin_unlocked_fmt": {**_unlocked, **_pin_num},
            "date_unlocked_fmt": {**_unlocked, **_date_num},
            "hyperlink_unlocked_fmt": {**_unlocked, **_hyperlink},
        }.items()
    }

    ws = workbook.add_worksheet("Permits")
    ws.freeze_panes(1, 0)

    # --- Apply column widths and default formats ---
    for col_def in PERMITS_COLUMNS_BY_IDX:
        ci = col_def["col_idx"]
        ws.set_column(ci, ci, col_def["width"], formats[col_def["fmt"]])

    # --- Header row ---
    for col_def in PERMITS_COLUMNS_BY_IDX:
        ci = col_def["col_idx"]
        ws.write(0, ci, col_def["header"], formats["bold"])

    # --- Data rows ---
    for row_idx, (_, row_data) in enumerate(df_all.iterrows(), start=1):
        xl_row = row_idx

        for col_def in PERMITS_COLUMNS_BY_IDX:
            ci = col_def["col_idx"]
            cell_type = col_def["cell_type"]
            fmt = formats[col_def["fmt"]]

            # --- Computed / special cell types ---
            if cell_type == "row_number":
                ws.write(xl_row, ci, row_idx, fmt)
                continue

            if cell_type == "formula":
                ws.write_formula(
                    xl_row,
                    ci,
                    _build_textjoin_errors_formula(xl_row + 1),
                    fmt,
                )
                continue

            if cell_type == "checkbox":
                ws.insert_checkbox(xl_row, ci, False, formats["checkbox"])
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

            # Hyperlink cells
            if isinstance(val, str) and val.startswith("=HYPERLINK("):
                if cell_type == "suggested_pins":
                    ws.write_formula(
                        xl_row, ci, val, formats["hyperlink_unlocked_fmt"]
                    )
                else:
                    ws.write_formula(xl_row, ci, val, formats["hyperlink_fmt"])
                continue

            # Suggested PINs non-hyperlink
            if cell_type == "suggested_pins":
                ws.write(xl_row, ci, val, formats["unlocked_wrap"])
                continue

            # PIN: zero-pad to 14 digits
            if cell_type == "pin":
                val = str(val).zfill(14)
                ws.write(xl_row, ci, val, fmt)
                continue

            # Date: parse and write as Excel date serial
            if cell_type == "date":
                try:
                    parsed = pd.to_datetime(
                        str(val).strip(), dayfirst=False
                    ).to_pydatetime()
                    ws.write_datetime(xl_row, ci, parsed, fmt)
                except (ValueError, pd.errors.ParserError):
                    ws.write(xl_row, ci, val, fmt)
                continue

            # Default: plain write
            ws.write(xl_row, ci, val, fmt)

        ws.set_row(xl_row, None)  # auto height

    # Conditional formatting
    if n_data_rows > 0:
        errors_col = _col_letter("errors")
        resolved_col = _col_letter("errors_resolved")
        last_col = max(cd["col_idx"] for cd in PERMITS_COLUMNS.values())
        for criteria, color in [
            (
                f'=AND(${errors_col}2="",${resolved_col}2=FALSE)',
                "#FFD5A8",
            ),  # no errors, unchecked → orange
            (f'=${errors_col}2<>""', "#FFB3B3"),  # has errors → red
            (
                f'=AND(${resolved_col}2=TRUE,${errors_col}2="")',
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

    # --- Data validation ---
    if n_data_rows > 0:
        for col_def in PERMITS_COLUMNS_BY_IDX:
            v = col_def.get("validation")
            if v is None:
                continue
            v = v.copy()
            show_error = v.pop("show_error", True)
            error_type = v.pop("error_type", "stop")
            ci = col_def["col_idx"]
            ws.data_validation(
                1,
                ci,
                n_data_rows,
                ci,
                {"show_error": show_error, "error_type": error_type, **v},
            )

    # --- Data autofilter ---
    if n_data_rows > 0:
        last_col = max(cd["col_idx"] for cd in PERMITS_COLUMNS.values())
        ws.autofilter(0, 0, n_data_rows, last_col)

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
