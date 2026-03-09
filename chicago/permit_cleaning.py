"""
Chicago Permit Ingest Process - Automation

This script automates the current process for cleaning permit data from the Chicago Data Portal's Building Permits table
and preparing it for upload to iasWorld via SmartFile. This involves fetching the data, cleaning up certain fields,
organizing columns to match the SmartFile template, and batching the data into Excel workbooks of 200 rows each. This process
also splits off data that is ready for upload from data that still needs some manual review before upload, saving each
in separate Excel workbooks in separate folders. Data that need review are split into two categories and corresponding folders/files:
those with quick fixes for fields over character or amount limits, and those with more complicated fixes for missing and/or invalid fields.

The following optional environment variables can be set:
    AWS_ATHENA_S3_STAGING_DIR: S3 path where Athena query results are stored
    AWS_REGION: Region that AWS operations should be run in

The script also expects three positional arguments:
    * start_date (str, YYYY-MM-DD): The lower bound date to use for filtering permits
    * end_date (str, YYYY-MM-DD): The upper bound date to use for filtering
    * deduplicate (bool): Whether to filter out permits that already exist in iasworld
"""

import decimal
import math
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
    """Helper function to parse and validate command line args to this
    script"""
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
    df["NOTE: 0000 added to PIN?"] = df["pin_final"].apply(
        lambda x: "Yes" if len(x) == 10 else "No"
    )
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
        "solo_pin": "Original PIN",
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
        "Original PIN",  # will keep original PIN column for rows flagged for invalid PINs
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


# flag invalid PINs for review by analysts
def flag_invalid_pins(df, chicago_pin_universe):
    df["FLAG COMMENTS"] = ""

    # invalid 14-digit PIN flag
    df["FLAG, INVALID: PIN* [PARID]"] = np.where(
        df["PIN* [PARID]"] == "",
        0,
        ~df["PIN* [PARID]"].isin(chicago_pin_universe["pin"]),
    )

    # also check if 10-digit PINs are valid to narrow down on problematic portion of invalid PINs
    df["pin_10digit"] = df["PIN* [PARID]"].astype("string").str[:10]
    df["FLAG, INVALID: pin_10digit"] = np.where(
        df["pin_10digit"] == "",
        0,
        ~df["pin_10digit"].isin(chicago_pin_universe["pin10"]),
    )

    # create variable that is the numbers following the 10-digit PIN
    # (not pulling last 4 digits from the end in case there are PINs that are not 14-digits in Chicago permit data)
    df["pin_suffix"] = df["PIN* [PARID]"].astype("string").str[10:]

    # comment for rows with invalid PINs
    df["FLAG COMMENTS"] += df.apply(
        lambda row: "First 10 digits of PIN* [PARID] do not match a valid PIN10; "
        if row["FLAG, INVALID: pin_10digit"] == 1
        else (
            "First 10 digits of PIN* [PARID] match a valid PIN10, but last 4 digits do not match; "
            if row["FLAG, INVALID: PIN* [PARID]"] == 1
            else ""
        ),
        axis=1,
    )

    return df


def flag_fix_long_fields(df):
    # will use these abbreviations to shorten applicant name field (Applicant* [USER21]) within 50 character field limit
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

    # these fields have the following character limits in Smartfile / iasWorld, flag if over limit
    long_fields_to_flag = [
        (
            "FLAG, LENGTH: Applicant Name",
            "Applicant* [USER21]",
            50,
            "Applicant* [USER21] over 50 char limit by ",
        ),
        (
            "FLAG, LENGTH: Permit Number",
            "Local Permit No.* [USER28]",
            18,
            "Local Permit No.* [USER28] over 18 char limit by ",
        ),
        (
            "FLAG, LENGTH: Applicant Street Address",
            "Applicant Street Address* [ADDR1]",
            40,
            "Applicant Street Address* [ADDR1] over 40 char limit by ",
        ),
        (
            "FLAG, LENGTH: Note1",
            "Notes [NOTE1]",
            2000,
            "Notes [NOTE1] over 2000 char limit by ",
        ),
    ]

    for flag_name, column, limit, comment in long_fields_to_flag:
        df[flag_name] = df[column].apply(
            lambda val: 0
            if pd.isna(val)
            else (1 if len(str(val)) > limit else 0)
        )
        df["FLAG COMMENTS"] += df[column].apply(
            lambda val: ""
            if pd.isna(val)
            else (
                ""
                if len(str(val)) <= limit
                else comment + str(len(str(val)) - limit) + "; "
            )
        )

    # round Amount to closest dollar because smart file doesn't accept decimal amounts, then flag values above upper limit
    df["Amount* [AMOUNT]"] = (
        pd.to_numeric(df["Amount* [AMOUNT]"], errors="coerce")
        .round()
        .astype("Int64")
    )
    df["FLAG, VALUE: Amount"] = df["Amount* [AMOUNT]"].apply(
        lambda value: 0 if pd.isna(value) or value <= 2147483647 else 1
    )
    df["FLAG COMMENTS"] += df["Amount* [AMOUNT]"].apply(
        lambda value: ""
        if pd.isna(value) or value <= 2147483647
        else "Amount* [AMOUNT] over value limit of 2147483647; "
    )

    # also flag rows where fields are blank for manual review (for fields we're populating in smartfile template)
    empty_fields_to_flag = [
        ("FLAG, EMPTY: PIN", "PIN* [PARID]"),
        ("FLAG, EMPTY: Issue Date", "Issue Date* [PERMDT]"),
        ("FLAG, EMPTY: Amount", "Amount* [AMOUNT]"),
        ("FLAG, EMPTY: Applicant", "Applicant* [USER21]"),
        (
            "FLAG, EMPTY: Applicant Street Address",
            "Applicant Street Address* [ADDR1]",
        ),
        ("FLAG, EMPTY: Permit Number", "Local Permit No.* [USER28]"),
        ("FLAG, EMPTY: Note1", "Notes [NOTE1]"),
    ]

    for flag_name, column in empty_fields_to_flag:
        comment = column + " is missing; "
        df[flag_name] = df[column].apply(
            lambda val: 1 if pd.isna(val) or str(val).strip() == "" else 0
        )
        df["FLAG COMMENTS"] += df[flag_name].apply(
            lambda val: "" if val == 0 else comment
        )

    # Create flags based on the pin errors and then those based on other errors.
    # These are sorted into separate pages
    flag_cols = df.filter(regex="^FLAG,").columns.tolist()
    pin_flag_cols = [c for c in flag_cols if "pin" in c.lower()]
    other_flag_cols = [c for c in flag_cols if "pin" not in c.lower()]

    # Sum pin values to create the pin flags vs other flags
    if pin_flag_cols:
        df["FLAGS, TOTAL - PIN"] = df[pin_flag_cols].sum(axis=1)
    else:
        df["FLAGS, TOTAL - PIN"] = 0

    if other_flag_cols:
        df["FLAGS, TOTAL - OTHER"] = df[other_flag_cols].sum(axis=1)
    else:
        df["FLAGS, TOTAL - OTHER"] = 0

    # for ease of analysts viewing, edits flag columns to read "Yes" when row is flagged and blank otherwise (easier than columns of 0s and 1s)
    df[pin_flag_cols] = df[pin_flag_cols].replace({0: "", 1: "Yes"})
    df[other_flag_cols] = df[other_flag_cols].replace({0: "", 1: "Yes"})

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


# join addresses and format columns
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

    # Apply
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

    new_permits["permdt"] = new_permits["permdt"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y").strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
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
    r = row
    return (
        f'=_xlfn.TEXTJOIN(", ", TRUE, '
        f'IF(LEN(TRIM(D{r}))=0, "Missing PIN14", ""), '
        f'IF(COUNTIF(\'Universe of Valid PINs\'!A:A, D{r}) > 0, "", "Provide Valid Pin"), '
        f'IF(LEN(TRIM(D{r}))<>14, "PIN is not 14 digits", ""), '
        f'IF(LEN(R{r})>50, "Applicant Name > 50 characters", ""), '
        f'IF(LEN(F{r})>40, "Address > 40 characters", ""), '
        f'IF(LEN(S{r})>2000, "Work Description > 2000 characters", ""), '
        f'IF(AND(ISNUMBER(M{r}), M{r}>2147483647), "Amount exceeds limit", ""), '
        f'IF(OR(H{r}="", NOT(ISNUMBER(DATEVALUE(H{r})))), "Missing or Invalid Issue Date", ""), '
        f'IF(OR(M{r}="", NOT(ISNUMBER(M{r}))), "Missing Amount", ""), '
        f'IF(LEN(TRIM(R{r}))=0, "Missing Applicant", ""), '
        f'IF(LEN(TRIM(F{r}))=0, "Missing Applicant Street Address", ""), '
        f'IF(LEN(TRIM(G{r}))=0, "Missing Permit Number", ""), '
        f'IF(LEN(TRIM(S{r}))=0, "Missing Work Description", "")'
        f")"
    )


# Column layout for the "Needs Review" sheet, matching the demo workbook exactly
REVIEW_HEADERS = [
    "Row Number",
    "Errors",  # TEXTJOIN formula
    "Suggested PINs",
    "PIN",
    "Suggested Property Address",  # hyperlink to Cook County viewer
    "Applicant Street Address",
    "Local Permit No.",
    "Issue Date",
    "Desc 1* [DESC1]",
    "Desc 2 Code 1 [USER6]",
    "Desc 2 Code 2 [USER7]",
    "Desc 2 Code 3 [USER8]",
    "Amount",
    "Assessable [IS_ASSESS]",
    "Applicant Address 2 [ADDR2]",
    "Applicant City, State, Zip* [ADDR3]",
    "Contact Phone* [PHONE]",
    "Applicant",
    "Work Description",  # Notes [NOTE1] (renamed for clarity)
    "Occupy Dt [UDATE1]",
    "Submit Dt* [CERTDATE]",
    "Est Comp Dt [UDATE2]",
    "Matched Keywords",
    "Errors are Resolved",
    "Reviewer Name",  # empty, filled by reviewer
    "Reviewer Notes",  # empty, filled by reviewer
]

REVIEW_HIDDEN_COLS = {9, 10, 11, 12, 14, 15, 17, 20, 21, 22}


def save_xlsx_files(df, max_rows, file_base_name, chicago_pin_universe):
    # Separate rows ready for upload from those needing review
    df_ready = df[
        (df["FLAGS, TOTAL - PIN"] == 0) & (df["FLAGS, TOTAL - OTHER"] == 0)
    ].reset_index()
    df_ready = df_ready.drop(
        columns=df_ready.filter(like="FLAG").columns
    ).drop(
        columns=[
            "index",
            "Original PIN",
            "pin_10digit",
            "pin_suffix",
            "Property Address",
            "Suggested PINs",
            "Matched Keywords",
        ]
    )

    # PIN errors: any row with a PIN flag (may also have other errors)
    df_pin_errors = df[df["FLAGS, TOTAL - PIN"] > 0].reset_index(drop=True)

    # Other errors: rows with non-PIN errors only
    df_other_errors = df[
        (df["FLAGS, TOTAL - PIN"] == 0) & (df["FLAGS, TOTAL - OTHER"] > 0)
    ].reset_index(drop=True)

    print("# rows ready for upload: ", len(df_ready))
    print("# rows with PIN errors: ", len(df_pin_errors))
    print("# rows with other errors: ", len(df_other_errors))

    folder_for_files_ready = (
        datetime.today().date().strftime("files_for_smartfile_%Y_%m_%d")
    )
    os.makedirs(folder_for_files_ready, exist_ok=True)
    folder_for_files_review = (
        datetime.today().date().strftime("files_for_review_%Y_%m_%d")
    )
    os.makedirs(folder_for_files_review, exist_ok=True)

    # Save ready permits batched into max_rows per file
    num_files_ready = math.ceil(len(df_ready) / max_rows)
    print(f"Creating {num_files_ready} xlsx files ready for SmartFile upload")
    for i in range(num_files_ready):
        file_dataframe = df_ready.iloc[
            i * max_rows : (i + 1) * max_rows
        ].copy()
        file_dataframe.reset_index(drop=True, inplace=True)
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_dataframe = file_dataframe.reset_index()
        file_name = os.path.join(
            folder_for_files_ready,
            file_base_name + f"ready_for_upload_{i + 1}.xlsx",
        )
        file_dataframe.to_excel(file_name, index=False, engine="xlsxwriter")

    def _write_review_sheet(df_review, workbook, sheet_name):
        """Write one Needs Review sheet into an existing xlsxwriter workbook.
        Always writes the sheet (with headers) even if df_review is empty or None."""
        n_data_rows = len(df_review) if df_review is not None else 0

        L = {
            "font_name": "Arial",
            "locked": True,
            "align": "left",
            "text_wrap": False,
            "num_format": "0.##",
        }
        U = {
            "font_name": "Arial",
            "locked": False,
            "align": "left",
            "text_wrap": False,
            "num_format": "0.##",
        }

        def fmt(d):
            return workbook.add_format(d)

        bold = fmt({**L, "bold": True})
        normal = fmt(L)
        wrap = fmt(L)
        hidden_col = fmt(L)
        hyperlink_fmt = fmt({**L, "font_color": "blue", "underline": True})
        unlocked_normal = fmt(U)
        unlocked_wrap = fmt(U)
        unlocked_wrap_col = fmt({**U, "text_wrap": True})
        checkbox_unlocked = fmt({**U, "align": "center"})
        pin_fmt = fmt({**L, "num_format": "@"})
        pin_unlocked_fmt = fmt({**U, "num_format": "@"})
        hyperlink_unlocked_fmt = fmt(
            {**U, "font_color": "blue", "underline": True}
        )

        # --- Sheet 1: Needs Review ---
        ws_review = workbook.add_worksheet(sheet_name)
        ws_review.freeze_panes(1, 0)

        # Setting an explicit alignment on every column prevents text overflow
        # into adjacent empty cells.
        # Editable columns use unlocked_normal as column default so cell-level
        # locked/unlocked formats aren't overridden by the column default.
        for _ci in range(len(REVIEW_HEADERS)):
            if _ci == 3:
                col_fmt = pin_unlocked_fmt  # PIN col: text format, unlocked when errors
            elif _ci in {2, 5, 6, 7, 12, 17, 18, 23, 24, 25}:
                col_fmt = (
                    unlocked_normal  # col 2 = Suggested PINs, always unlocked
                )
            else:
                col_fmt = normal
            ws_review.set_column(_ci, _ci, 25, col_fmt)
        ws_review.set_column(1, 1, 67, normal)  # Errors column wider
        ws_review.set_column(
            2, 2, 50, unlocked_wrap_col
        )  # Suggested PINs wraps to prevent overflow
        ws_review.set_column(
            18, 18, 50, unlocked_normal
        )  # Work Description wider for better readability

        # Write header row
        for col_idx, header in enumerate(REVIEW_HEADERS):
            ws_review.write(0, col_idx, header, bold)

        # col_map: source dataframe column
        col_map = {
            "PIN* [PARID]": 3,
            "Suggested PINs": 2,
            "Property Address": 4,
            "Applicant Street Address* [ADDR1]": 5,
            "Local Permit No.* [USER28]": 6,
            "Issue Date* [PERMDT]": 7,
            "Desc 1* [DESC1]": 8,
            "Desc 2 Code 1 [USER6]": 9,
            "Desc 2 Code 2 [USER7]": 10,
            "Desc 2 Code 3 [USER8]": 11,
            "Amount* [AMOUNT]": 12,
            "Assessable [IS_ASSESS]": 13,
            "Applicant Address 2 [ADDR2]": 14,
            "Applicant City, State, Zip* [ADDR3]": 15,
            "Contact Phone* [PHONE]": 16,
            "Applicant* [USER21]": 17,
            "Notes [NOTE1]": 18,  # displayed as "Work Description"
            "Occupy Dt [UDATE1]": 19,
            "Submit Dt* [CERTDATE]": 20,
            "Est Comp Dt [UDATE2]": 21,
            "Matched Keywords": 22,
            # col 23 = "Errors are Resolved" written below
        }

        for row_idx, (_, row_data) in enumerate(
            df_review.iterrows() if df_review is not None else [], start=1
        ):
            xl_row = row_idx

            # pass/fail per editable column — determines locked vs unlocked
            pin_val = str(row_data.get("PIN* [PARID]", "") or "").strip()
            applicant_val = str(
                row_data.get("Applicant* [USER21]", "") or ""
            ).strip()
            address_val = str(
                row_data.get("Applicant Street Address* [ADDR1]", "") or ""
            ).strip()
            notes_val = str(row_data.get("Notes [NOTE1]", "") or "").strip()
            amount_val = row_data.get("Amount* [AMOUNT]", None)
            issue_date_val = str(
                row_data.get("Issue Date* [PERMDT]", "") or ""
            ).strip()
            permit_val = str(
                row_data.get("Local Permit No.* [USER28]", "") or ""
            ).strip()
            pin_flag = row_data.get("FLAG, INVALID: PIN* [PARID]", 0)
            pin_empty_flag = row_data.get("FLAG, EMPTY: PIN", 0)
            try:
                amount_num = (
                    float(amount_val)
                    if amount_val is not None and str(amount_val).strip() != ""
                    else None
                )
                amount_error = amount_num is None or amount_num > 2147483647
            except (ValueError, TypeError):
                amount_error = True

            # True = cell fails check = unlocked for editing
            error_cols = {
                3: bool(pin_flag)
                or bool(pin_empty_flag)
                or len(pin_val) != 14,
                5: len(address_val) == 0 or len(address_val) > 40,
                6: len(permit_val) == 0,
                7: len(issue_date_val) == 0,
                12: amount_error,
                17: len(applicant_val) == 0 or len(applicant_val) > 50,
                18: len(notes_val) == 0 or len(notes_val) > 2000,
            }

            ws_review.write(xl_row, 0, row_idx, normal)
            ws_review.write_formula(
                xl_row, 1, _build_textjoin_errors_formula(xl_row + 1), normal
            )
            for src_col, dest_col in col_map.items():
                val = row_data.get(src_col)
                if not isinstance(val, str) and pd.isna(val):
                    val = None
                fmt = (
                    unlocked_wrap if error_cols.get(dest_col, False) else wrap
                )
                if val is None:
                    pass
                elif isinstance(val, str) and val.startswith("=HYPERLINK("):
                    if dest_col == 2:
                        # Single PIN — restore as clickable hyperlink with unlocked format
                        ws_review.write_formula(
                            xl_row, dest_col, val, hyperlink_unlocked_fmt
                        )
                    else:
                        ws_review.write_formula(
                            xl_row, dest_col, val, hyperlink_fmt
                        )
                else:
                    # Zero-pad PIN to 14 digits at write time, use text format
                    if dest_col == 3 and val and not str(val).startswith("="):
                        val = str(val).zfill(14)
                        fmt = (
                            pin_unlocked_fmt
                            if error_cols.get(dest_col, False)
                            else pin_fmt
                        )
                    # Suggested PINs always unlocked
                    if dest_col == 2:
                        fmt = unlocked_wrap_col
                    ws_review.write(xl_row, dest_col, val, fmt)

            # checkbox — always unlocked
            ws_review.insert_checkbox(xl_row, 23, False, checkbox_unlocked)
            # Reviewer Name and Reviewer Notes — always unlocked, empty

            ws_review.set_row(
                xl_row, None
            )  # auto height to accommodate wrapped Suggested PINs

        # Row conditional formatting: orange=no errors/unchecked, red=has errors, blue=resolved
        if n_data_rows > 0:
            last_col = len(REVIEW_HEADERS) - 1
            for criteria, color in [
                ('=AND($B2="",$X2=FALSE)', "#FFD5A8"),
                ('=$B2<>""', "#FFB3B3"),
                ('=AND($X2=TRUE,$B2="")', "#B8D4E8"),
            ]:
                ws_review.conditional_format(
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

        # Data validations — input constraints on editable columns
        if n_data_rows > 0:

            def dv(col, opts):
                ws_review.data_validation(
                    1,
                    col,
                    n_data_rows,
                    col,
                    {"show_error": True, "error_type": "stop", **opts},
                )

            dv(
                3,
                {
                    "validate": "custom",
                    "value": "=AND(LEN(TRIM(D2))=14,COUNTIF('Universe of Valid PINs'!$A:$A,D2)>0)",
                    "ignore_blank": False,
                    "error_title": "Invalid PIN",
                    "error_message": "PIN must be 14 digits and exist in the Universe of Valid PINs.",
                },
            )
            dv(
                5,
                {
                    "validate": "text length",
                    "criteria": "between",
                    "minimum": 1,
                    "maximum": 40,
                    "error_title": "Invalid Address",
                    "error_message": "Address must be between 1 and 40 characters.",
                },
            )
            dv(
                6,
                {
                    "validate": "text length",
                    "criteria": "greater than or equal to",
                    "value": 1,
                    "error_title": "Invalid Permit No.",
                    "error_message": "Permit No. must not be empty.",
                },
            )
            dv(
                7,
                {
                    "validate": "date",
                    "criteria": "greater than or equal to",
                    "value": "1900-01-01",
                    "error_title": "Invalid Date",
                    "error_message": "Issue Date must be a valid date.",
                },
            )
            dv(
                12,
                {
                    "validate": "integer",
                    "criteria": "between",
                    "minimum": 1,
                    "maximum": 2147483647,
                    "error_title": "Invalid Amount",
                    "error_message": "Amount must be a whole number between 1 and 2,147,483,647.",
                },
            )
            dv(
                17,
                {
                    "validate": "text length",
                    "criteria": "between",
                    "minimum": 1,
                    "maximum": 50,
                    "error_title": "Invalid Applicant",
                    "error_message": "Applicant must be between 1 and 50 characters.",
                },
            )
            dv(
                18,
                {
                    "validate": "text length",
                    "criteria": "between",
                    "minimum": 1,
                    "maximum": 2000,
                    "error_title": "Invalid Work Description",
                    "error_message": "Work Description must be between 1 and 2000 characters.",
                },
            )
            dv(
                23,
                {
                    "validate": "custom",
                    "value": '=$B2=""',
                    "error_title": "Errors not resolved",
                    "error_message": "This row still has errors in column B. Fix them before marking resolved.",
                },
            )
            ws_review.data_validation(
                1,
                2,
                n_data_rows,
                2,
                {
                    "validate": "custom",
                    "value": '=C2="343343434343"',
                    "error_type": "warning",
                    "error_title": "Suggested PINs",
                    "error_message": "Make sure that changes to PIN values are in PIN column.",
                    "show_error": True,
                },
            )
            ws_review.autofilter(0, 0, n_data_rows, len(REVIEW_HEADERS) - 1)

        # Hide specific columns
        for col_idx in REVIEW_HIDDEN_COLS:
            ws_review.set_column(
                col_idx - 1, col_idx - 1, 25, hidden_col, {"hidden": True}
            )

        # Protect sheet
        ws_review.protect(
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

    # Build single combined review workbook with two sheets
    file_name_review = os.path.join(
        folder_for_files_review, file_base_name + "needing_review.xlsx"
    )
    workbook = xlsxwriter.Workbook(file_name_review)

    _write_review_sheet(df_pin_errors, workbook, "PIN Errors")
    _write_review_sheet(df_other_errors, workbook, "Other Errors")

    # Universe of Valid PINs sheet — shared across both review sheets
    _base_u = {
        "font_name": "Arial",
        "locked": True,
        "align": "left",
        "text_wrap": False,
        "num_format": "0",
    }
    bold_u = workbook.add_format({**_base_u, "bold": True})
    pin_fmt_u = workbook.add_format({**_base_u, "num_format": "@"})
    ws_pins = workbook.add_worksheet("Universe of Valid PINs")
    ws_pins.set_column(0, 0, 16, pin_fmt_u)
    ws_pins.write(0, 0, "pin", bold_u)
    for i, pin in enumerate(chicago_pin_universe["pin"], start=1):
        ws_pins.write(i, 0, str(pin).zfill(14), pin_fmt_u)
    ws_pins.protect("")

    workbook.close()
    print(f"Saved review workbook to {file_name_review}")


if __name__ == "__main__":
    # Parse command line arguments
    start_date, end_date, deduplicate = parse_args()

    # Set up database connection cursor to query Athena
    conn = connect(
        s3_staging_dir=os.getenv(
            "AWS_ATHENA_S3_STAGING_DIR",
            "s3://ccao-athena-results-us-east-1",
        ),
        region_name=os.getenv(
            "AWS_REGION",
            "us-east-1",
        ),
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

    permits_pin = format_pin(permits_expanded)

    permits_renamed = organize_columns(permits_expanded)

    permits_validated = flag_invalid_pins(
        permits_renamed, chicago_pin_universe
    )

    joined_permits = add_address_link_and_suggested_pins(
        permits_validated, chicago_pin_universe
    )

    permits_shortened = flag_fix_long_fields(joined_permits)

    if deduplicate:
        print(
            "Number of permits prior to deduplication: "
            f"{len(permits_shortened)}"
        )
        permits_deduped = deduplicate_permits(
            cursor, permits_shortened, start_date, end_date
        )
        print(f"Number of permits after deduplication: {len(permits_deduped)}")
    else:
        permits_deduped = permits_shortened

    file_base_name = gen_file_base_name(start_date, end_date)

    save_xlsx_files(permits_deduped, 200, file_base_name, chicago_pin_universe)
