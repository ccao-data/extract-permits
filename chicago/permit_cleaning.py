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

The following will also need to be updated:
    - At the beginning of each year: update year to current year in SQL_QUERY inside pull_existing_pins_from_athena() function
"""

import decimal
import math
import os
import re
import sys
from datetime import datetime

import numpy as np
import openpyxl
import pandas as pd
import requests
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

    # invalid 14-digit PIN flag (int 0/1)
    df["FLAG, INVALID: PIN* [PARID]"] = np.where(
        df["PIN* [PARID]"] == "",
        0,
        (~df["PIN* [PARID]"].isin(chicago_pin_universe["pin"])).astype(int),
    ).astype(int)

    # 10-digit validity (int 0/1)
    df["pin_10digit"] = df["PIN* [PARID]"].astype("string").str[:10]
    df["FLAG, INVALID: pin_10digit"] = np.where(
        df["pin_10digit"] == "",
        0,
        (~df["pin_10digit"].isin(chicago_pin_universe["pin10"])).astype(int),
    ).astype(int)

    # suffix after first 10
    df["pin_suffix"] = df["PIN* [PARID]"].astype("string").str[10:]

    # comments only when flag == 1
    df["FLAG COMMENTS"] += df["FLAG, INVALID: PIN* [PARID]"].apply(
        lambda v: "PIN* [PARID] is invalid, see Original PIN for raw form; "
        if v == 1
        else ""
    )
    df["FLAG COMMENTS"] += df["FLAG, INVALID: pin_10digit"].apply(
        lambda v: "10-digit PIN is invalid; " if v == 1 else ""
    )

    # length-limit flags
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
        df[flag_name] = (
            df[column]
            .apply(
                lambda val: 0
                if pd.isna(val)
                else (1 if len(str(val)) > limit else 0)
            )
            .astype(int)
        )
        df["FLAG COMMENTS"] += df[column].apply(
            lambda val: ""
            if pd.isna(val) or len(str(val)) <= limit
            else comment + str(len(str(val)) - limit) + "; "
        )

    # amount rounding + limit
    df["Amount* [AMOUNT]"] = (
        pd.to_numeric(df["Amount* [AMOUNT]"], errors="coerce")
        .round()
        .astype("Int64")
    )
    df["FLAG, VALUE: Amount"] = (
        df["Amount* [AMOUNT]"]
        .apply(lambda value: 0 if pd.isna(value) or value <= 2147483647 else 1)
        .astype(int)
    )
    df["FLAG COMMENTS"] += df["Amount* [AMOUNT]"].apply(
        lambda value: ""
        if pd.isna(value) or value <= 2147483647
        else "Amount* [AMOUNT] over value limit of 2147483647; "
    )

    # separate "empty" PIN flag (don't overwrite INVALID flags)
    df["FLAG, EMPTY: PIN* [PARID]"] = (
        df["PIN* [PARID]"]
        .apply(lambda val: 1 if pd.isna(val) or str(val).strip() == "" else 0)
        .astype(int)
    )
    df["FLAG COMMENTS"] += df["FLAG, EMPTY: PIN* [PARID]"].apply(
        lambda v: "PIN* [PARID] is missing; " if v == 1 else ""
    )

    # ---- totals: only true flag columns, kept numeric ----
    # Start with columns that begin with 'FLAG,' (exclude FLAG COMMENTS and anything else)
    flag_cols = df.filter(regex=r"^FLAG,").columns.tolist()
    pin_flag_cols = [c for c in flag_cols if "pin" in c.lower()]
    other_flag_cols = [c for c in flag_cols if "pin" not in c.lower()]

    # Coerce to numeric before summing to prevent string concat errors
    if pin_flag_cols:
        df["FLAGS, TOTAL - PIN"] = (
            df[pin_flag_cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum(axis=1)
            .astype(int)
        )
    else:
        df["FLAGS, TOTAL - PIN"] = 0

    if other_flag_cols:
        df["FLAGS, TOTAL - OTHER"] = (
            df[other_flag_cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum(axis=1)
            .astype(int)
        )
    else:
        df["FLAGS, TOTAL - OTHER"] = 0

    df["FLAGS, TOTAL - OTHER"] = df.apply(
        lambda row: 0
        if row["FLAGS, TOTAL - PIN"] > 0
        else row["FLAGS, TOTAL - OTHER"],
        axis=1,
    )

    # presentation: map 0/1 → ""/"Yes" AFTER totals are computed
    df[pin_flag_cols] = df[pin_flag_cols].replace({0: "", 1: "Yes"})
    df[other_flag_cols] = df[other_flag_cols].replace({0: "", 1: "Yes"})

    return df


# join addresses and format columns
def join_addresses_and_format_columns(df, chicago_pin_universe):
    # Collapse multiple pins per address into a single comma-separated string
    pin_map = (
        chicago_pin_universe.groupby(["prop_address_full"])["pin"]
        .apply(lambda pins: ",".join(pins.astype(str).unique()))
        .reset_index()
    )

    # Merge using the collapsed mapping
    df = df.merge(
        pin_map,
        left_on="Applicant Street Address* [ADDR1]",
        right_on="prop_address_full",
        how="left",
    )

    # Insert Property Address column right after the Applicant Street Address column
    df.insert(
        df.columns.get_loc("Applicant Street Address* [ADDR1]") + 1,
        "Property Address",
        df["Applicant Street Address* [ADDR1]"],
    )

    # Suggested PINs (replace NA with NO PIN FOUND)
    df = df.rename(columns={"pin": "Suggested PINs"})
    df["Suggested PINs"] = df["Suggested PINs"].fillna("NO PIN FOUND")

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

    def make_pin_hyperlink(pin_str):
        if pd.isna(pin_str):
            return "NO PIN FOUND"

        raw = str(pin_str).strip()
        if raw.upper() == "NO PIN FOUND" or raw == "":
            return "NO PIN FOUND"

        digits = re.sub(r"\D", "", raw)
        if len(digits) == 14:
            return f'=HYPERLINK("https://www.cookcountyassessoril.gov/pin/{digits}", "{raw}")'

        # This will be a list of comma separated pins
        return raw

    # Apply
    df["Suggested PINs"] = df["Suggested PINs"].apply(make_pin_hyperlink)

    # List of keywords to identify likely assessable permits
    keywords = [
        "remodel",
        "demolition",
        "construction",
        "solar",
        "roof",
        "foundation",
        "addition",
        "garage",
        "deck",
        "pool",
        "basement",
        "kitchen",
        "bathroom",
        "siding",
        "HVAC",
        "plumbing",
        "electrical",
    ]

    df = df.assign(
        Likely_Assessable=lambda x: x["Notes [NOTE1]"].apply(
            lambda note: (
                "Yes"
                if any(
                    kw in re.sub(r"[^a-z\s]", "", str(note).lower())
                    for kw in keywords
                )
                else "No"
            )
        )
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


def gen_file_base_name():
    today = datetime.today().date()
    today_string = today.strftime("%Y_%m_%d")
    file_name = today_string + "_permits_"
    return file_name


def save_xlsx_files(df, max_rows, file_base_name):
    # separate rows that are ready for upload from ones that need manual review or have missing or invalid PINs
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
            "Likely_Assessable",
        ]
    )

    df_other = df[df["FLAGS, TOTAL - OTHER"] > 0].reset_index()
    df_other = (
        df_other.drop(columns=df_other.filter(like="FLAG, PIN"))
        .drop(columns=df_other.filter(like="FLAG, OTHER"))
        .drop(
            columns=[
                "Original PIN",
                "FLAGS, TOTAL - OTHER",
                "index",
                "pin_10digit",
                "pin_suffix",
            ]
        )
    )

    df_review_pin_error = (
        df[df["FLAGS, TOTAL - PIN"] > 0]
        .reset_index()
        .drop(columns=["index", "pin_10digit", "pin_suffix"])
    )

    print("# rows ready for upload: ", len(df_ready))
    print(
        "# rows flagged for pin error: ",
        len(df_review_pin_error),
    )
    print("# rows flagged for other errors: ", len(df_other))

    folder_for_files_ready = (
        datetime.today().date().strftime("files_for_smartfile_%Y_%m_%d")
    )
    os.makedirs(
        folder_for_files_ready, exist_ok=True
    )  # note this will override an existing folder with same name
    folder_for_files_review = (
        datetime.today().date().strftime("files_for_review_%Y_%m_%d")
    )
    os.makedirs(
        folder_for_files_review, exist_ok=True
    )  # note this will override an existing folder with same name

    # save ready permits batched into 200 permits max per excel file
    num_files_ready = math.ceil(len(df_ready) / max_rows)
    print(
        "creating "
        + str(num_files_ready)
        + " xlsx files ready for SmartFile upload"
    )
    for i in range(num_files_ready):
        start_index = i * max_rows
        end_index = (i + 1) * max_rows
        file_dataframe = df_ready.iloc[start_index:end_index].copy()
        file_dataframe.reset_index(
            drop=True, inplace=True
        )  # each xlsx file needs an index from 1 to 200
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_dataframe = file_dataframe.reset_index()
        file_name = os.path.join(
            folder_for_files_ready, file_base_name + f"ready_{i + 1}.xlsx"
        )
        file_dataframe.to_excel(file_name, index=False, engine="xlsxwriter")

    # permits needing manual field shortening and those with missing fields will be saved as single xlsx files, not batched by 200 rows
    # Create a single Excel file with two sheets: "PIN" and "Other"

    # Define the exact column order
    COL_ORDER = [
        "# [LLINE]",
        "Original PIN",
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
        "FLAG COMMENTS",
        "FLAG, INVALID: PIN* [PARID]",
        "FLAG, INVALID: pin_10digit",
        "FLAG, LENGTH: Applicant Name",
        "FLAG, LENGTH: Permit Number",
        "FLAG, LENGTH: Applicant Street Address",
        "FLAG, LENGTH: Note1",
        "FLAG, VALUE: Amount",
        "FLAG, EMPTY: PIN",
        "FLAG, EMPTY: Issue Date",
        "FLAG, EMPTY: Amount",
        "FLAG, EMPTY: Applicant",
        "FLAG, EMPTY: Applicant Street Address",
        "FLAG, EMPTY: Permit Number",
        "FLAG, EMPTY: Note1",
        "Property Address",
        "Suggested PINs",
        "Likely_Assessable",
    ]

    file_name_combined = os.path.join(
        folder_for_files_review, file_base_name + "review.xlsx"
    )

    # Copy template workbook first
    template_file = "template/permits_template.xlsx"
    wb = openpyxl.load_workbook(template_file)
    wb.save(file_name_combined)

    with pd.ExcelWriter(
        file_name_combined,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="overlay",
    ) as writer:
        # PIN_Error sheet
        df_review_pin_error.index = df_review_pin_error.index + 1
        df_review_pin_error.index.name = "# [LLINE]"
        df_review_pin_error = df_review_pin_error.reset_index()
        df_review_pin_error = df_review_pin_error.reindex(columns=COL_ORDER)
        df_review_pin_error.to_excel(
            writer,
            sheet_name="Pin_Error",
            index=False,
            header=False,
            startrow=1,
        )

        # Other sheet
        df_other.index = df_other.index + 1
        df_other.index.name = "# [LLINE]"
        df_other = df_other.reset_index()
        df_other = df_other.reindex(columns=COL_ORDER)
        df_other.to_excel(
            writer, sheet_name="Other", index=False, header=False, startrow=1
        )


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

    pin_cache_filename = get_pin_cache_filename(start_date, end_date)
    if os.path.exists(pin_cache_filename):
        print(f"Loading Chicago PIN universe data from {pin_cache_filename}")
        chicago_pin_universe = pd.read_csv(
            pin_cache_filename,
            dtype={"pin": "string", "pin10": "string"},
        )
    else:
        print("Pulling PINs from Athena")
        chicago_pin_universe = pull_existing_pins_from_athena(
            cursor, start_date, end_date
        )

    permits = download_permits(start_date, end_date)
    print(
        f"Downloaded {len(permits)} "
        f"permit{'' if len(permits) == 1 else 's'} "
        f"between {start_date} and {end_date}"
    )

    # Chicago permit data does not include city and state, but smartfile
    # expects it, so add it manually
    permits["city_state"] = "CHICAGO, IL"

    permits_expanded = expand_multi_pin_permits(permits)

    permits_pin = format_pin(permits_expanded)

    permits_renamed = organize_columns(permits_expanded)

    permits_validated = flag_invalid_pins(
        permits_renamed, chicago_pin_universe
    )

    permits_shortened = join_addresses_and_format_columns(
        permits_validated, chicago_pin_universe
    )

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

    file_base_name = gen_file_base_name()

    save_xlsx_files(permits_deduped, 200, file_base_name)
