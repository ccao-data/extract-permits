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

from datetime import datetime
import decimal
import math
import os
import sys

import numpy as np
import pandas as pd
from pyathena import connect
from pyathena.pandas.util import as_pandas
import requests


# DEFINE FUNCTIONS

# Connect to Athena and download existing 14-digit PINs in Chicago
def pull_existing_pins_from_athena(cursor):
    print("Pulling PINs from Athena")
    SQL_QUERY = """
        SELECT
            CAST(pin AS varchar) AS pin,
            CAST(pin10 AS varchar) AS pin10
        FROM default.vw_pin_universe
        WHERE triad_name = 'City' AND year = '2023'
    """
    cursor.execute(SQL_QUERY)
    chicago_pin_universe = as_pandas(cursor)
    chicago_pin_universe.to_csv("chicago_pin_universe.csv", index=False)

    return chicago_pin_universe


def download_permits(start_date, end_date):
    params = {
        "$where": f"issue_date between '{start_date}' and '{end_date}'",
        "$order": "issue_date DESC",
        "$limit": 1000000  # Artificial limit to override the default
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

    df['pin_list'] = df['pin_list'].apply(parse_pin_list)

    # Retain rows where pin_list is NA, since the pivot operation will
    # remove them but we want to keep them
    na_rows = df[df["pin_list"].isna()]
    df = df.dropna(subset=["pin_list"])

    # Pivot the dataframe longer by "pin_list" so that we have one row
    # per PIN with "solo_pin" being the PIN column
    df = df.explode("pin_list").reset_index(drop=True).rename(columns={
        "pin_list": "solo_pin"
    })

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
    df["NOTE: 0000 added to PIN?"] = df["pin_final"].apply(lambda x: "Yes" if len(x) == 10 else "No")
    return df


# Eliminate columns not included in permit upload and rename and order to match Smartfile excel format
def organize_columns(df):

    address_columns = ["street_number", "street_direction", "street_name"]
    df[address_columns] = df[address_columns].fillna("")
    df["Address"] = df[address_columns].astype("string").agg(" ".join, axis=1)

    df["issue_date"] = pd.to_datetime(df["issue_date"], format="%Y-%m-%dT%H:%M:%S.%f", errors='coerce').dt.strftime("%-m/%-d/%Y")

    column_renaming_dict = {
        "solo_pin": "Original PIN",
        "pin_final": "PIN* [PARID]",
        "permit_": "Local Permit No.* [USER28]",
        "issue_date": "Issue Date* [PERMDT]",
        "reported_cost": "Amount* [AMOUNT]",
        "Address": "Applicant Street Address* [ADDR1]",
        "city_state": "Applicant City, State, Zip* [ADDR3]",
        "contact_1_name": "Applicant* [USER21]",
        "work_description": "Notes [NOTE1]"
        }

    data_relevant = df[[col for col in df.columns if col in column_renaming_dict]]
    data_renamed = data_relevant.rename(columns=column_renaming_dict)

    column_order = ["Original PIN", # will keep original PIN column for rows flagged for invalid PINs
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
                    "Est Comp Dt [UDATE2]"
                    ]

    data_all_cols = data_renamed.assign(**{col: None for col in column_order if col not in data_renamed})
    data_ordered = data_all_cols[column_order]

    return data_ordered


# flag invalid PINs for review by analysts
def flag_invalid_pins(df, valid_pins):
    df["FLAG COMMENTS"] = ""

    # invalid 14-digit PIN flag
    df["FLAG, INVALID: PIN* [PARID]"] = np.where(df["PIN* [PARID]"] == "", 0, ~df["PIN* [PARID]"].isin(valid_pins["pin"]))

    # also check if 10-digit PINs are valid to narrow down on problematic portion of invalid PINs
    df["pin_10digit"] = df["PIN* [PARID]"].astype("string").str[:10]
    df["FLAG, INVALID: pin_10digit"] = np.where(df["pin_10digit"] == "", 0, ~df["pin_10digit"].isin(valid_pins["pin10"]))

    # create variable that is the numbers following the 10-digit PIN
    # (not pulling last 4 digits from the end in case there are PINs that are not 14-digits in Chicago permit data)
    df["pin_suffix"] = df["PIN* [PARID]"].astype("string").str[10:]

    # comment for rows with invalid pin
    df["FLAG COMMENTS"] += df["FLAG, INVALID: PIN* [PARID]"].apply(lambda val: "" if val == 0 else "PIN* [PARID] is invalid, see Original PIN for raw form; ")
    df["FLAG COMMENTS"] += df["FLAG, INVALID: pin_10digit"].apply(lambda val: "" if val == 0 else "10-digit PIN is invalid; ")

    return df



def flag_fix_long_fields(df):
    # will use these abbreviations to shorten applicant name field (Applicant* [USER21]) within 50 character field limit
    name_shortening_dict = {
        "ASSOCIATION":   "ASSN",
        "COMPANY":      "CO",
        "BUILDING":     "BLDG",
        "FOUNDATION":   "FNDN",
        "ILLINOIS":     "IL",
        "STREET":       "ST",
        "BOULEVARD":    "BLVD",
        "AVENUE":       "AVE",
        "APARTMENT":    "APT",
        "APARTMENTS":   "APTS",
        "MANAGEMENT":   "MGMT",
        "CORPORATION":  "CORP",
        "INCORPORATED": "INC",
        "LIMITED":      "LTD",
        "PLAZA":        "PLZ"
}

    df["Applicant* [USER21]"] = df["Applicant* [USER21]"].replace(name_shortening_dict, regex=True)

    # these fields have the following character limits in Smartfile / iasWorld, flag if over limit
    long_fields_to_flag = [
        ("FLAG, LENGTH: Applicant Name", "Applicant* [USER21]", 50, "Applicant* [USER21] over 50 char limit by "),
        ("FLAG, LENGTH: Permit Number", "Local Permit No.* [USER28]", 18, "Local Permit No.* [USER28] over 18 char limit by "),
        ("FLAG, LENGTH: Applicant Street Address", "Applicant Street Address* [ADDR1]", 40, "Applicant Street Address* [ADDR1] over 40 char limit by "),
        ("FLAG, LENGTH: Note1", "Notes [NOTE1]", 2000, "Notes [NOTE1] over 2000 char limit by ")
    ]

    for flag_name, column, limit, comment in long_fields_to_flag:
        df[flag_name] = df[column].apply(lambda val: 0 if pd.isna(val) else (1 if len(str(val)) > limit else 0))
        df["FLAG COMMENTS"] += df[column].apply(lambda val: "" if pd.isna(val) else ("" if len(str(val)) <= limit else comment + str(len(str(val)) - limit) + "; "))

    # round Amount to closest dollar because smart file doesn't accept decimal amounts, then flag values above upper limit
    df["Amount* [AMOUNT]"] = pd.to_numeric(df["Amount* [AMOUNT]"], errors="coerce").round().astype("Int64")
    df["FLAG, VALUE: Amount"] = df["Amount* [AMOUNT]"].apply(lambda value: 0 if pd.isna(value) or value <= 2147483647 else 1)
    df["FLAG COMMENTS"] += df["Amount* [AMOUNT]"].apply(lambda value: "" if pd.isna(value) or value <= 2147483647 else "Amount* [AMOUNT] over value limit of 2147483647; ")

    # also flag rows where fields are blank for manual review (for fields we're populating in smartfile template)
    empty_fields_to_flag = [
        ("FLAG, EMPTY: PIN", "PIN* [PARID]"),
        ("FLAG, EMPTY: Issue Date", "Issue Date* [PERMDT]"),
        ("FLAG, EMPTY: Amount", "Amount* [AMOUNT]"),
        ("FLAG, EMPTY: Applicant", "Applicant* [USER21]"),
        ("FLAG, EMPTY: Applicant Street Address", "Applicant Street Address* [ADDR1]"),
        ("FLAG, EMPTY: Permit Number", "Local Permit No.* [USER28]"),
        ("FLAG, EMPTY: Note1", "Notes [NOTE1]")
        ]

    for flag_name, column in empty_fields_to_flag:
        comment = column + " is missing; "
        df[flag_name] = df[column].apply(lambda val: 1 if pd.isna(val) or str(val).strip() == "" else 0)
        df["FLAG COMMENTS"] += df[flag_name].apply(lambda val: "" if val == 0 else comment)

    # create columns for total number of flags for length and for missingness since they'll get sorted into separate excel files
    df["FLAGS, TOTAL - LENGTH/VALUE"] = df.filter(like="FLAG, LENGTH").values.sum(axis=1) + df.filter(like="FLAG, VALUE").values.sum(axis=1)
    df["FLAGS, TOTAL - EMPTY/INVALID"] = df.filter(like="FLAG, EMPTY").values.sum(axis=1) + df.filter(like="FLAG, INVALID").values.sum(axis=1)

    # need a column that identifies rows with flags for field length/amount but no flags for emptiness/invalidness
    # since these two categories will get split into separate excel workbooks
    df["MANUAL REVIEW"] = np.where((df["FLAGS, TOTAL - EMPTY/INVALID"] == 0) & (df["FLAGS, TOTAL - LENGTH/VALUE"] > 0), 1, 0)

    # for ease of analysts viewing, edits flag columns to read "Yes" when row is flagged and blank otherwise (easier than columns of 0s and 1s)
    flag_columns = list(df.filter(like="FLAG, LENGTH").columns) + list(df.filter(like="FLAG, VALUE").columns) + list(df.filter(like="FLAG, EMPTY").columns) + list(df.filter(like="FLAG, INVALID").columns)
    df[flag_columns] = df[flag_columns].replace({0: "", 1: "Yes"})

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
        "Applicant* [USER21]": "user21",
        "Local Permit No.* [USER28]": "user28",
        "Notes [NOTE1]": "user43",
    }
    new_permits = df.copy()
    for workbook_key, iasworld_key in workbook_to_iasworld_col_map.items():
        new_permits[iasworld_key] = new_permits[workbook_key]

    # Transform new columns to ensure they match the iasworld formatting
    new_permits["amount"] = new_permits["amount"].apply(
        lambda x: decimal.Decimal("{:.2f}".format(x))
    )
    new_permits["permdt"] = new_permits["permdt"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y").strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-5]
    )
    new_permits["note2"] = new_permits["note2"] + ",,CHICAGO, IL"
    new_permits["user43"] = new_permits["user43"].str.replace(
        "(", ""
    ).replace(")", "")
    new_permits["user43"] = new_permits["user43"].str.slice(0, 261)

    # Antijoin new_permits to existing_permits to find permits that do
    # not exist in iasworld
    merged_permits = pd.merge(
        new_permits,
        existing_permits,
        how="left",
        on=list(workbook_to_iasworld_col_map.values()),
        indicator=True
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
    df_ready = df[(df["FLAGS, TOTAL - LENGTH/VALUE"] == 0) & (df["FLAGS, TOTAL - EMPTY/INVALID"] == 0)].reset_index()
    df_ready = df_ready.drop(columns=df_ready.filter(like="FLAG").columns).\
        drop(columns=["index", "Original PIN", "MANUAL REVIEW", "pin_10digit", "pin_suffix"])

    df_review_length = df[df["MANUAL REVIEW"] == 1].reset_index()
    df_review_length = df_review_length.drop(columns=df_review_length.filter(like="FLAG, EMPTY")).\
        drop(columns=df_review_length.filter(like="FLAG, INVALID")).\
        drop(columns=["Original PIN", "FLAGS, TOTAL - EMPTY/INVALID", "index", "MANUAL REVIEW", "pin_10digit", "pin_suffix"])

    df_review_empty_invalid = df[df["FLAGS, TOTAL - EMPTY/INVALID"] > 0].reset_index().\
        drop(columns=["index", "MANUAL REVIEW", "pin_10digit", "pin_suffix"])

    print("# rows ready for upload: ", len(df_ready))
    print("# rows flagged for length: ", len(df_review_length))
    print("# rows flagged for empty/invalid fields: ", len(df_review_empty_invalid))

    # create new folders with today's date to save xlsx files in (1 each for ready, needing manual shortening of fields, have missing fields or invalid PIN)
    folder_for_files_ready = datetime.today().date().strftime("files_for_smartfile_%Y_%m_%d")
    os.makedirs(folder_for_files_ready, exist_ok=True) # note this will override an existing folder with same name
    folder_for_files_review_length = datetime.today().date().strftime("files_for_review_length_%Y_%m_%d")
    os.makedirs(folder_for_files_review_length, exist_ok=True) # note this will override an existing folder with same name
    folder_for_files_review_empty_invalid = datetime.today().date().strftime("files_for_review_empty_invalid_%Y_%m_%d")
    os.makedirs(folder_for_files_review_empty_invalid, exist_ok=True) # note this will override an existing folder with same name

    # save ready permits batched into 200 permits max per excel file
    num_files_ready = math.ceil(len(df_ready) / max_rows)
    print("creating " + str(num_files_ready) + " xlsx files ready for SmartFile upload")
    for i in range(num_files_ready):
        start_index = i * max_rows
        end_index = (i + 1) * max_rows
        file_dataframe = df_ready.iloc[start_index:end_index].copy()
        file_dataframe.reset_index(drop=True, inplace=True) # each xlsx file needs an index from 1 to 200
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_dataframe = file_dataframe.reset_index()
        file_name = os.path.join(folder_for_files_ready, file_base_name + f"ready_{i+1}.xlsx")
        file_dataframe.to_excel(file_name, index=False, engine="xlsxwriter")

    # permits needing manual field shortening and those with missing fields will be saved as single xlsx files, not batched by 200 rows
    df_review_length.index = df_review_length.index + 1
    df_review_length.index.name = "# [LLINE]"
    df_review_length = df_review_length.reset_index()
    file_name_review_length = os.path.join(folder_for_files_review_length, file_base_name + "review_length.xlsx")
    df_review_length.to_excel(file_name_review_length, index=False, engine="xlsxwriter")

    df_review_empty_invalid.index = df_review_empty_invalid.index +1
    df_review_empty_invalid.index.name = "# [LLINE]"
    df_review_empty_invalid = df_review_empty_invalid.reset_index()
    file_name_review_empty_invalid = os.path.join(folder_for_files_review_empty_invalid, file_base_name + "review_empty_invalid.xlsx")
    df_review_empty_invalid.to_excel(file_name_review_empty_invalid, index=False, engine="xlsxwriter")


if __name__ == "__main__":
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

    if os.path.exists("chicago_pin_universe.csv"):
        print("Loading Chicago PIN universe data from csv.")
        chicago_pin_universe = pd.read_csv(
            "chicago_pin_universe.csv",
            dtype={"pin": "string", "pin10": "string"}
        )
    else:
        chicago_pin_universe = pull_existing_pins_from_athena(cursor)

    start_date, end_date, deduplicate = sys.argv[1], sys.argv[2], sys.argv[3]
    deduplicate = deduplicate.lower() == "true"

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

    permits_validated = flag_invalid_pins(permits_renamed, chicago_pin_universe)

    permits_shortened = flag_fix_long_fields(permits_validated)

    if deduplicate:
        print(
            "Number of permits prior to deduplication: "
            f"{len(permits_shortened)}"
        )
        permits_deduped = deduplicate_permits(
            cursor,
            permits_shortened,
            start_date,
            end_date
        )
        print(
            "Number of permits after deduplication: "
            f"{len(permits_deduped)}"
        )
    else:
        permits_deduped = permits_shortened

    file_base_name = gen_file_base_name()

    save_xlsx_files(permits_deduped, 200, file_base_name)
