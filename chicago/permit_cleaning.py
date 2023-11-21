"""
Chicago Permit Ingest Process - Automation

This script automates the current process for cleaning permit data from the Chicago Data Portal's Building Permits table
and preparing it for upload to iasWorld via Smartfile. This involves fetching the data, cleaning up certain fields, 
organizing columns to match the Smartfile template, and batching the data into csv files of 200 rows each. This process
also splits off data that is ready for upload from data that still needs some manual review before upload, saving each
in separate csvs in separate folders. Data that need review are split into two categories and corresponding folderes/files:
those with quick fixes for fields over character or amount limits, those with more complicated fixes for missing fields.
"""

import requests
import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta
import os
import numpy as np
import math


# DEFINE FUNCTIONS

def download_all_permits():
    # update limit in url below when ready to work with full dataset (as of Nov 17, 2023 dataset has 756,766 rows)
    url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=500&$order=issue_date DESC"
    permits_response = requests.get(url)
    permits_response.raise_for_status()
    permits = permits_response.json()
    permits_df = pd.DataFrame(permits)
    return permits_df



def expand_multi_pin_permits(df):
    """ 
    Data from the Chicago open data permits table (data this script works with) has rows uniquely identified by permit number.
    Permits can apply to multiple PINs, with additional PINs recorded in the PIN2 - PIN10 fields.
    We want rows that are uniquely identified by PIN and permit number. 
    This function creates new rows for each additional PIN in multi-PIN permits and saves the relevant PIN in pin_solo.
    """    
    # the downloaded dataframe will not include any pin columns that are completely blank, so check for existing ones here
    all_pin_columns = ["pin1", "pin2", "pin3", "pin4", "pin5", "pin6", "pin7", "pin8", "pin9", "pin10"]
    pin_columns = [col for col in df.columns if col in all_pin_columns]
    extra_pins = [pin for pin in pin_columns if pin != "pin1"]
    non_pin_columns = [col for col in df.columns if col not in pin_columns]

    melted_df = pd.melt(df, id_vars=non_pin_columns, value_vars=pin_columns, var_name="pin_type", value_name="solo_pin")
    
    # keep rows with NA for pin1, filter out rows with NA for other pins
    melted_df = melted_df[(melted_df["pin_type"] == "pin1") | ((melted_df["pin_type"] != "pin1") & melted_df["solo_pin"].notna())]
    
    # order rows by permit number then pin type (so pins will be in order of their assigned numbering in permit table, not necessarily by pin number)
    melted_df = melted_df.sort_values(by=["permit_", "pin_type"]).reset_index(drop=True)

    return melted_df


# update pin to match formatting of iasWorld
def format_pin(df): 
    # iasWorld format doesn't include dashes
    df["pin_final"] = df["solo_pin"].astype(str).str.replace("-", "")
    # add zeros to 10-digit PINs to transform into 14-digits PINs
    df["pin_final"] = df["pin_final"].apply(lambda x: x + "0000" if len(x) == 10 else x if x != "nan" else "")
    return df


# Eliminate columns not included in permit upload and rename and order to match Smartfile excel format
def organize_columns(df):

    address_columns = ["street_number", "street_direction", "street_name", "suffix"]
    df["Address"] = df[address_columns].astype(str).fillna("").agg(" ".join, axis=1)

    df["issue_date"] = pd.to_datetime(df["issue_date"], format="%Y-%m-%dT%H:%M:%S.%f", errors='coerce').dt.strftime("%-m/%-d/%Y")

    column_renaming_dict = {
        "pin_final": "PIN* [PARID]",
        "permit_": "Local Permit No.* [USER28]",
        "issue_date": "Issue Date* [PERMDT]",
        "reported_cost": "Amount* [AMOUNT]",
        "Address": "Applicant Street Address* [ADDR1]",
        "contact_1_name": "Applicant* [USER21]",
        "work_description": "Notes [NOTE1]"
        }
    
    data_relevant = df[[col for col in df.columns if col in column_renaming_dict]]
    data_renamed = data_relevant.rename(columns=column_renaming_dict)
 
    column_order = ["PIN* [PARID]",	
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
    
    df["FLAG COMMENTS"] = "" # will append written comments into this column

    # these fields have the following character limits in Smartfile / iasWorld, flag if over limit
    long_fields_to_flag = [
        ("FLAG, LENGTH: Applicant Name", "Applicant* [USER21]", 50, "Applicant* [USER21] over 50 char limit by "),
        ("FLAG, LENGTH: Permit Number", "Local Permit No.* [USER28]", 18, "Local Permit No.* [USER28] over 18 char limit by "),
        ("FLAG, LENGTH: Applicant Street Address", "Applicant Street Address* [ADDR1]", 40, "Applicant Street Address* [ADDR1] over 40 char limit by "),
        ("FLAG, LENGTH: Note1", "Notes [NOTE1]", 2000, "Notes [NOTE1] over 2000 char limit by ")
    ]
    
    for flag_name, column, limit, comment in long_fields_to_flag:
        df[flag_name] = df[column].apply(lambda val: 0 if pd.isna(val) else (1 if len(str(val)) > limit else 0))
        df["FLAG COMMENTS"] += df[column].apply(lambda val: "" if pd.isna(val) else ("" if len(str(val)) < limit else comment + str(len(str(val)) - limit) + "; "))
     
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

    # create columns for total number of flags for length and for missingness since they'll get sorted into separate csv files
    df["FLAGS, TOTAL - LENGTH/VALUE"] = df.filter(like="FLAG, LENGTH").values.sum(axis=1) + df.filter(like="FLAG, VALUE").values.sum(axis=1)
    df["FLAGS, TOTAL - EMPTY"] = df.filter(like="FLAG, EMPTY").values.sum(axis=1)

    # need a column that identifies rows with flags for field length/amount but no flags for emptiness
    df["MANUAL REVIEW"] = np.where((df["FLAGS, TOTAL - EMPTY"] == 0) & (df["FLAGS, TOTAL - LENGTH/VALUE"] > 0), 1, 0)
    
    return df



def gen_csv_base_name():
    today = datetime.today().date()
    today_string = today.strftime("%Y_%m_%d")
    csv_name = today_string + "_permits_"
    return csv_name


def save_csv_files(df, max_rows, csv_base_name):
    # separate rows that are ready for upload from ones that need manual review or have missing PINs
    df_ready = df[(df["FLAGS, TOTAL - LENGTH/VALUE"] == 0) & (df["FLAGS, TOTAL - EMPTY"] == 0)].reset_index().drop(columns=["index"])  
    df_ready = df_ready.drop(columns=df_ready.filter(like="FLAG").columns).drop(columns=["MANUAL REVIEW"])
    
    df_review_length = df[df["MANUAL REVIEW"] == 1].drop(columns=["MANUAL REVIEW"]).reset_index().drop(columns=["index"])
    df_review_length = df_review_length.drop(columns=df_review_length.filter(like="FLAG, EMPTY")).drop(columns=["FLAGS, TOTAL - EMPTY"])
    
    df_review_empty = df[df["FLAGS, TOTAL - EMPTY"] > 0].reset_index().drop(columns=["index", "MANUAL REVIEW"]) 

    print("df_ready length: ", len(df_ready))
    print("df_review_length length: ", len(df_review_length))
    print("df_review_empty length: ", len(df_review_empty))

    # create new folders with today's date to save csv files in (1 each for ready, needing manual shortening of fields, have missing fields)
    folder_for_csv_files_ready = datetime.today().date().strftime("csvs_for_smartfile_%Y_%m_%d")
    os.makedirs(folder_for_csv_files_ready, exist_ok=True) # note this will override an existing folder with same name
    folder_for_csv_files_review_length = datetime.today().date().strftime("csvs_for_review_length_%Y_%m_%d")
    os.makedirs(folder_for_csv_files_review_length, exist_ok=True) # note this will override an existing folder with same name
    folder_for_csv_files_review_empty = datetime.today().date().strftime("csvs_for_review_empty_%Y_%m_%d")
    os.makedirs(folder_for_csv_files_review_empty, exist_ok=True) # note this will override an existing folder with same name

    # save ready permits batched into 200 permits max per csv file
    num_files_ready = len(df_ready) // max_rows + 1
    print("creating " + str(num_files_ready) + " csv files ready for smartfile upload")
    for i in range(num_files_ready):
        start_index = i * max_rows
        end_index = (i + 1) * max_rows
        file_dataframe = df_ready.iloc[start_index:end_index].copy()
        file_dataframe.reset_index(drop=True, inplace=True) # each csv needs an index from 1 to 200
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_name = os.path.join(folder_for_csv_files_ready, csv_base_name + f"ready_{i+1}.csv")
        file_dataframe.to_csv(file_name, index=True)

    # permits needing manual field shortening and those with missing fields will be saved as single csvs, not batched by 200 rows
    df_review_length.index = df_review_length.index + 1
    df_review_length.index.name = "# [LLINE]"
    file_name_review_length = os.path.join(folder_for_csv_files_review_length, csv_base_name + "review_length.csv")
    df_review_length.to_csv(file_name_review_length, index=True)

    df_review_empty.index = df_review_empty.index +1
    df_review_empty.index.name = "# [LLINE]"
    file_name_review_empty = os.path.join(folder_for_csv_files_review_empty, csv_base_name + "review_empty.csv")
    df_review_empty.to_csv(file_name_review_length,index=True)





# CALL FUNCTIONS

permits = download_all_permits()

permits_expanded = expand_multi_pin_permits(permits)
print("df expanded length: ", len(permits_expanded))
permits_pin = format_pin(permits_expanded)

permits_renamed = organize_columns(permits_pin)

permits_shortened = flag_fix_long_fields(permits_renamed)

csv_base_name = gen_csv_base_name()

save_csv_files(permits_shortened, 200, csv_base_name)