"""
Chicago Permit Ingest Process - Automation

This script automates the current process for cleaning permit data from the Chicago Data Portal's Building Permits table
and preparing it for upload to iasWorld via Smartfile. This involves fetching the data, cleaning up certain fields, 
organizing columns to match the Smartfile template, and batching the data into csv files of 200 rows each. This process
also splits off data that is ready for upload from data that still needs some manual review before upload, saving each
in separate csvs in separate folders.
"""

import requests
import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta
import os


# DEFINE FUNCTIONS

def download_all_permits():
    # update limit in url below when ready to work with full dataset (as of Nov 17, 2023 dataset has 756,766 rows)
    url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=5000"
    permits_response = requests.get(url)
    print("status code: ", permits_response.status_code)
    print("Headers: ", permits_response.headers.get("Content-Type"))
    permits = permits_response.json()
    permits_df = pd.DataFrame(permits)
    return permits_df


def expand_multi_pin_permits(df):
    """ 
    Data from the Chicago open data permits table (data this script works with) has rows uniquely identified by permit number.
    Permits can apply to multiple PINs, with additional PINs recorded in the PIN2 - PIN10 fields.
    We want rows that are uniquely identified by PIN and permit number. 
    This function creates new rows for each additional PIN in multi-PIN permits and saved the relevant PIN in pin_solo.
    """    
    # the downloaded dataframe will not include any pin columns that are completely blank, so check for existing ones here
    all_pin_columns = ["pin1", "pin2", "pin3", "pin4", "pin5", "pin6", "pin7", "pin8", "pin9", "pin10"]
    pin_columns = [col for col in df.columns if col in all_pin_columns]
    extra_pins = [pin for pin in pin_columns if pin != "pin1"]
    non_pin_columns = [col for col in df.columns if col not in pin_columns]

    print("number of not NA values in pin2 - pin7 fields: ", df[extra_pins].notna().sum().sum())
    print("starting number of rows: ", len(df)) 
    melted_df = pd.melt(df, id_vars=non_pin_columns, value_vars=pin_columns, var_name="pin_type", value_name="solo_pin")
    print("number of rows after melting before removing NaNs: ", len(melted_df))
    
    # keep rows with NA for pin1, filter out rows with NA for other pins
    melted_df = melted_df[(melted_df["pin_type"] == "pin1") | ((melted_df["pin_type"] != "pin1") & melted_df["solo_pin"].notna())]
    print("number of rows after removing NaNs: ", len(melted_df))
    
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
        "pin14": "PIN* [PARID]",
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



# if possible to code, Will has also seen times when applicant name is listed twice pushing the field over the limit
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
  
    # flag rows that had changes made to Applicant name
    df["FLAG, SHORTENED: Applicant Name"] = df["Applicant* [USER21]"].apply(lambda text: 0 if pd.isna(text) else (1 if len(str(text)) > 50 else 0))
    df["Applicant* [USER21]"] = df["Applicant* [USER21]"].replace(name_shortening_dict, regex=True)
    # flag rows that are still too long after substititions and need manual review
    df["FLAG, LENGTH: Applicant Name"] = df["Applicant* [USER21]"].apply(lambda text: 0 if pd.isna(text) else (1 if len(text) > 50 else 0))
    
    # flag other fields over character limit for manual review
    df["FLAG, LENGTH: Permit Number"] = df["Local Permit No.* [USER28]"].apply(lambda number: 0 if pd.isna(number) else (1 if len(str(number)) > 18 else 0))
    df["FLAG, LENGTH: Applicant Street Address"] = df["Applicant Street Address* [ADDR1]"].apply(lambda text: 0 if pd.isna(text) else (1 if len(text) > 40 else 0))
    df["FLAG, LENGTH: Applicant City State Zip"] = df["Applicant City, State, Zip* [ADDR3]"].apply(lambda text: 0 if pd.isna(text) else (1 if len(text) > 28 else 0))
    df["FLAG, LENGTH: Note1"] = df["Notes [NOTE1]"].apply(lambda text: 0 if pd.isna(text) else (1 if len(text) > 2000 else 0))
    
    # round Amount to closest dollar because smart file doesn't accept decimal amounts, then flag values above upper limit
    print("Before rounding Amount, there are this many nans: ", df["Amount* [AMOUNT]"].isna().sum())
    df["Amount* [AMOUNT]"] = pd.to_numeric(df["Amount* [AMOUNT]"], errors="coerce").round().astype("Int64")
    print("After rounding Amount, there are this many nans: ", df["Amount* [AMOUNT]"].isna().sum())
    df["FLAG, VALUE: Amount"] = df["Amount* [AMOUNT]"].apply(lambda value: 0 if pd.isna(value) or value <= 2147483647 else 1)
    
    # also flag rows where fields are blank for manual review (for fields we're populating in smartfile template)
    df["FLAG, EMPTY: PIN"] = df["PIN* [PARID]"].apply(lambda value: 1 if value == "" else 0)
    df["FLAG, EMPTY: Issue Date"] = df["Issue Date* [PERMDT]"].apply(lambda value: 1 if pd.isna(value) or value.strip() == "" else 0)
    df["FLAG, EMPTY: Amount"] = df["Amount* [AMOUNT]"].apply(lambda value: 1 if pd.isna(value) or value == "" else 0)
    # df["FLAG, EMPTY: Submit Dt"] = df["Submit Dt* [CERTDATE]"].apply(lambda value: 1 if pd.isna(value) or value == "" else 0)
    # (not using this field for now) df["FLAG, EMPTY: Applicant Street Address"] = df["Applicant Street Address* [ADDR1]"].apply(lambda value: 1 if pd.isna(value) or value == "" else 0)
    df["FLAG, EMPTY: Applicant"] = df["Applicant* [USER21]"].apply(lambda text: 1 if pd.isna(text) or text.strip() == "" else 0)
    df["FLAG, EMPTY: Local Permit No"] = df["Local Permit No.* [USER28]"].apply(lambda value: 1 if pd.isna(value) or value == "" else 0)
    df["FLAG, EMPTY: Note1"] = df["Notes [NOTE1]"].apply(lambda text: 1 if pd.isna(text) or text.strip() == "" else 0)

    # create column for total number of flags per row for easy division of data into smartfile ready and needing manual review
    df["FLAGS, TOTAL"] = df.filter(like="FLAG").sum(axis=1)
    # think about whether I want flags for PINs to be included in my total flags measure, might want to deal with them separately since there are so many....
    return df



def gen_csv_base_name():
    today = datetime.today().date()
    print("today: ", today)
    first_day_current_month = today.replace(day=1)
    print("first day current month: ", first_day_current_month)
    last_day_previous_month = first_day_current_month - timedelta(days=1)
    print("last day previous month: ", last_day_previous_month)
    previous_month_year = last_day_previous_month.strftime("%Y_%m")
    print("previous month and year: ", previous_month_year)
    csv_name = previous_month_year + "_permits_"
    return csv_name


def save_csv_files(df, max_rows, csv_base_name):
    # separate rows that are ready for upload from ones that need manual review
    df_ready = df[df["FLAGS, TOTAL"] == 0]  
    df_ready = df_ready.drop(columns=df_ready.filter(like="FLAG").columns)
    df_review = df[df["FLAGS, TOTAL"] > 0]

    # create new folders with today's date to save csv files in
    folder_for_csv_files_ready = datetime.today().date().strftime("csvs_for_smartfile_%Y_%m_%d")
    os.makedirs(folder_for_csv_files_ready, exist_ok=True) # note this will override an existing folder with same name
    folder_for_csv_files_review = datetime.today().date().strftime("csvs_for_review_%Y_%m_%d")
    os.makedirs(folder_for_csv_files_review, exist_ok=True) # note this will override an existing folder with same name

    num_files_ready = len(df_ready) // max_rows + 1
    for i in range(num_files_ready):
        start_index = i * max_rows
        end_index = (i + 1) * max_rows
        file_dataframe = df_ready.iloc[start_index:end_index]
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_name = os.path.join(folder_for_csv_files_ready, csv_base_name + f"ready_{i+1}.csv")
        print("file name: ", file_name)
        file_dataframe.to_csv(file_name, index=True)

    
    num_files_review = len(df_review) // max_rows + 1
    for i in range(num_files_review):
        start_index = i * max_rows
        end_index = (i + 1) * max_rows
        file_dataframe = df_review.iloc[start_index:end_index]
        file_dataframe.index = file_dataframe.index + 1
        file_dataframe.index.name = "# [LLINE]"
        file_name = os.path.join(folder_for_csv_files_review, csv_base_name + f"review_{i+1}.csv")
        print("file name: ", file_name)
        file_dataframe.to_csv(file_name, index=True)




# CALL FUNCTIONS

permits = download_all_permits()

permits_expanded = expand_multi_pin_permits(permits)

permits_pin = format_pin(permits_expanded)

permits_renamed = organize_columns(permits_pin)

permits_shortened = flag_fix_long_fields(permits_renamed)

csv_base_name = gen_csv_base_name()
save_csv_files(permits_shortened, 200, csv_base_name)

