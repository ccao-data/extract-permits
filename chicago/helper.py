import re

import pandas as pd

# Ordered column output for final CSV upload
REQUIRED_COLS = [
    "LLINE",
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


def finalize_columns(
    df: pd.DataFrame, filled_columns: list[str]
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
    df_flagged["valid_amount"] = df_flagged["Amount* [AMOUNT]"] < 2147483647

    df_flagged["valid_row"] = (
        df_flagged["valid_filled"]
        & df_flagged["valid_pin"]
        & df_flagged["valid_permit"]
        & df_flagged["valid_addr_len"]
        & df_flagged["valid_note_len"]
        & df_flagged["valid_name_len"]
    )

    upload = df_flagged[df_flagged["valid_row"]].copy()
    upload["LLINE"] = range(1, len(upload) + 1)
    upload = upload.loc[:, ~upload.columns.str.startswith("valid_")]

    need_review = df_flagged[~df_flagged["valid_row"]].copy()
    need_review["LLINE"] = range(1, len(need_review) + 1)

    return {"upload": upload, "need_review": need_review}
