library(dplyr)
library(openxlsx)
library(tidyr)

options(scipen = 999)

column_order <- c(
  "# [LLINE]",
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
)

needed_columns <- c(
  "PIN* [PARID]",
  "Local Permit No.* [USER28]",
  "Issue Date* [PERMDT]",
  "Amount* [AMOUNT]",
  "Applicant Street Address* [ADDR1]",
  "Applicant* [USER21]",
  "Applicant City, State, Zip* [ADDR3]",
  "Notes [NOTE1]"
)

expand_pins <- function(df_raw) {
  pin_cols <- names(df_raw)[grepl("^PIN\\d+$",
    names(df_raw),
    ignore.case = TRUE
  )]

  df_long <- df_raw %>%
    pivot_longer(
      cols = all_of(pin_cols),
      names_to = "pin_col",
      values_to = "extra_pin",
      values_drop_na = TRUE
    ) %>%
    mutate(`PIN* [PARID]` = extra_pin) %>%
    select(-pin_col, -extra_pin)

  bind_rows(
    df_long,
    df_raw %>% select(-all_of(pin_cols))
  ) %>%
    distinct() %>%
    arrange(`Local Permit No.* [USER28]`, `PIN* [PARID]`)
}



normalize_pin <- function(pin_vec) {
  # remove - from PIN
  pin_vec <- gsub("-", "", pin_vec)
  # If pin is 13 digits add leading 0
  pin_vec <- ifelse(nchar(pin_vec) == 13,
    paste0("0", pin_vec), pin_vec
  )
  # If PIN is 10 digits add 4 final digits
  pin_vec <- ifelse(nchar(pin_vec) == 10,
    paste0(pin_vec, "0000"), pin_vec
  )
  # If pin is 9 digits do both
  pin_vec <- ifelse(nchar(pin_vec) == 9,
    paste0("0", pin_vec, "0000"), pin_vec
  )

  pin_vec
}

ensure_columns <- function(df, column_order) {
  for (col in column_order) {
    if (!col %in% names(df)) df[[col]] <- NA
  }
  df[, column_order, drop = FALSE]
}

read_xlsx_all_char <- function(path, sheet) {
  openxlsx::read.xlsx(path, sheet = sheet) %>%
    dplyr::mutate(across(everything(), as.character))
}
finalize_columns <- function(df, needed_columns) {
  df_flagged <- df %>%
    mutate(
      valid_needed = if_all(all_of(needed_columns), ~ !is.na(.x)),
      valid_pin = nchar(.data[["PIN* [PARID]"]]) == 14 & grepl(
        "^\\d{14}$",
        .data[["PIN* [PARID]"]]
      ),
      valid_permit = nchar(.data[["Local Permit No.* [USER28]"]])
      %in% c(9, 10),
      valid_addr_len =
        nchar(.data[["Applicant Street Address* [ADDR1]"]]) <= 40,
      valid_note_len = nchar(.data[["Notes [NOTE1]"]]) <= 2000,
      valid_name_len = nchar(.data[["Applicant* [USER21]"]]) <= 50,
      valid_amount =
        suppressWarnings(
          as.numeric(.data[["Amount* [AMOUNT]"]]) < 2147483647
        ),
      valid_row = valid_needed &
        valid_pin & # nolint
        valid_permit &
        valid_addr_len &
        valid_note_len &
        valid_name_len
    )

  upload <- df_flagged %>%
    filter(valid_row) %>%
    mutate("# [LLINE]" = row_number()) %>%
    select(-starts_with("valid_"))

  need_review <- df_flagged %>%
    filter(!valid_row) %>%
    mutate("# [LLINE]" = row_number())

  list(
    upload = upload,
    need_review = need_review
  )
}
