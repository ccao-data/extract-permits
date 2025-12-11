library(dplyr)
library(openxlsx)
library(tidyr)

column_order <- c(
  "ID	PIN* [PARID]",
  "Local Permit No.* [USER28]",
  "Issue Date* [PERMDT]",
  "Desc 1* [DESC1]",
  "Desc 2 Code 1 [USER6]",
  "Desc 2 Code 1 [USER6]2",
  "Desc 2 Code 2 [USER7]",
  "Desc 2 Code 3 [USER8]",
  "Amount* [AMOUNT]",
  "Assessable [IS_ASSESS]",
  "Applicant Street Address* [ADDR1]",
  "Applicant Address 2 [ADDR2]",
  "SUFFIX",
  "Applicant City, State, Zip* [ADDR3]",
  "Contact Phone* [PHONE]",
  "Applicant* [USER21]",
  "Notes [NOTE1]",
  "Occupy Dt [UDATE1]",
  "Submit Dt* [CERTDATE]",
  "Est Comp Dt [UDATE2]"
)

needed_columns <- c(  "ID	PIN* [PARID]",
  "Local Permit No.* [USER28]",
  "Issue Date* [PERMDT]",
  "Amount* [AMOUNT]",
  "Applicant Street Address* [ADDR1]",
  "Applicant* [USER21]",
  "Applicant City, State, Zip* [ADDR3]",
  "Notes [NOTE1]"
)

expand_pins <- function(df_raw) {
  df_long <- df_raw %>%
    # pivot longer and replicate data for any pin_x which does not have NA value
    # to the ID PIN* [PARID] column
    pivot_longer(
      cols = starts_with("pin"),
      names_to  = "pin_col",
      values_to = "extra_pin",
      values_drop_na = TRUE
    ) %>%
    mutate(
      `ID	PIN* [PARID]` = extra_pin
    ) %>%
    select(
      -pin_col,
      -extra_pin
    )

  # Stack the original pin1 rows with the extra-pin rows
  bind_rows(
    df_long,
    df_raw %>% select(-starts_with("pin"))
  ) %>%
    distinct() %>%
    arrange(
      `Local Permit No.* [USER28]`,
      `ID	PIN* [PARID]`
    )
}


normalize_pin <- function(pin_vec) {
  # remove - from PIN
  pin_vec <- gsub("-", "", pin_vec)
  # If pin is 13 digits add leading 0
  pin_vec <- ifelse(nchar(pin_vec) == 13, paste0("0", pin_vec), pin_vec)
  # If PIN is 10 digits add 4 final digits
  pin_vec <- ifelse(nchar(pin_vec) == 10, paste0(pin_vec, "0000"), pin_vec)
  # If pin is 9 digits do both
  pin_vec <- ifelse(nchar(pin_vec) == 9, paste0("0", pin_vec, "0000"), pin_vec)

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
