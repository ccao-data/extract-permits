library(dplyr)
library(openxlsx)
library(tidyr)

source("helper.R")

assessed_raw <- read_xlsx_all_char(
  "2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Assessed"
) %>%
  mutate(across(everything(), as.character)) %>%
  select(
    "ID	PIN* [PARID]"                   = PIN1,
    "Local Permit No.* [USER28]"        = Local.Permit.No,
    "Issue Date* [PERMDT]"              = ISSUE_DATE,
    "Amount* [AMOUNT]"                  = Amount,
    "Applicant Street Address* [ADDR1]" = Street.Address,
    "Applicant* [USER21]"               = Applicant,
    "Notes [NOTE1]"                     = Notes,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

# Expand multi-PIN rows
assessed <- expand_pins(assessed_raw)

# Read the "Assessed" sheet, everything as character, and rename
read_worked_raw <- read_xlsx_all_char(
  "2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Need worked"
) %>%
  mutate(across(everything(), as.character)) %>%
  select(
    "ID	PIN* [PARID]"                   = PIN1,
    "Local Permit No.* [USER28]"        = Local.Permit.No,
    "Issue Date* [PERMDT]"              = ISSUE_DATE,
    "Amount* [AMOUNT]"                  = Amount,
    "Applicant Street Address* [ADDR1]" = Street.Address,
    "Applicant* [USER21]"               = Applicant,
    "Notes [NOTE1]"                     = Notes,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

# Expand multi-PIN rows
assessed   <- ensure_columns(assessed, column_order)
need_worked <- ensure_columns(need_worked, column_order)

# Final formatting: ensure columns and normalize PIN
data <- rbind(assessed, need_worked) %>%
  mutate(
    `ID	PIN* [PARID]` = normalize_pin(`ID	PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  # Remove rows without 14 digit pins
  filter(nchar(`ID	PIN* [PARID]`) == 14)

write.csv(
  data,
  "2022/2022permits_processed_legacy.csv",
  row.names = FALSE
)