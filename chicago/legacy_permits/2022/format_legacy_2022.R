library(dplyr)
library(openxlsx)
library(tidyr)

source("legacy_permits/helper.R")

actionable_raw <- read_xlsx_all_char(
  "legacy_permits/2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Actionable"
) %>%
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
actionable <- expand_pins(actionable_raw)

# Read the "actionable" sheet, everything as character, and rename
need_worked_raw <- read_xlsx_all_char(
  "legacy_permits/2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Need worked"
) %>%
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

need_worked <- expand_pins(need_worked_raw)

# Expand multi-PIN rows
actionable   <- ensure_columns(actionable, column_order)
need_worked <- ensure_columns(need_worked, column_order)

actionable <- actionable %>%
  mutate(
    `ID	PIN* [PARID]` = normalize_pin(`ID	PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  # Remove rows without 14 digit pins
  filter(nchar(`ID	PIN* [PARID]`) == 14)


# Final formatting: ensure columns and normalize PIN
need_worked <- need_worked %>%
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
  need_worked,
  "legacy_permits/2022/2022permits_processed_legacy_need_worked.csv",
  row.names = FALSE
)

write.csv(
  actionable,
  "legacy_permits/2022/2022permits_processed_legacy_actionable.csv",
  row.names = FALSE
  )
