library(dplyr)
library(openxlsx)
library(tidyr)

source("legacy_permits/helper.R")

actionable_raw <- read_xlsx_all_char(
  "legacy_permits/2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Actionable"
) %>%
  select(
    "ID PIN* [PARID]"                   = PIN1,
    "Local Permit No.* [USER28]"        = Local.Permit.No,
    "Issue Date* [PERMDT]"              = ISSUE_DATE,
    "Amount* [AMOUNT]"                  = Amount,
    "Applicant Street Address* [ADDR1]" = Street.Address,
    "Applicant* [USER21]"               = Applicant,
    "Notes [NOTE1]"                     = Notes,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
  expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `ID PIN* [PARID]` = normalize_pin(`ID PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  finalize_columns(needed_columns)

need_worked <- read_xlsx_all_char(
  "legacy_permits/2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Need worked"
) %>%
  select(
    "ID PIN* [PARID]"                   = PIN1,
    "Local Permit No.* [USER28]"        = Local.Permit.No,
    "Issue Date* [PERMDT]"              = ISSUE_DATE,
    "Amount* [AMOUNT]"                  = Amount,
    "Applicant Street Address* [ADDR1]" = Street.Address,
    "Applicant* [USER21]"               = Applicant,
    "Notes [NOTE1]"                     = Notes,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
    expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `ID PIN* [PARID]` = normalize_pin(`ID PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  finalize_columns(needed_columns)

write.csv(
  need_worked$upload,
  "legacy_permits/2022/2022permits_processed_legacy_need_worked_review.csv",
  row.names = FALSE
)

write.csv(
  need_worked$need_review,
  "legacy_permits/2022/2022permits_processed_legacy_need_worked_review.csv",
  row.names = FALSE
)

write.csv(
  actionable$upload,
  "legacy_permits/2022/2022permits_processed_legacy_actionable.csv",
  row.names = FALSE
  )

write.csv(
  actionable$need_review,
  "legacy_permits/2022/2022permits_processed_legacy_actionable_review.csv",
  row.names = FALSE
  )
