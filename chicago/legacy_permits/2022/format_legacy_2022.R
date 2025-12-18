library(dplyr)
library(openxlsx)
library(tidyr)

source("helper.R")

actionable <- read_xlsx_all_char(
  "2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Actionable"
) %>%
  select(
    "PIN* [PARID]"                   = PIN1,
    "Local Permit No.* [USER28]"        = Local.Permit.No,
    "Issue Date* [PERMDT]"              = ISSUE_DATE,
    "Amount* [AMOUNT]"                  = Amount,
    "Applicant Street Address* [ADDR1]" = Street.Address,
    "Applicant* [USER21]"               = Applicant,
    "Notes [NOTE1]"                     = Notes,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "CHICAGO, IL") %>%
  expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `PIN* [PARID]` = normalize_pin(`PIN* [PARID]`),
    `Issue Date* [PERMDT]` = format(
      as.Date(as.numeric(`Issue Date* [PERMDT]`), origin = "1899-12-30"),
      "%m/%d/%Y"
    )
  ) %>%
  finalize_columns(needed_columns)

need_worked <- read_xlsx_all_char(
  "2022/2022 City permits for manual review v5processed (005).xlsx",
  sheet = "Need worked"
) %>%
  select(
    "PIN* [PARID]"                   = PIN1,
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
    `PIN* [PARID]` = normalize_pin(`PIN* [PARID]`),
    `Issue Date* [PERMDT]` = format(
      as.Date(as.numeric(`Issue Date* [PERMDT]`), origin = "1899-12-30"),
      "%m/%d/%Y"
    )
  ) %>%
  finalize_columns(needed_columns)

write.csv(
  need_worked$upload,
  "2022/2022permits_processed_legacy_need_worked_upload.csv",
  row.names = FALSE
)

write.csv(
  need_worked$need_review,
  "2022/2022permits_processed_legacy_need_worked_review.csv",
  row.names = FALSE
)

write.csv(
  actionable$upload,
  "2022/2022permits_processed_legacy_actionable.csv",
  row.names = FALSE
  )

write.csv(
  actionable$need_review,
  "2022/2022permits_processed_legacy_actionable_review.csv",
  row.names = FALSE
  )
