library(dplyr)
library(openxlsx)
library(tidyr)

source("legacy_permits/helper.R")

actionable <- read_xlsx_all_char(
  "legacy_permits/2023/2023permits_processed_2.xlsx",
  "Actionable"
) %>%
  select(
    "PIN* [PARID]"                   = pin1,
    "Local Permit No.* [USER28]"        = `permit#`,
    "Issue Date* [PERMDT]"              = issue_date,
    "Amount* [AMOUNT]"                  = rounded.cost,
    "Notes [NOTE1]"                     = notes,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant* [USER21]"               = contact_1_name,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
  expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `PIN* [PARID]` = normalize_pin(`PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  finalize_columns(needed_columns)

need_worked <- read_xlsx_all_char(
  "legacy_permits/2023/2023permits_processed_2.xlsx",
  "Need worked"
) %>%
  select(
    "PIN* [PARID]"                   = pin1,
    "Local Permit No.* [USER28]"        = `permit#`,
    "Issue Date* [PERMDT]"              = issue_date,
    "Amount* [AMOUNT]"                  = rounded.cost,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant* [USER21]"               = contact_1_name,
    "Notes [NOTE1]"                     = notes,
    "Reinstated"          = Reinstated.permit,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL",
  # if reinstated is not NA replace the value for Notes with it
  `Notes [NOTE1]` = ifelse(!is.na(`Reinstated`), `Reinstated`, `Notes [NOTE1]`)) %>%
  select(-c(`Reinstated`)) %>%
  expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `PIN* [PARID]` = normalize_pin(`PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
  finalize_columns(needed_columns)

write.csv(
  need_worked$upload,
  "legacy_permits/2023/2023permits_processed_legacy_need_worked_upload.csv",
  row.names = FALSE
)

write.csv(
  need_worked$need_review,
  "legacy_permits/2023/2023permits_processed_legacy_need_worked_review.csv",
  row.names = FALSE
)

write.csv(
  actionable$upload,
  "legacy_permits/2023/2023permits_processed_legacy_actionable_upload.csv",
  row.names = FALSE
)

write.csv(
  actionable$need_review,
  "legacy_permits/2023/2023permits_processed_legacy_actionable_review.csv",
  row.names = FALSE
)
