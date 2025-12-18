library(dplyr)
library(openxlsx)
library(tidyr)

source("helper.R")

need_worked <- read_xlsx_all_char(
  "legacy_permits/2021/2021 manual review processed.xlsx",
  sheet = "Need worked"
) %>%
  mutate(
    `Applicant Street Address* [ADDR1]` =
      paste(STREET_NUMBER, STREET.DIRECTION, STREET_NAME, SUFFIX)
  ) %>%
  select(
    "PIN* [PARID]"            = PIN1,
    "Local Permit No.* [USER28]" = `PERMIT#`,
    "Issue Date* [PERMDT]"       = ISSUE_DATE,
    "Amount* [AMOUNT]"           = REPORTED_COST,
    "Applicant Street Address* [ADDR1]",
    "Applicant* [USER21]"        = CONTACT_1_NAME,
    "Notes [NOTE1]"              = WORK_DESCRIPTION,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "CHICAGO, IL") %>%
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
  "legacy_permits/2021/2021permits_processed_legacy_need_worked_upload.csv",
  row.names = FALSE
)

write.csv(
  need_worked$need_review,
  "legacy_permits/2021/2021permits_processed_legacy_need_worked_review.csv",
  row.names = FALSE
)
