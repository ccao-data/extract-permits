library(dplyr)
library(openxlsx)
library(tidyr)

source("legacy_permits/helper.R")

need_worked <- read.xlsx(
  "legacy_permits/2021/2021 manual review processed.xlsx",
  sheet = "Need worked"
) %>%
  mutate(across(everything(), as.character)) %>%
  mutate(
    `Applicant Street Address* [ADDR1]` =
      paste(STREET_NUMBER, STREET.DIRECTION, STREET_NAME, SUFFIX)
  ) %>%
  select(
    "ID	PIN* [PARID]"            = PIN1,
    "Local Permit No.* [USER28]" = `PERMIT#`,
    "Issue Date* [PERMDT]"       = ISSUE_DATE,
    "Amount* [AMOUNT]"           = REPORTED_COST,
    "Applicant Street Address* [ADDR1]",
    "Applicant* [USER21]"        = CONTACT_1_NAME,
    "Notes [NOTE1]"              = WORK_DESCRIPTION,
    PIN2, PIN3, PIN4, PIN5, PIN6, PIN7
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
  expand_pins() %>%
  ensure_columns(column_order)

# Expand multi-PIN rows
need_worked <- expand_pins(need_worked_raw)

need_worked <- ensure_columns(need_worked, column_order) %>%
  mutate(
    `ID	PIN* [PARID]` = normalize_pin(`ID	PIN* [PARID]`),
    `Issue Date* [PERMDT]` = as.Date(
      as.numeric(`Issue Date* [PERMDT]`),
      origin = "1899-12-30"
    )
  ) %>%
filter(
  nchar(`ID	PIN* [PARID]`) == 14,
  if_all(all_of(needed_columns), ~ !is.na(.x))
)

write.csv(
  need_worked,
  "legacy_permits/2021/2021permits_processed_legacy_need_worked.csv",
  row.names = FALSE
)
