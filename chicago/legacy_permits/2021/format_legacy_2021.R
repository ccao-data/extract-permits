library(dplyr)
library(openxlsx)
library(tidyr)

source("helper.R")

assessed_raw <- read.xlsx(
  "2021/2021 manual review processed- JW completed (1).xlsx",
  sheet = "Assessed"
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
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

# Expand multi-PIN rows
assessed <- expand_pins(assessed_raw)

need_worked_raw <- read.xlsx(
  "2021/2021 manual review processed- JW completed (1).xlsx",
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
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

# Expand multi-PIN rows
need_worked <- expand_pins(need_worked_raw)

assessed   <- ensure_columns(assessed, column_order)
need_worked <- ensure_columns(need_worked, column_order)

data <- rbind(assessed, need_worked) %>%
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
  data,
  "2021/2021permits_processed_legacy.csv",
  row.names = FALSE
)
