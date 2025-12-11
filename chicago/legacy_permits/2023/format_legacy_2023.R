library(dplyr)
library(openxlsx)
library(tidyr)

source("legacy_permits/helper.R")

actionable_raw <- read_xlsx_all_char(
  "legacy_permits/2023/2023permits_processed_2.xlsx",
  "Actionable"
) %>%
  select(
    "ID	PIN* [PARID]"                   = pin1,
    "Local Permit No.* [USER28]"        = `permit#`,
    "Issue Date* [PERMDT]"              = issue_date,
    "Amount* [AMOUNT]"                  = rounded.cost,
    "Notes [NOTE1]"                     = notes,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant Name* [USER21]"          = contact_1_name,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

actionable <- expand_pins(actionable_raw)

need_worked_raw <- read_xlsx_all_char(
  "legacy_permits/2023/2023permits_processed_2.xlsx",
  "Need worked"
) %>%
  select(
    "ID	PIN* [PARID]"                   = pin1,
    "Local Permit No.* [USER28]"        = `permit#`,
    "Issue Date* [PERMDT]"              = issue_date,
    "Amount* [AMOUNT]"                  = rounded.cost,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant Name* [USER21]"          = contact_1_name,
    "Notes [NOTE1]"                     = notes,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

need_worked <- expand_pins(need_worked_raw)

actionable <- ensure_columns(actionable, column_order)
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
  "legacy_permits/2023/2023permits_processed_legacy_need_worked.csv",
  row.names = FALSE
)

write.csv(
  actionable,
  "legacy_permits/2023/2023permits_processed_legacy_actionable.csv",
  row.names = FALSE
  )
