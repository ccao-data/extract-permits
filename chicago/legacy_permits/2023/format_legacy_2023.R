library(dplyr)
library(openxlsx)
library(tidyr)
library(openxlsx)

source("helper.R")

crosswalk <- read.xlsx("crosswalk.xlsx") %>%
  filter(year == "2023") %>%
  select(meta_pin, original_pin) %>%
  mutate(
    meta_pin = as.character(meta_pin),
    original_pin = as.character(original_pin)
  )

actionable <- read_xlsx_all_char(
  "2023/2023permits_processed_2.xlsx",
  "Actionable"
) %>%
  select(
    "PIN* [PARID]" = pin1,
    "Local Permit No.* [USER28]" = `permit#`,
    "Issue Date* [PERMDT]" = issue_date,
    "Amount* [AMOUNT]" = rounded.cost,
    "Notes [NOTE1]" = notes,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant* [USER21]" = contact_1_name,
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
  left_join(crosswalk, by = c("PIN* [PARID]" = "original_pin")) %>%
  # Replace PIN* [PARID] with meta_pin from crosswalk only if it is not NA
  mutate(`PIN* [PARID]` = coalesce((meta_pin), (`PIN* [PARID]`))) %>%
  select(-meta_pin) %>%
  group_by(
    `PIN* [PARID]`,
    `Local Permit No.* [USER28]`
  ) %>%
  slice(1) %>%
  ungroup() %>%
  finalize_columns(needed_columns)

need_worked <- read_xlsx_all_char(
  "2023/2023permits_processed_2.xlsx",
  "Need worked"
) %>%
  select(
    "PIN* [PARID]" = pin1,
    "Local Permit No.* [USER28]" = `permit#`,
    "Issue Date* [PERMDT]" = issue_date,
    "Amount* [AMOUNT]" = rounded.cost,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant* [USER21]" = contact_1_name,
    "Notes [NOTE1]" = notes,
    "Reinstated" = Reinstated.permit,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(
    `Applicant City, State, Zip* [ADDR3]` = "CHICAGO, IL",
    # if reinstated is not NA replace the value for Notes with it
    `Notes [NOTE1]` = ifelse(!is.na(`Reinstated`),
      `Reinstated`, `Notes [NOTE1]`
    )
  ) %>%
  select(-c(`Reinstated`)) %>%
  expand_pins() %>%
  ensure_columns(column_order) %>%
  mutate(
    `PIN* [PARID]` = normalize_pin(`PIN* [PARID]`),
    `Issue Date* [PERMDT]` = format(
      as.Date(as.numeric(`Issue Date* [PERMDT]`), origin = "1899-12-30"),
      "%m/%d/%Y"
    )
  ) %>%
  left_join(crosswalk, by = c("PIN* [PARID]" = "original_pin")) %>%
  # Replace PIN* [PARID] with meta_pin from crosswalk only if it is not NA
  mutate(`PIN* [PARID]` = coalesce((meta_pin), (`PIN* [PARID]`))) %>%
  select(-meta_pin) %>%
  group_by(
    `PIN* [PARID]`,
    `Local Permit No.* [USER28]`
  ) %>%
  slice(1) %>%
  ungroup() %>%
  finalize_columns(needed_columns)

write.xlsx(
  need_worked$upload,
  "2023/2023permits_processed_legacy_need_worked_upload.xlsx",
  rowNames = FALSE
)

write.xlsx(
  need_worked$need_review,
  "2023/2023permits_processed_legacy_need_worked_review.xlsx",
  rowNames = FALSE
)

write.xlsx(
  actionable$upload,
  "2023/2023permits_processed_legacy_actionable_upload.xlsx",
  rowNames = FALSE
)

write.xlsx(
  actionable$need_review,
  "2023/2023permits_processed_legacy_actionable_review.xlsx",
  rowNames = FALSE
)
