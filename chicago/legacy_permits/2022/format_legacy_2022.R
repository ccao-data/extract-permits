library(dplyr)
library(openxlsx)
library(tidyr)
library(openxlsx)

source("helper.R")

crosswalk <- read.xlsx("crosswalk.xlsx") %>%
  filter(year == '2022') %>%
  select(meta_pin, original_pin) %>%
  mutate(meta_pin = as.character(meta_pin),
         original_pin = as.character(original_pin))

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
  left_join(crosswalk, by = c("PIN* [PARID]" = "original_pin")) %>%
  # Replace PIN* [PARID] with meta_pin from crosswalk only if it is not NA
  mutate(`PIN* [PARID]` = coalesce((meta_pin), (`PIN* [PARID]`))) %>%
  # Remove a pin which is valid 14 digits but all 0's
  filter(`PIN* [PARID]` != '00000000000000') %>%
  select(-meta_pin) %>%
  # There are duplicate pins in this script
 group_by(
    `PIN* [PARID]`,
    `Local Permit No.* [USER28]`
  ) %>%
  slice(1) %>%
  ungroup() %>%
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

openxlsx::write.xlsx(
  need_worked$upload,
  "2022/2022permits_processed_legacy_need_worked_upload.xlsx",
  rowNames = FALSE
)

openxlsx::write.xlsx(
  need_worked$need_review,
  "2022/2022permits_processed_legacy_need_worked_review.xlsx",
  rowNames = FALSE
)

openxlsx::write.xlsx(
  actionable$upload,
  "2022/2022permits_processed_legacy_actionable_upload.xlsx",
  rowNames = FALSE
)

openxlsx::write.xlsx(
  actionable$need_review,
  "2022/2022permits_processed_legacy_actionable_review.xlsx",
  rowNames = FALSE
)
