library(dplyr)
library(openxlsx)
library(tidyr)


column_order <- c(
  "ID PIN* [PARID]",
  "Local Permit No.* [USER28]",
  "Issue Date* [PERMDT]",
  "Desc 1* [DESC1]",
  "Desc 2 Code 1 [USER6]",
  "Desc 2 Code 1 [USER6]2",
  "Desc 2 Code 2 [USER7]",
  "Desc 2 Code 3 [USER8]",
  "Amount* [AMOUNT]",
  "Assessable [IS_ASSESS]",
  "Applicant Street Address* [ADDR1]",
  "Applicant Address 2 [ADDR2]",
  "SUFFIX",
  "Applicant City, State, Zip* [ADDR3]",
  "Contact Phone* [PHONE]",
  "Applicant* [USER21]",
  "Notes [NOTE1]",
  "Occupy Dt [UDATE1]",
  "Submit Dt* [CERTDATE]",
  "Est Comp Dt [UDATE2]"
)


# Read the "Assessed" sheet
assessed_raw <- read.xlsx("2021/2021 manual review processed- JW completed (1).xlsx", sheet = "Assessed") %>%
  mutate(
    `Applicant Street Address* [ADDR1]` = paste(STREET_NUMBER, STREET.DIRECTION, STREET_NAME, SUFFIX)
  ) %>%
  select(
    "ID PIN* [PARID]" = PIN1,
    "Local Permit No.* [USER28]" = "PERMIT#",
    "Issue Date* [PERMDT]" = ISSUE_DATE,
    "Amount* [AMOUNT]" = REPORTED_COST,
    "Applicant Street Address* [ADDR1]",
    "Applicant Name* [USER21]" = CONTACT_1_NAME,
    "Notes [NOTE1]" = WORK_DESCRIPTION,
    "PIN2" = PIN2,
    "PIN3" = PIN3,
    "PIN4" = PIN4,
    "PIN5" = PIN5,
    "PIN6" = PIN6,
    "PIN7" = PIN7
  )

assessed <- assessed_raw %>%
  # Remove all - from any column which contains PIN
  mutate(
    across(starts_with("PIN"), ~ gsub("-", "", .)),
    `ID PIN* [PARID]` = gsub("-", "", `ID PIN* [PARID]`)
  ) %>%
  # pivot longer and replicate data for any pin_x which does not have NA value
  pivot_longer(
    cols = starts_with("PIN"),
    names_to  = "pin_col",
    values_to = "extra_pin",
    values_drop_na = TRUE
  ) %>%
  mutate(
    `ID PIN* [PARID]` = extra_pin
  ) %>%
  select(
    -pin_col,
    -extra_pin
  ) %>%
  # Stack the original pin1 rows with the extra-pin rows
  bind_rows(
    assessed_raw %>%
      select(-starts_with("PIN"))
  ) %>%
  distinct() %>%
  arrange(
    `Local Permit No.* [USER28]`,
    `ID PIN* [PARID]`
  ) %>%
  mutate(
    `Applicant City, State, Zip* [ADDR3]` = "Chicago, IL"
  )

# Add missing columns as NA
for (col in column_order) {
  if (!col %in% names(assessed)) assessed[[col]] <- NA
}

# Now select columns in the specified order
data <- assessed %>%
  select(all_of(column_order)) %>%
  mutate(
    # Remove - from `ID PIN* [PARID]`
    `ID PIN* [PARID]` = gsub("-", "", `ID PIN* [PARID]`),
    `ID PIN* [PARID]` = ifelse(nchar(`ID PIN* [PARID]`) == 13, paste0("0", `ID PIN* [PARID]`), `ID PIN* [PARID]`),
    `ID PIN* [PARID]` = ifelse(nchar(`ID PIN* [PARID]`) == 10, paste0(`ID PIN* [PARID]`, "0000"), `ID PIN* [PARID]`),
    `ID PIN* [PARID]` = ifelse(nchar(`ID PIN* [PARID]`) == 9,  paste0("0", `ID PIN* [PARID]`, "0000"), `ID PIN* [PARID]`)
  )

write.csv(data, "2021/2021permits_processed_legacy.csv", row.names = FALSE)
