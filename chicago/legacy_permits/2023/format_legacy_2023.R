library(dplyr)
library(openxlsx)
library(tidyr)


# Read and select/rename columns
actionable_raw <- read.xlsx("2023/2023permits_processed_2.xlsx",
sheet = "Actionable") %>%
  select(
    "ID	PIN* [PARID]"                 = pin1,
    "Local Permit No.* [USER28]"      = "permit#",
    "Issue Date* [PERMDT]"            = issue_date,
    "Amount* [AMOUNT]"                = rounded.cost,
    "Notes [NOTE1]"                   = notes,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant Name* [USER21]" = contact_1_name,
    pin2,
    pin3,
    pin4,
    pin5,
    pin6,
    pin7,
    pin8,
    pin9,
    pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")

actionable <- actionable_raw %>%
  # pivot longer and replicate data for any pin_x which does not have NA value
  # to the ID PIN* [PARID] column
  pivot_longer(
    cols = starts_with("pin"),
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
  bind_rows(actionable_raw %>%
              select(-starts_with("pin"))) %>%
  distinct() %>%
  arrange(
    `Local Permit No.* [USER28]`,
    `ID PIN* [PARID]`
  )


# Read the "Assessed" sheet
assessed <- read.xlsx("2023/2023permits_processed_2.xlsx", sheet = "Assessed") %>%
    select("ID	PIN* [PARID]" = "pin1",
         "Local Permit No.* [USER28]" = "permit#",
         "Issue Date* [PERMDT]" = "issue_date",
         "Amount* [AMOUNT]" = "rounded.cost",
         "Applicant Street Address* [ADDR1]" = "address",
         "Applicant Name* [USER21]" = "contact_1_name",
         "pin2" = as.character(pin2),
         "pin3" = as.character(pin3),
         "pin4" = as.character(pin4),
         "pin5" = as.character(pin5),
         "pin6" = as.character(pin6),
         "pin7" = as.character(pin7),
         "pin8" = as.character(pin8),
         "pin9" = as.character(pin9),
         "pin10" = as.character(pin10)
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
  # pivot longer and replicate data for any pin_x which does not have NA value
  # to the ID PIN* [PARID] column
  pivot_longer(
    cols = starts_with("pin"),
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
  bind_rows(actionable_raw %>%
              select(-starts_with("pin"))) %>%
  distinct() %>%
  arrange(
    `Local Permit No.* [USER28]`,
    `ID PIN* [PARID]`
  )

assessed_raw <- read.xlsx("2023/2023permits_processed_2.xlsx", sheet = "Assessed")

assessed <- assessed_raw %>%
  select(
    "ID	PIN* [PARID]" = pin1,
    "Local Permit No.* [USER28]" = `permit#`,
    "Issue Date* [PERMDT]" = issue_date,
    "Amount* [AMOUNT]" = rounded.cost,
    "Applicant Street Address* [ADDR1]" = address,
    "Applicant Name* [USER21]" = contact_1_name,
    pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
  ) %>%
  mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL") %>%
  pivot_longer(
    cols = starts_with("pin"),
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
  bind_rows(assessed_raw %>%
              select(
                "ID	PIN* [PARID]" = pin1,
                "Local Permit No.* [USER28]" = `permit#`,
                "Issue Date* [PERMDT]" = issue_date,
                "Amount* [AMOUNT]" = rounded.cost,
                "Applicant Street Address* [ADDR1]" = address,
                "Applicant Name* [USER21]" = contact_1_name,
                pin2, pin3, pin4, pin5, pin6, pin7, pin8, pin9, pin10
              ) %>%
              mutate(`Applicant City, State, Zip* [ADDR3]` = "Chicago, IL")
            ) %>%
  distinct() %>%
  arrange(
    `Local Permit No.* [USER28]`,
    `ID PIN* [PARID]`
  )

column_order <- c("ID	PIN* [PARID]",
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

# Ensure all columns exist in both data frames
for (col in column_order) {
  if (!col %in% names(actionable)) actionable[[col]] <- NA
  if (!col %in% names(assessed)) assessed[[col]] <- NA
}

# Reorder columns
actionable <- actionable[, column_order, drop = FALSE]
assessed <- assessed[, column_order, drop = FALSE]

data <- rbind(actionable, assessed) %>%
  # Match to column order including rows which don't exist
  select(all_of(column_order)) %>%
  mutate(
    # remove - from PIN
  `ID	PIN* [PARID]` = gsub("-", "", `ID	PIN* [PARID]`),
  # If pin is 13 digits add leading 0
  `ID	PIN* [PARID]` = ifelse(nchar(`ID	PIN* [PARID]`) == 13, paste0("0", `ID	PIN* [PARID]`), `ID	PIN* [PARID]`),
  # If PIN is 10 digits add 4 final digits
  `ID	PIN* [PARID]` = ifelse(nchar(`ID	PIN* [PARID]`) == 10, paste0(`ID	PIN* [PARID]`, "0000"), `ID	PIN* [PARID]`),
  # If pin is 9 digits do both
  `ID	PIN* [PARID]` = ifelse(nchar(`ID	PIN* [PARID]`) == 9, paste0("0", `ID	PIN* [PARID]`, "0000"), `ID	PIN* [PARID]`)
) 

write.csv(data, "2023/2023permits_processed_legacy.csv", row.names = FALSE)
