library(noctua)
library(DBI)
library(glue)
library(dplyr)

find_missing_pins <- function(dataset, conn, year) {
  pins_local <- dataset %>%
    select(`PIN* [PARID]`) %>%
    distinct() %>%
    pull(`PIN* [PARID]`) %>%
    as.character()

  pins_sql <- paste(
    sprintf("('%s')", pins_local),
    collapse = ", "
  )

  query <- glue("
    WITH local_pins (meta_pin) AS (
      VALUES
        {pins_sql}
    )
    SELECT
      lp.meta_pin,
      v.year
    FROM local_pins lp
    LEFT JOIN default.vw_pin_universe v
      ON lp.meta_pin = v.pin
      AND v.year = year
    WHERE v.pin IS NULL
  ")

  dbGetQuery(conn, query)
}

conn <- dbConnect(
  noctua::athena(),
  rstudio_conn_tab = FALSE
)

# Run each relevant script before using the different years
missing_pins_2021_need_worked <- find_missing_pins(
  need_worked$upload,
  conn, "2021"
) %>%
  mutate(
    year = "2021",
    tab = "need_worked"
  )

missing_pins_2022_need_worked <- find_missing_pins(
  need_worked$upload,
  conn, "2022"
) %>%
  mutate(
    year = "2022",
    tab = "need_worked"
  )

missing_pins_2022_actionable <- find_missing_pins(
  actionable$upload,
  conn, "2022"
) %>%
  mutate(
    year = "2022",
    tab = "actionable"
  )

missing_pins_2023_need_worked <- find_missing_pins(
  need_worked$upload,
  conn, "2023"
) %>%
  mutate(
    year = "2023",
    tab = "need_worked"
  )

missing_pins_2023_actionable <- find_missing_pins(
  actionable$upload,
  conn, "2023"
) %>%
  mutate(
    year = "2023",
    tab = "actionable"
  )

all_missing_pins <- rbind(
  missing_pins_2021_need_worked,
  missing_pins_2022_need_worked,
  missing_pins_2022_actionable,
  missing_pins_2023_need_worked,
  missing_pins_2023_actionable
)
write.csv(all_missing_pins, "legacy_pins_to_check.csv",
  row.names = FALSE
)
