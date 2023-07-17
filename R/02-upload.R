#' Connect to Local PostgreSQL
#'
#' Open a SQL connection to local PSQL server
#'
#' @importFrom RPostgres Postgres dbConnect
#' @export
con_local <- function() {
  dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )
}

#' @importFrom dplyr filter mutate mutate_if left_join group_by distinct arrange ungroup pull
#' @importFrom stringr str_to_lower str_squish
#' @importFrom RPostgres dbSendQuery dbWriteTable dbDisconnect
#'     dbListTables
#' @importFrom DBI dbGetQuery dbReadTable
#' @importFrom janitor clean_names
#' @importFrom readr cols col_double col_integer col_character col_skip
#' @importFrom purrr map
#' @importFrom tidyr nest unnest
#' @importFrom rlang sym
convert_to_postgres <- function(t, yrs, raw_dir, raw_zip, years_to_update, classification) {
  messageline(yrs[t])

  con <- con_local()

  zip <- grep(paste0(yrs[t], "_freq-A"), raw_zip, value = T)

  al <- ifelse(classification == "hs", 6L, 5L)

  d2 <- read_csv(
    zip,
    col_types = cols(
      Classification = col_skip(),
      Year = col_integer(),
      Period = col_skip(),
      `Period Desc.` = col_skip(),
      `Aggregate Level` = col_integer(),
      `Is Leaf Code` = col_skip(),
      `Trade Flow Code` = col_skip(),
      `Trade Flow` = col_character(),
      `Reporter Code` = col_integer(),
      Reporter = col_character(),
      `Reporter ISO` = col_character(),
      `Partner Code` = col_integer(),
      Partner = col_character(),
      `Partner ISO` = col_character(),
      `Commodity Code` = col_character(),
      Commodity = col_character(),
      `Qty Unit Code` = col_integer(),
      `Qty Unit` = col_character(),
      Qty = col_double(),
      `Netweight (kg)` = col_double(),
      `Trade Value (US$)` = col_double(),
      Flag = col_skip()
    )
  ) %>%
    clean_names() %>%
    mutate_if(is.character, function(x) {
      str_to_lower(str_squish(x))
    }) %>%
    filter(
      !!sym("aggregate_level") == al
    )

  d2 <- d2 %>%
    group_by(!!sym("trade_flow")) %>%
    nest() %>%
    ungroup() %>%
    mutate(trade_flow = str_to_lower(str_squish(!!sym("trade_flow"))))

  tfs <- d2$trade_flow

  d2 <- d2 %>%
    ungroup() %>%
    select(!!sym("data")) %>%
    pull()

  names(d2) <- tfs

  gc()

  map(
    tfs,
    function(tf) {
      # tf <- tfs[1]
      # al <- 6
      table_name <- paste(gsub("-", "_", raw_dir), "tf", gsub("-", "_", tf), "al", al, sep = "_")

      message(table_name)

      d2[[tf]] <- d2[[tf]] %>%
        mutate(
          reporter_iso = unspecified(!!sym("reporter_iso")),
          partner_iso = unspecified(!!sym("partner_iso"))
        ) %>%
        rename(trade_value_usd = !!sym("trade_value_us")) %>%
        select(-!!sym("aggregate_level"))

      d2[[tf]] <- d2[[tf]] %>%
        arrange(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code"))

      # print(d2)

      dbSendQuery(con, sprintf("CREATE TABLE IF NOT EXISTS %s (
        	commodity_code text,
        	commodity text)", paste0(gsub("-", "_", raw_dir), "_commodities")))

      dbSendQuery(con, sprintf("CREATE TABLE IF NOT EXISTS %s (
        	country_iso text,
        	country_code int4,
        	country text)", paste0(gsub("-", "_", raw_dir), "_countries")))

      dbSendQuery(con, sprintf("CREATE TABLE IF NOT EXISTS %s (
        	qty_unit_code int4,
        	qty_unit text)", paste0(gsub("-", "_", raw_dir), "_units")))

      dbSendQuery(con, sprintf(
        "CREATE TABLE IF NOT EXISTS %s (
        	year int4,
        	reporter_iso text,
        	partner_iso text,
        	reporter_code int4,
        	partner_code int4,
        	commodity_code text,
        	qty_unit_code int4,
        	qty float8,
        	netweight_kg float8,
        	trade_value_usd float8)", table_name
      ))

      dbSendQuery(con, sprintf("DELETE FROM %s WHERE year=%s", table_name, yrs[t]))

      commodities <- d2[[tf]] %>%
        select(!!sym("commodity_code"), !!sym("commodity")) %>%
        distinct() %>%
        bind_rows(dbReadTable(con, paste0(gsub("-", "_", raw_dir), "_commodities"))) %>%
        distinct() %>%
        arrange(!!sym("commodity_code"))

      dbWriteTable(con, paste0(gsub("-", "_", raw_dir), "_commodities"), commodities, append = F, overwrite = T)

      rm(commodities)

      d2[[tf]] <- d2[[tf]] %>%
        select(-!!sym("commodity"))

      countries <- d2[[tf]] %>%
        select(!!sym("reporter_iso"), !!sym("reporter_code"), !!sym("reporter")) %>%
        distinct() %>%
        rename(
          country_iso = !!sym("reporter_iso"),
          country_code = !!sym("reporter_code"),
          country = !!sym("reporter")
        ) %>%
        bind_rows(
          d2[[tf]] %>%
            select(!!sym("partner_iso"), !!sym("partner_code"), !!sym("partner")) %>%
            distinct() %>%
            rename(
              country_iso = !!sym("partner_iso"),
              country_code = !!sym("partner_code"),
              country = !!sym("partner")
            )
        ) %>%
        distinct() %>%
        bind_rows(dbReadTable(con, paste0(gsub("-", "_", raw_dir), "_countries"))) %>%
        distinct() %>%
        arrange(!!sym("country_iso"))

      dbWriteTable(con, paste0(gsub("-", "_", raw_dir), "_countries"), countries, append = F, overwrite = T)

      rm(countries)

      d2[[tf]] <- d2[[tf]] %>%
        select(-!!sym("reporter"), -!!sym("partner"))

      units <- d2[[tf]] %>%
        select(!!sym("qty_unit_code"), !!sym("qty_unit")) %>%
        distinct() %>%
        bind_rows(dbReadTable(con, paste0(gsub("-", "_", raw_dir), "_units"))) %>%
        distinct() %>%
        arrange(!!sym("qty_unit_code"))

      dbWriteTable(con, paste0(gsub("-", "_", raw_dir), "_units"), units, append = F, overwrite = T)

      rm(units)

      d2[[tf]] <- d2[[tf]] %>%
        select(-!!sym("qty_unit"))

      dbWriteTable(
        con,
        table_name,
        d2[[tf]],
        append = TRUE,
        overwrite = FALSE
      )

      # here we remove the sub-table to free resources
      d2[[tf]] <- NULL

      gc()
    }
  )

  dbDisconnect(con)
  rm(d2)
  gc()
}