# Internal functions ----

messageline <- function(txt = NULL, width = 80) {
  if(is.null(txt)) {
    message(rep("-", width))
  } else {
    message(txt, " ", rep("-", width - nchar(txt) - 1))
  }
}

#' @importFrom utils menu
check_token <- function() {
  has_token <- menu(
    c("yes", "no"),
    title = "Have you safely stored COMTRADE_TOKEN in .Renviron?",
    graphics = F
  )

  stopifnot(has_token == 1)
}

#' @importFrom utils download.file
download_files <- function(download_links, parallel) {
  if (Sys.info()[['sysname']] != "Windows") {
    if (isTRUE(parallel)) {
      messageline("Downloading files in parallel...")
      base_command <- "wget --continue --retry-connrefused --no-http-keep-alive --tries=0 --timeout=180 -O %s %s"
      writeLines(sprintf(base_command, download_links$new_file, download_links$url), "commands.txt")
      system("parallel --jobs 4 < commands.txt")
      unlink("commands.txt")
    } else {
      messageline("Downloading files sequentially...")
      lapply(seq_along(download_links$url),
             function(x) {
               base_command <- "wget --continue --retry-connrefused --no-http-keep-alive --tries=0 --timeout=180 -O %s %s"
               base_command <- sprintf(base_command, download_links$new_file[[x]], download_links$url[[x]])
               if (file.exists(download_links$new_file[[x]])) {
                 return(TRUE)
               } else {
                 system(base_command)
               }
             })
    }
  } else {
    messageline("Windows detected, downloading files sequentially...")
    lapply(seq_along(download_links$url),
           function(x) {
             if (file.exists(download_links$new_file[[x]])) {
               return(TRUE)
             } else {
               download.file(
                 url = download_links$url[[x]],
                 destfile = download_links$new_file[[x]],
                 method = "auto"
               )
             }
           })
  }
}

#' @importFrom stringr str_replace
extract <- function(x, y) {
  if (file.exists(str_replace(x, "zip", "csv"))) {
    messageline()
    message(paste(x, "already unzipped. Skipping."))
  } else {
    messageline()
    message(paste("Unzipping", x))
    system(sprintf("7z e -aos %s -oc:%s", x, y))
  }
}

#' @importFrom dplyr case_when
unspecified <- function(x) {
  case_when(
    x %in% c(NA, "") ~ "0-unspecified",
    TRUE ~ x
  )
}

file_remove <- function(x) {
  try(file.remove(x))
}

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
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )
}

#' @importFrom dplyr filter mutate mutate_if left_join group_by distinct arrange
#' @importFrom stringr str_to_lower str_squish
#' @importFrom RPostgres dbSendQuery dbWriteTable dbDisconnect
#'     dbListTables
#' @importFrom DBI dbGetQuery dbReadTable
#' @importFrom janitor clean_names
#' @importFrom readr cols col_double col_integer col_character col_skip
#' @importFrom purrr map
#' @importFrom rlang sym
convert_to_postgres <- function(t, yrs, raw_dir, raw_zip, years_to_update) {
  messageline(yrs[t])

  con <- con_local()

  tables <- dbListTables(con)
  tables <- grep(paste(gsub("-", "_", raw_dir), "tf_import_al_0", sep = "_"),
                 tables, value = T)
  rows_in_db <- rep(NA, length(tables))
  for (i in seq_along(tables)) {
    rows_in_db[i] <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) as year FROM %s WHERE year = %s", tables[[i]], yrs[t])))
  }

  rows_in_db <- min(rows_in_db)
  if (is.infinite(rows_in_db)) { rows_in_db <- 0 }

  if ((!yrs[t] %in% years_to_update) | (rows_in_db > 0)) {
    dbDisconnect(con)
    return(TRUE)
  }

  zip <- grep(paste0(yrs[t], "_freq-A"), raw_zip, value = T)

  csv <- zip %>%
    str_replace("/zip/", "/") %>%
    str_replace("zip$", "csv")

  extract(zip, raw_dir)

  d <- read_csv(
    csv,
    col_types = cols(
      Classification = col_skip(),
      Year = col_skip(),
      Period = col_skip(),
      `Period Desc.` = col_skip(),
      `Aggregate Level` = col_integer(),
      `Is Leaf Code` = col_skip(),
      `Trade Flow Code` = col_skip(),
      `Trade Flow` = col_character(),
      `Reporter Code` = col_skip(),
      Reporter = col_skip(),
      `Reporter ISO` = col_skip(),
      `Partner Code` = col_skip(),
      Partner = col_skip(),
      `Partner ISO` = col_skip(),
      `Commodity Code` = col_skip(),
      Commodity = col_skip(),
      `Qty Unit Code` = col_skip(),
      `Qty Unit` = col_skip(),
      Qty = col_skip(),
      `Netweight (kg)` = col_skip(),
      `Trade Value (US$)` = col_skip(),
      Flag = col_skip())
  ) %>%
    clean_names() %>%
    mutate_if(is.character, function(x) { str_to_lower(str_squish(x)) }) %>%
    distinct(!!sym("trade_flow"), !!sym("aggregate_level"))

  d <- d %>%
    filter(!!sym("aggregate_level") %in% 5:6)

  map2(
    d$trade_flow, d$aggregate_level,
    function(tf,al) {
      table_name <- paste(gsub("-", "_", raw_dir), "tf", gsub("-", "_", tf), "al", al, sep = "_")

      message(table_name)

      d2 <- read_csv(
        csv,
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
          Flag = col_skip())
      ) %>%
        clean_names()

      d2 <- d2 %>%
        mutate_if(is.character, function(x) { str_to_lower(str_squish(x)) }) %>%
        filter(
          !!sym("trade_flow") == tf,
          !!sym("aggregate_level") == al
        ) %>%
        select(-!!sym("trade_flow"), -!!sym("aggregate_level"))

      gc()

      d2 <- d2 %>%
        rename(trade_value_usd = !!sym("trade_value_us")) %>%
        mutate(
          reporter_iso = unspecified(!!sym("reporter_iso")),
          partner_iso = unspecified(!!sym("partner_iso"))
        )

      d2 <- d2 %>%
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
        	trade_value_usd float8)", table_name))

      dbSendQuery(con, sprintf("DELETE FROM %s WHERE year=%s", table_name, yrs[t]))

      commodities <- d2 %>%
        select(!!sym("commodity_code"), !!sym("commodity")) %>%
        distinct() %>%
        bind_rows(dbReadTable(con, paste0(gsub("-", "_", raw_dir), "_commodities"))) %>%
        distinct() %>%
        arrange(!!sym("commodity_code"))

      dbWriteTable(con, paste0(gsub("-", "_", raw_dir), "_commodities"), commodities, append = F, overwrite = T)

      rm(commodities)

      d2 <- d2 %>%
        select(-!!sym("commodity"))

      countries <- d2 %>%
        select(!!sym("reporter_iso"), !!sym("reporter_code"), !!sym("reporter")) %>%
        distinct() %>%
        rename(
          country_iso = !!sym("reporter_iso"),
          country_code = !!sym("reporter_code"),
          country = !!sym("reporter")
        ) %>%
        bind_rows(
          d2 %>%
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

      d2 <- d2 %>%
        select(-!!sym("reporter"), -!!sym("partner"))

      units <- d2 %>%
        select(!!sym("qty_unit_code"), !!sym("qty_unit")) %>%
        distinct() %>%
        bind_rows(dbReadTable(con, paste0(gsub("-", "_", raw_dir), "_units"))) %>%
        distinct() %>%
        arrange(!!sym("qty_unit_code"))

      dbWriteTable(con, paste0(gsub("-", "_", raw_dir), "_units"), units, append = F, overwrite = T)

      rm(units)

      d2 <- d2 %>%
        select(-!!sym("qty_unit"))

      dbWriteTable(
        con,
        table_name,
        d2,
        append = TRUE,
        overwrite = FALSE
      )

      gc()
    }
  )

  dbDisconnect(con)
  file_remove(csv); rm(d); gc()
}

#' @importFrom RPostgres Postgres dbConnect dbSendQuery dbListTables dbDisconnect
create_indexes_postgres <- function(raw_dir) {
  message("rebuilding indexes...")
  con <- con_local()
  tables <- dbListTables(con)
  tables <- grep(gsub("-", "_", raw_dir), tables, value = T)
  tables <- grep("export|import", tables, value = T)

  for (table_name in tables) {
    message(table_name)
    dbSendQuery(con, sprintf("DROP INDEX IF EXISTS %s_year", table_name))
    dbSendQuery(con, sprintf("DROP INDEX IF EXISTS %s_reporter_iso", table_name))
    dbSendQuery(con, sprintf("CREATE INDEX %s_year ON %s (year)", table_name, table_name))
    dbSendQuery(con, sprintf("CREATE INDEX %s_reporter_iso ON %s (reporter_iso)", table_name, table_name))
  }

  dbDisconnect(con)
}

# Mega function to download data ----

#' A wrapper to GNU Parallel and wget for faster downloads
#'
#' COMPLETE DESC...
#'
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr select arrange pull rename tibble bind_rows distinct group_by summarise
#' @importFrom readr read_csv write_csv
#' @importFrom stringr str_replace_all
#' @importFrom utils menu
#' @importFrom rlang sym
#'
#' @param postgres Set to `FALSE` to just download the datasets without
#'     converting to PostgreSQL
#' @param token parameter for non-interactive calls, otherwise it shows a prompt
#' @param dataset parameter for non-interactive calls, otherwise it shows a prompt
#' @param remove_old_files parameter for non-interactive calls, otherwise it shows a prompt
#' @param subset_years parameter for non-interactive calls, otherwise it shows a prompt
#' @param parallel parameter for non-interactive calls, otherwise it shows a prompt
#' @param subdir parameter to download in a sub-directory such as `"finp"`, etc.
#'
#' @export
data_downloading <- function(postgres = F, token = NULL, dataset = NULL, remove_old_files = NULL,
                             subset_years = NULL, parallel = NULL, subdir = NULL, skip_updates = F) {
  if (is.null(token)) { check_token() }

  # download ----
  if (is.null(dataset)) {
    dataset <- menu(
      c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "HS rev 2012",
        "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
      title = "Select dataset:",
      graphics = F
    )
  }

  if (is.null(remove_old_files)) {
    remove_old_files <- menu(
      c("yes", "no"),
      title = "Remove old files (y/n):",
      graphics = F
    )
  }

  if (is.null(subset_years)) {
    subset_years <- readline(prompt = "Years to download (i.e. `2000:2020`, hit enter to download all available data): ")
    subset_years <- as.numeric(unlist(strsplit(subset_years, ":")))
  }

  if (is.null(parallel)) {
    parallel <- menu(
      c("yes", "no"),
      title = "Download in parallel (even when parallel is faster, with UN COMTRADE is better to download files 1-by-1 at 300-500 kb/s and wait):",
      graphics = F
    )
  }

  classification <- ifelse(dataset < 6, "hs", "sitc")

  revision <- switch (
    dataset,
    `1` = 1992,
    `2` = 1996,
    `3` = 2002,
    `4` = 2007,
    `5` = 2012,
    `6` = 1,
    `7` = 2,
    `8` = 3,
    `9` = 4
  )

  revision2 <- switch (
    dataset,
    `1` = 1988,
    `2` = 1996,
    `3` = 2002,
    `4` = 2007,
    `5` = 2012,
    `6` = 1962,
    `7` = 1976,
    `8` = 1988,
    `9` = 2007
  )

  classification2 <- switch (
    dataset,
    `1` = "H0",
    `2` = "H1",
    `3` = "H2",
    `4` = "H3",
    `5` = "H4",
    `6` = "S1",
    `7` = "S2",
    `8` = "S3",
    `9` = "S4"
  )

  max_year <- 2020

  years <- revision2:max_year
  if(length(subset_years) > 0) {
    years <- years[years >= min(subset_years) & years <= max(subset_years)]
  }

  if (is.null(subdir)) {
    raw_dir <- sprintf("%s-rev%s", classification, revision)
  } else {
    raw_dir <- paste0(subdir, "/", sprintf("%s-rev%s", classification, revision))
  }
  try(dir.create(raw_dir))

  raw_dir_zip <- sprintf("%s/%s", raw_dir, "zip")
  try(dir.create(raw_dir_zip))

  raw_dir_parquet <- str_replace(raw_dir_zip, "zip", "parquet")
  try(dir.create(raw_dir_parquet))

  try(
    old_file <- max(
      list.files(raw_dir, pattern = "downloaded-files.*csv", full.names = T), na.rm = T)
  )

  if (isTRUE(nchar(old_file) > 0)) {
    old_download_links <- read_csv(old_file) %>%
      mutate(
        local_file_date = as.Date(gsub("_fmt.*", "", gsub(".*pub-", "", file)), "%Y%m%d")
      ) %>%
      rename(old_file = file)
  }

  download_links <- tibble(
    year = years,
    url = paste0(
      "https://comtrade.un.org/api/get/bulk/C/A/",
      !!sym("year"),
      "/ALL/",
      classification2,
      "?token=",
      Sys.getenv("COMTRADE_TOKEN")
    ),
    file = NA
  )

  files <- fromJSON(sprintf(
    "https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s",
    classification2,
    Sys.getenv("COMTRADE_TOKEN"))) %>%
    filter(!!sym("ps") %in% years) %>%
    arrange(!!sym("ps"))

  if (exists("old_download_links")) {
    download_links <- download_links %>%
      mutate(
        file = paste0(raw_dir_zip, "/", files$name),
        server_file_date = as.Date(gsub("_fmt.*", "", gsub(".*pub-", "", file)), "%Y%m%d")
      ) %>%
      left_join(old_download_links %>% select(-url), by = "year") %>%
      rename(new_file = file) %>%
      mutate(
        server_file_date = as.Date(
          ifelse(is.na(!!sym("local_file_date")), !!sym("server_file_date") + 1, !!sym("server_file_date")),
          origin = "1970-01-01"
        ),
        local_file_date = as.Date(
          ifelse(is.na(!!sym("local_file_date")), !!sym("server_file_date") - 1, !!sym("local_file_date")),
          origin = "1970-01-01"
        )
      )
  } else {
    download_links <- download_links %>%
      mutate(
        file = paste0(raw_dir_zip, "/", files$name),

        server_file_date = as.Date(gsub("_fmt.*", "", gsub(".*pub-", "", file)), "%Y%m%d"),

        # trick in case there are no old files
        old_file = NA,

        local_file_date = !!sym("server_file_date"),
        server_file_date = as.Date(!!sym("server_file_date") + 1, origin = "1970-01-01")
      ) %>%
      rename(new_file = file)
  }

  if (isFALSE(skip_updates)) {
    files_to_update <- download_links %>%
      filter(!!sym("local_file_date") < !!sym("server_file_date"))

    files_to_update_2 <- download_links %>%
      mutate(file_exists = file.exists(!!sym("new_file"))) %>%
      filter(!!sym("file_exists") == FALSE)

    files_to_update <- files_to_update %>%
      bind_rows(files_to_update_2)

    years_to_update <- files_to_update$year

    if (length(years_to_update) > 0) {
      download_files(download_links, parallel = parallel)
    }
  }

  if (remove_old_files == 1) {
    files_to_remove <- list.files(raw_dir_zip,
                                  pattern = paste(paste0("ps-",years), collapse = "|"),
                                  full.names = T)
    files_to_remove <- data.frame(
      file = files_to_remove,
      year = gsub("_freq.*", "", gsub(".*_ps-", "", files_to_remove))
    ) %>%
      group_by(!!sym("year")) %>%
      filter(!!sym("file") != max(!!sym("file"))) %>%
      pull(!!sym("file"))
    lapply(files_to_remove, file_remove)
  }

  download_links <- download_links %>%
    select(!!sym("year"), !!sym("url"), !!sym("new_file"), !!sym("local_file_date")) %>%
    rename(file = !!sym("new_file"))

  download_links <- download_links %>%
    mutate(url = str_replace_all(url, "token=.*", "token=REPLACE_TOKEN"))

  if (length(years_to_update) > 0) {
    write_csv(download_links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))

    write_csv(
      download_links %>% filter(!!sym("year") %in% years_to_update),
      paste0(raw_dir, "/updated-files-", Sys.Date(), ".csv")
    )
  }

  if (classification == "sitc") {
    aggregations <- 0:5
  } else {
    aggregations <- c(0,2,4,6)
  }

  raw_zip <- list.files(
    path = raw_dir_zip,
    pattern = "\\.zip",
    full.names = T
  )

  # convert to postgres ----

  if (isTRUE(postgres)) {
    lapply(seq_along(years), convert_to_postgres, yrs = years, raw_dir, raw_zip, years)
    create_indexes_postgres(raw_dir)
  }
}
