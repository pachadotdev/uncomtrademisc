# Internal functions ----

messageline <- function(txt = NULL, width = 80) {
  if (is.null(txt)) {
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
  if (Sys.info()[["sysname"]] != "Windows") {
    if (isTRUE(parallel)) {
      messageline("Downloading files in parallel...")
      base_command <- "wget --continue --retry-connrefused --no-http-keep-alive --tries=0 --timeout=180 -O %s %s"
      writeLines(sprintf(base_command, download_links$new_file, download_links$url), "commands.txt")
      system("parallel --jobs 4 < commands.txt")
      unlink("commands.txt")
    } else {
      messageline("Downloading files sequentially...")
      lapply(
        seq_along(download_links$url),
        function(x) {
          base_command <- "wget --continue --retry-connrefused --no-http-keep-alive --tries=0 --timeout=180 -O %s %s"
          base_command <- sprintf(base_command, download_links$new_file[[x]], download_links$url[[x]])
          if (file.exists(download_links$new_file[[x]])) {
            return(TRUE)
          } else {
            system(base_command)
          }
        }
      )
    }
  } else {
    messageline("Windows detected, downloading files sequentially...")
    lapply(
      seq_along(download_links$url),
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
      }
    )
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

#' @importFrom RPostgres Postgres dbConnect dbSendQuery dbListTables dbDisconnect
create_indexes_postgres <- function(raw_dir) {
  message("creating indexes...")
  con <- con_local()
  tables <- dbListTables(con)
  tables <- grep(gsub("-", "_", raw_dir), tables, value = T)
  tables <- grep("export|import", tables, value = T)

  for (table_name in tables) {
    message(table_name)

    # drop primary indexes
    dbSendQuery(con, sprintf("DROP INDEX IF EXISTS %s_year", table_name))
    dbSendQuery(con, sprintf("DROP INDEX IF EXISTS %s_reporter", table_name))
    dbSendQuery(con, sprintf("DROP INDEX IF EXISTS %s_partner", table_name))

    # create primary indexes
    dbSendQuery(con, sprintf("CREATE INDEX %s_year ON %s (year)", table_name, table_name))
    dbSendQuery(con, sprintf("CREATE INDEX %s_reporter ON %s (reporter_iso)", table_name, table_name))
    dbSendQuery(con, sprintf("CREATE INDEX %s_partner ON %s (partner_iso)", table_name, table_name))

    coun <- gsub("tf_.*", "countries", table_name)
    comm <- gsub("tf_.*", "commodities", table_name)
    unit <- gsub("tf_.*", "units", table_name)
    rev <- gsub("_tf.*", "", table_name)

    # drop foreign keys
    dbSendQuery(con, sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_country", table_name, rev))
    dbSendQuery(con, sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_commodity", table_name, rev))
    dbSendQuery(con, sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s_unit", table_name, rev))

    # reference keys
    dbSendQuery(con, sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s_country ON %s (country_iso, country_code)", rev, coun))
    dbSendQuery(con, sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s_commodity ON %s (commodity_code)", rev, comm))
    dbSendQuery(con, sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s_unit ON %s (qty_unit_code)", rev, unit))

    # create foreign keys
    dbSendQuery(con, sprintf("ALTER TABLE %s ADD FOREIGN KEY (reporter_iso, reporter_code) REFERENCES %s (country_iso, country_code)", table_name, coun))
    dbSendQuery(con, sprintf("ALTER TABLE %s ADD FOREIGN KEY (partner_iso, partner_code) REFERENCES %s (country_iso, country_code)", table_name, coun))
    dbSendQuery(con, sprintf("ALTER TABLE %s ADD FOREIGN KEY (commodity_code) REFERENCES %s (commodity_code)", table_name, comm))
    dbSendQuery(con, sprintf("ALTER TABLE %s ADD FOREIGN KEY (qty_unit_code) REFERENCES %s (qty_unit_code)", table_name, unit))
  }

  dbDisconnect(con)
}

# Mega function to download data ----

#' A wrapper to GNU Parallel and wget for faster downloads
#'
#' COMPLETE DESC...
#'
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr select arrange pull rename tibble bind_rows distinct group_by summarise filter
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
#' @param skip_updates parameter to skip checking for updates
#'
#' @export
data_downloading <- function(postgres = F, token = NULL, dataset = NULL, remove_old_files = NULL,
                             subset_years = NULL, parallel = NULL, subdir = NULL, skip_updates = F) {
  if (is.null(token)) {
    check_token()
  }

  # download ----
  if (is.null(dataset)) {
    dataset <- menu(
      c(
        "HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007",
        "HS rev 2012", "HS rev 2017",
        "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4", "EB 02"
      ),
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
    subset_years <- readline(prompt = "Years to download (i.e. `2000:2021`, hit enter to download all available data): ")
    subset_years <- as.numeric(unlist(strsplit(subset_years, ":")))
  }

  if (is.null(parallel)) {
    parallel <- menu(
      c("yes", "no"),
      title = "Download in parallel (even when parallel is faster, with UN COMTRADE is better to download files 1-by-1 at 300-500 kb/s and wait):",
      graphics = F
    )
  }

  classification <- ifelse(dataset < 7, "hs",
    ifelse(dataset < 11, "sitc",
      ifelse(dataset == 11, "bec", "eb")
    ))

  revision <- switch(dataset,
    `1` = 1992,
    `2` = 1996,
    `3` = 2002,
    `4` = 2007,
    `5` = 2012,
    `6` = 2017,
    `7` = 1,
    `8` = 2,
    `9` = 3,
    `10` = 4,
    `11` = "",
    `12` = "02"
  )

  revision2 <- switch(dataset,
    `1` = 1988,
    `2` = 1996,
    `3` = 2002,
    `4` = 2007,
    `5` = 2012,
    `6` = 2017,
    `7` = 1962,
    `8` = 1976,
    `9` = 1988,
    `10` = 2007,
    `11` = 1993,
    `12` = 2000
  )

  if (dataset == 11) {
    message("Unfortunately, BEC data for 1991-1992 is missing in the API.")
  }

  classification2 <- switch(dataset,
    `1` = "H0",
    `2` = "H1",
    `3` = "H2",
    `4` = "H3",
    `5` = "H4",
    `6` = "H5",
    `7` = "S1",
    `8` = "S2",
    `9` = "S3",
    `10` = "S4",
    `11` = "BEC",
    `12` = "EB02"
  )

  max_year <- 2021

  years <- revision2:max_year
  if (length(subset_years) > 0) {
    years <- years[years >= min(subset_years) & years <= max(subset_years)]
  }

  raw_dir <- sprintf("%s-rev%s", classification, revision)

  if (classification == "bec") {
    raw_dir <- classification
  }

  if (!is.null(subdir)) {
    raw_dir <- paste0(subdir, "/", raw_dir)
  }
  
  try(dir.create(raw_dir))

  raw_dir_zip <- sprintf("%s/%s", raw_dir, "zip")
  try(dir.create(raw_dir_zip))

  try(
    old_file <- max(
      list.files(raw_dir, pattern = "downloaded-files.*csv", full.names = T),
      na.rm = T
    )
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
      ifelse(classification == "eb",
        "https://comtrade.un.org/api/get/bulk/S/A/",
        "https://comtrade.un.org/api/get/bulk/C/A/"),
      !!sym("year"),
      "/ALL/",
      classification2,
      "?token=",
      Sys.getenv("COMTRADE_TOKEN")
    ),
    file = NA
  )

  files_url <- sprintf(
    "https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&token=%s",
    Sys.getenv("COMTRADE_TOKEN")
  )

  finp <- "download_links.json"

  if (!file.exists(finp)) {
    # SSL error
    # download.file(files_url, download_links_json, method = "wget")

    # use system to download with wget --no-check-certificate
    system(
      paste("wget -O", finp, "--no-check-certificate", files_url)
    )
  }

  files <- fromJSON(finp) %>%
    filter(
      !!sym("ps") %in% years,
      grepl("r-ALL", !!sym("name")),
      grepl(paste0("px-", classification2), !!sym("name"))
    ) %>%
    arrange(!!sym("ps")) %>%
    group_by(!!sym("ps")) %>%
    filter(!!sym("publicationDate") == max(!!sym("publicationDate")))

  if (exists("old_download_links") & isFALSE(skip_updates)) {
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
    if (isFALSE(skip_updates)) {
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
    } else {
      download_links <- old_download_links
    }
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
      pattern = paste(paste0("ps-", years), collapse = "|"),
      full.names = T
    )
    files_to_remove <- data.frame(
      file = files_to_remove,
      year = gsub("_freq.*", "", gsub(".*_ps-", "", files_to_remove))
    ) %>%
      group_by(!!sym("year")) %>%
      filter(!!sym("file") != max(!!sym("file"))) %>%
      pull(!!sym("file"))
    lapply(files_to_remove, file_remove)
  }

  if (isFALSE(skip_updates)) {
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
  }

  if (classification == "sitc") {
    aggregations <- 0:5
  } else {
    if (classification == "bec") {
      aggregations <- 0:3
    } else {
      aggregations <- c(0, 2, 4, 6)
    }
  }

  raw_zip <- list.files(
    path = raw_dir_zip,
    pattern = "\\.zip",
    full.names = T
  )

  # convert to postgres ----

  years <- rev(years)

  if (isTRUE(postgres)) {
    lapply(seq_along(years), convert_to_postgres, yrs = years, raw_dir, raw_zip, years, classification)
    create_indexes_postgres(raw_dir)
  }
}
