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

#' @importFrom dplyr filter mutate mutate_if left_join group_by
#' @importFrom stringr str_to_lower str_squish
#' @importFrom arrow write_dataset
#' @importFrom janitor clean_names
#' @importFrom readr cols col_double col_integer col_character
#' @importFrom purrr map
#' @importFrom rlang sym
convert_to_arrow <- function(t, yrs, raw_dir_parquet, raw_subdirs_parquet, raw_zip) {
  messageline(yrs[t])

  try(unlink(grep(paste0("/",yrs[t],"/"), raw_subdirs_parquet$file, value = T), recursive = T))

  zip <- grep(paste0(yrs[t], "_freq-A"), raw_zip, value = T)

  csv <- zip %>%
    str_replace("/zip/", "/parquet/") %>%
    str_replace("zip$", "csv")

  extract(zip, raw_dir_parquet)

  d <- read_csv(
    csv,
    col_types = cols(
      Classification = col_character(),
      Year = col_integer(),
      Period = col_integer(),
      `Period Desc.` = col_integer(),
      `Aggregate Level` = col_integer(),
      `Is Leaf Code` = col_integer(),
      `Trade Flow Code` = col_integer(),
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
      Flag = col_integer())
  )

  d <- d %>%
    clean_names() %>%
    rename(trade_value_usd = !!sym("trade_value_us")) %>%
    mutate_if(is.character, function(x) { str_to_lower(str_squish(x)) }) %>%
    mutate(
      reporter_iso = unspecified(!!sym("reporter_iso")),
      partner_iso = unspecified(!!sym("partner_iso")),
      trade_flow = unspecified(!!sym("trade_flow"))
    )

  al <- sort(unique(d$aggregate_level))

  tf <- sort(unique(d$trade_flow))

  map(
    al,
    function(x) {
      d2 <- d %>%
        filter(!!sym("aggregate_level") == x)

      gc()

      if (nrow(d2) > 0) {
        map(
          tf,
          function(x) {
            d2 %>%
              filter(!!sym("trade_flow") == x) %>%
              group_by(!!sym("aggregate_level"), !!sym("trade_flow"), !!sym("year"), !!sym("reporter_iso")) %>%
              write_dataset(raw_dir_parquet, hive_style = F)

            gc()
          }
        )
      }

      rm(d2); gc()
    }
  )

  file_remove(csv); rm(d); gc()
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
#' @param arrow Set to `FALSE` to just download the datasets without
#'     converting to Apache Arrow. Otherwise keep this set to `TRUE`, which
#'     eases posterior analysis and modeling.
#' @param token parameter for non-interactive calls, otherwise it shows a prompt
#' @param dataset parameter for non-interactive calls, otherwise it shows a prompt
#' @param remove_old_files parameter for non-interactive calls, otherwise it shows a prompt
#' @param subset_years parameter for non-interactive calls, otherwise it shows a prompt
#' @param parallel parameter for non-interactive calls, otherwise it shows a prompt
#' @param subdir parameter to download in a sub-directory such as `"finp"`, etc.
#'
#' @export
data_downloading <- function(arrow = T, token = NULL, dataset = NULL, remove_old_files = NULL,
                             subset_years = NULL, parallel = NULL, subdir = NULL) {
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

  files_to_update <- download_links %>%
    filter(!!sym("local_file_date") < !!sym("server_file_date"))

  files_to_update_2 <- download_links %>%
    mutate(file_exists = file.exists(!!sym("new_file"))) %>%
    filter(!!sym("file_exists") == FALSE)

  files_to_update <- files_to_update %>%
    bind_rows(files_to_update_2)

  years_to_update <- files_to_update$year

  if (remove_old_files == 1L) {
    files_to_remove <- list.files(raw_dir_zip,
                                  pattern = paste(paste0("ps-",years_to_update), collapse = "|"),
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

  if (length(years_to_update) > 0) {
    download_files(download_links, parallel = parallel)
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

  if (isFALSE(arrow)) return(TRUE)

  # convert to arrow ----

  if (classification == "sitc") {
    aggregations <- 0:5
  } else {
    aggregations <- c(0,2,4,6)
  }

  # re-create any missing/updated arrow dataset
  raw_subdirs_parquet <- expand.grid(
    base_dir = raw_dir_parquet,
    aggregations = aggregations,
    flow = c("import", "export", "re-import", "re-export"),
    year = years
  ) %>%
    mutate(
      file = paste0(
        !!sym("base_dir"),
        "/", aggregations, "/",
        !!sym("flow"), "/", !!sym("year")),
      exists = file.exists(file)
    )

  update_years <- raw_subdirs_parquet %>%
    group_by(!!sym("year")) %>%
    summarise(exists = as.logical(max(!!sym("exists")))) %>%
    filter(exists == FALSE | !!sym("year") %in% years_to_update) %>%
    select(!!sym("year")) %>%
    distinct() %>%
    pull()

  raw_zip <- list.files(
    path = raw_dir_zip,
    pattern = "\\.zip",
    full.names = T
  )

  raw_zip <- grep(paste(paste0("ps-", update_years), collapse = "|"), raw_zip, value = TRUE)

  if (any(update_years > 0)) {
    lapply(seq_along(update_years), convert_to_arrow, yrs = update_years, raw_dir_parquet, raw_subdirs_parquet, raw_zip)
  }
}
