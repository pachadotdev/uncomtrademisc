#' Connect to PostgreSQL
#'
#' Open a SQL connection to tradestatistics PSQL server
#'
#' @export
con_tradestatistics <- function() {
  RPostgres::dbConnect(
    RPostgres::Postgres(),
    host = "tradestatistics.io",
    user = Sys.getenv("TRADESTATISTICS_SQL_USR"),
    password = Sys.getenv("TRADESTATISTICS_SQL_PWD")
  )
}

#' Update countries table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_countries <- function(con, path = "attributes") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.countries")

  dbSendQuery(
    con,
    "CREATE TABLE public.countries
      (
      country_iso varchar(5) DEFAULT NULL,
      country_name_english text DEFAULT NULL,
      country_fullname_english text DEFAULT NULL,
      continent_id integer DEFAULT NULL,
      continent_name_english text DEFAULT NULL,
      eu28_member integer DEFAULT NULL
      )"
  )

  dbWriteTable(con, "countries", readRDS(paste0(path, "/countries.rds")), append = TRUE, overwrite = FALSE, row.names = FALSE)
}

#' Update commodities table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_commodities <- function(con, path = "attributes") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.commodities")

  dbSendQuery(
    con,
    "CREATE TABLE public.commodities
      (
      commodity_code varchar(6) DEFAULT NULL,
      commodity_fullname_english text DEFAULT NULL,
      section_code varchar(3) DEFAULT NULL,
      section_fullname_english text DEFAULT NULL
      )"
  )

  dbWriteTable(con, "commodities", readRDS(paste0(path, "/commodities.rds")),
               append = TRUE, overwrite = FALSE, row.names = FALSE)

  dbSendQuery(con, "DROP TABLE IF EXISTS public.commodities_short")

  dbSendQuery(
    con,
    "CREATE TABLE public.commodities_short
      (
      commodity_code char(4) NOT NULL,
      commodity_fullname_english text DEFAULT NULL
      )"
  )

  dbWriteTable(con, "commodities_short", readRDS(paste0(path, "/commodities_short.rds")), append = TRUE, overwrite = FALSE, row.names = FALSE)
}

#' Update sections table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_sections <- function(con, path = "attributes") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.sections")

  dbSendQuery(
    con,
    "CREATE TABLE public.sections
      (
      section_code char(2) DEFAULT NULL,
      section_fullname_english text DEFAULT NULL
      )"
  )

  dbWriteTable(con, "sections", readRDS(paste0(path, "/sections.rds")),
               append = TRUE, overwrite = FALSE, row.names = FALSE)

  dbSendQuery(con, "DROP TABLE IF EXISTS public.sections_colors")

  dbSendQuery(
    con,
    "CREATE TABLE public.sections_colors
      (
      section_code varchar(3) DEFAULT NULL,
      section_color char(7) DEFAULT NULL
      )"
  )

  dbWriteTable(con, "sections_colors", readRDS(paste0(path, "/sections_colors.rds")), append = TRUE, overwrite = FALSE, row.names = FALSE)

}

#' Update YR table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom purrr map
#' @importFrom dplyr filter collect group_by summarise ungroup arrange
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_yr <- function(con, path = "hs-rev2012-tidy") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.yr")

  dbSendQuery(
    con,
    "CREATE TABLE public.yr
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
  )

  d <- open_dataset(path, partitioning = c("year", "reporter_iso"))

  y <- 2002:2020

  map(
    y,
    function(y) {
      message(y)
      d2 <- d %>%
        filter(!!sym("year") == y) %>%
        collect() %>%
        group_by(!!sym("year"), !!sym("reporter_iso")) %>%
        summarise(
          trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
          trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(!!sym("reporter_iso"))

      dbWriteTable(con, "yr", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )

  dbSendQuery(
    con,
    "CREATE INDEX yr_y ON public.yr (year)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yr_r ON public.yr (reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yr_yr ON public.yr (year, reporter_iso)"
  )
}

#' Update YRP table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom purrr map
#' @importFrom dplyr filter collect group_by summarise ungroup arrange
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_yrp <- function(con, path = "hs-rev2012-tidy") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.yrp")

  dbSendQuery(
    con,
    "CREATE TABLE public.yrp
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      partner_iso varchar(5) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
  )

  d <- open_dataset(path, partitioning = c("year", "reporter_iso"))

  y <- 2002:2020

  map(
    y,
    function(y) {
      message(y)
      d2 <- d %>%
        filter(!!sym("year") == y) %>%
        collect() %>%
        group_by(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso")) %>%
        summarise(
          trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
          trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(!!sym("reporter_iso"), !!sym("partner_iso"))

      dbWriteTable(con, "yrp", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrp_y ON public.yrp (year)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrp_r ON public.yrp (reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrp_p ON public.yrp (partner_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrp_yr ON public.yrp (year, reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrp_yp ON public.yrp (year, partner_iso)"
  )
}

#' Update YRPC table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom purrr map2
#' @importFrom dplyr filter collect group_by summarise ungroup arrange
#' @importFrom tidyr expand_grid
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_yrpc <- function(con, path = "hs-rev2012-tidy") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.yrpc")

  dbSendQuery(
    con,
    "CREATE TABLE public.yrpc
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      partner_iso varchar(5) NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
  )

  d <- open_dataset(path, partitioning = c("year", "reporter_iso"))

  y <- 2002:2020
  r <- d %>%
    select(!!sym("reporter_iso")) %>%
    distinct() %>%
    collect() %>%
    pull() %>%
    sort()

  pairs <- expand_grid(y,r)

  map2(
    pairs$y,
    pairs$r,
    function(y,r) {
      message(paste(y,r))
      d2 <- d %>%
        filter(!!sym("year") == y, !!sym("reporter_iso") == r) %>%
        collect() %>%
        group_by(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso"), !!sym("section_code"), !!sym("commodity_code")) %>%
        summarise(
          trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
          trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("section_code"), !!sym("commodity_code"))

      dbWriteTable(con, "yrpc", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_y ON public.yrpc (year)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_r ON public.yrpc (reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_p ON public.yrpc (partner_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_c ON public.yrpc (commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_s ON public.yrpc (section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yr ON public.yrpc (year, reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yp ON public.yrpc (year, partner_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yc ON public.yrpc (year, commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_ys ON public.yrpc (year, section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_rp ON public.yrpc (reporter_iso, partner_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_rpc ON public.yrpc (reporter_iso, partner_iso, commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yrp ON public.yrpc (year, reporter_iso, partner_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yrc ON public.yrpc (year, reporter_iso, commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yrs ON public.yrpc (year, reporter_iso, section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_ypc ON public.yrpc (year, partner_iso, commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrpc_yps ON public.yrpc (year, partner_iso, section_code)"
  )
}

#' Update YRC table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom purrr map2
#' @importFrom dplyr filter collect group_by summarise ungroup arrange
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_yrc <- function(con, path = "hs-rev2012-tidy") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.yrc")

  dbSendQuery(
    con,
    "CREATE TABLE public.yrc
      (
      year integer NOT NULL,
      reporter_iso varchar(5) NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
  )

  d <- open_dataset(path, partitioning = c("year", "reporter_iso"))

  y <- 2002:2020

  map(
    y,
    function(y) {
      message(y)
      d2 <- d %>%
        filter(!!sym("year") == y) %>%
        collect() %>%
        group_by(!!sym("year"), !!sym("reporter_iso"), !!sym("section_code"), !!sym("commodity_code")) %>%
        summarise(
          trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
          trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(!!sym("reporter_iso"), !!sym("section_code"))

      dbWriteTable(con, "yrc", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_y ON public.yrc (year)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_r ON public.yrc (reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_s ON public.yrc (section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_c ON public.yrc (commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_yr ON public.yrc (year, reporter_iso)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_ys ON public.yrc (year, section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_yc ON public.yrc (year, commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_rs ON public.yrc (reporter_iso, section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yrc_rc ON public.yrc (reporter_iso, commodity_code)"
  )
}

#' Update YC table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom purrr map2
#' @importFrom dplyr filter collect group_by summarise ungroup arrange
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_yc <- function(con, path = "hs-rev2012-tidy") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.yc")

  dbSendQuery(
    con,
    "CREATE TABLE public.yc
      (
      year integer NOT NULL,
      section_code char(2) NOT NULL,
      commodity_code char(6) NOT NULL,
      trade_value_usd_imp decimal(16,2) DEFAULT NULL,
      trade_value_usd_exp decimal(16,2) DEFAULT NULL
      )"
  )

  d <- open_dataset(path, partitioning = c("year", "reporter_iso"))

  y <- 2002:2020

  map(
    y,
    function(y) {
      message(y)
      d2 <- d %>%
        filter(!!sym("year") == y) %>%
        collect() %>%
        group_by(!!sym("year"), !!sym("section_code"), !!sym("commodity_code")) %>%
        summarise(
          trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
          trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(!!sym("section_code"))

      dbWriteTable(con, "yc", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )

  dbSendQuery(
    con,
    "CREATE INDEX yc_y ON public.yc (year)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yc_s ON public.yc (section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yc_c ON public.yc (commodity_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yc_ys ON public.yc (year, section_code)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX yc_yc ON public.yc (year, commodity_code)"
  )
}

#' Update RTAs table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_rtas <- function(con, path = "mfn-rtas") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.rtas")

  dbSendQuery(
    con,
    "CREATE TABLE public.rtas
      (
      year integer NOT NULL,
      country1 char(3) DEFAULT NULL,
      country2 char(3) DEFAULT NULL,
      rta integer DEFAULT NULL
      )"
  )

  dbSendQuery(
    con,
    "CREATE INDEX rtas_y ON public.rtas (year)"
  )

  dbWriteTable(con, "rtas", readRDS(paste0(path, "/rtas.rds")), append = TRUE, overwrite = FALSE, row.names = FALSE)
}

#' Update tariffs table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @param path directory where the tidy data is
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @export
update_tariffs <- function(con, path = "mfn-rtas") {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.tariffs")

  dbSendQuery(
    con,
    "CREATE TABLE public.tariffs
      (
      year integer NOT NULL,
      reporter_iso char(3) DEFAULT NULL,
      commodity_code char(6) DEFAULT NULL,
      sum_of_rates decimal(7,2) DEFAULT NULL,
      min_rate decimal(7,2) DEFAULT NULL,
      max_rate decimal(7,2) DEFAULT NULL,
      simple_average decimal(7,2) DEFAULT NULL,
      nbr_na_lines integer DEFAULT NULL,
      nbr_free_lines integer DEFAULT NULL,
      nbr_ave_lines integer DEFAULT NULL,
      nbr_dutiable_lines integer DEFAULT NULL,
      total_no_of_valid_lines integer DEFAULT NULL,
      total_no_of_lines integer DEFAULT NULL
      )"
  )

  dbSendQuery(
    con,
    "CREATE INDEX tariffs_y ON public.tariffs (year)"
  )

  d <- readRDS(paste0(path, "/tariffs.rds"))

  map(
    2002:2020,
    function(y) {
      message(y)
      d2 <- d %>%
        filter(!!sym("year") == y) %>%
        collect() %>%
        arrange(!!sym("reporter_iso"), !!sym("commodity_code"))

      dbWriteTable(con, "tariffs", d2, append = TRUE, overwrite = FALSE, row.names = FALSE)
    }
  )
}

#' Update distances table
#'
#' Deletes and uploads the table again
#'
#' @param con SQL connection object
#' @importFrom RPostgres dbSendQuery dbWriteTable
#' @importFrom dplyr select everything mutate distinct inner_join
#' @export
update_distances <- function(con) {
  dbSendQuery(con, "DROP TABLE IF EXISTS public.distances")

  dbSendQuery(
    con,
    "CREATE TABLE public.distances
      (
      country1 char(3) NOT NULL,
      country2 char(3) NOT NULL,
      dist decimal(7,2) DEFAULT NULL,
      distcap decimal(7,2) DEFAULT NULL,
      colony integer DEFAULT NULL,
      comlang_ethno integer DEFAULT NULL,
      comlang_off integer DEFAULT NULL,
      contig integer DEFAULT NULL,
      smctry integer DEFAULT NULL
      )"
  )

  dbSendQuery(
    con,
    "CREATE INDEX distances_o ON public.distances (country1)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX distances_d ON public.distances (country2)"
  )

  dbSendQuery(
    con,
    "CREATE INDEX distances_od ON public.distances (country1, country2)"
  )

  d <- cepiigeodist::dist_cepii %>%
    select(c("iso_o", "iso_d", "dist", "distcap",
             "colony", "comlang_ethno", "comlang_off",
             "contig", "smctry")) %>%
    mutate(
      country1 = tolower(pmin(!!sym("iso_o"), !!sym("iso_d"))),
      country2 = tolower(pmax(!!sym("iso_o"), !!sym("iso_d")))
    ) %>%
    select(-c("iso_o", "iso_d")) %>%
    select(c("country1", "country2"), everything()) %>%
    distinct()

  d <- d %>%
    inner_join(tradestatistics::ots_countries %>% select(country_iso), by = c("country1" = "country_iso")) %>%
    inner_join(tradestatistics::ots_countries %>% select(country_iso), by = c("country2" = "country_iso"))

  dbWriteTable(con, "distances", d, append = TRUE, overwrite = FALSE, row.names = FALSE)
}
