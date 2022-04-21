# Internal functions ----

#' @importFrom dplyr collect
filter_flow <- function(d, y, f, a = 6) {
  d <- d %>%
    filter(
      !!sym("year") == y,
      !!sym("trade_flow") == f,
      !!sym("aggregate_level") == a
    ) %>%
    filter(
      !(!!sym("reporter_iso") %in% c("wld", "0-unspecified")),
      !(!!sym("partner_iso") %in% c("wld", "0-unspecified"))
    ) %>%
    select(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code"), !!sym("trade_value_usd")) %>%
    collect()

  # print(sort(unique(d$reporter_iso)))
  # print(sort(unique(d$partner_iso)))

  return(d)
}

# compute_ratios <- function(d) {
#   d %>%
#
#     mutate(
#       cif_fob_ratio = trade_value_usd_imp / trade_value_usd_exp
#     ) %>%
#
#     mutate(
#       cif_fob_ratio_ok = ifelse(
#         cif_fob_ratio >= 1 & is.finite(cif_fob_ratio), 1L, 0L)
#     ) %>%
#
#     # The unit or net CIF/FOB ratios can be weighted, or not, by the gap between
#     # reported mirror quantities Min(Xij,Mji) / Max(Xij,Mji)
#     mutate(
#       cif_fob_weights = pmin(trade_value_usd_imp, trade_value_usd_exp, na.rm = T) /
#         pmax(trade_value_usd_imp, trade_value_usd_exp, na.rm = T)
#     )
# }

#' @importFrom arrow open_dataset
count_countries <- function(path, y) {
  open_dataset(path,
               partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso")) %>%
    filter(
      !!sym("year") == y,
      !!sym("aggregate_level") == 6
    ) %>%
    select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("reporter_code"), !!sym("partner_code")) %>%
    distinct() %>%
    collect() %>%
    fix_missing_iso()
}

open_dataset_partitioning <- function(path) {
  open_dataset(path,
               partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))
}

#' @importFrom dplyr summarise
data_partitioned <- function(y, f, replace_unspecified_iso) {
  cat("reading data.")

  if (y >= 2012) {
    hs02_count <- count_countries("hs-rev2002/parquet")
    hs12_count <- count_countries("hs-rev2012/parquet")

    hs02_count <- length(unique(c(hs02_count$reporter_iso, hs02_count$partner_iso)))
    hs12_count <- length(unique(c(hs12_count$reporter_iso, hs12_count$partner_iso)))

    d <- if (hs12_count >= hs02_count) {
      open_dataset_partitioning("hs-rev2012/parquet")
    } else {
      open_dataset_partitioning("hs-rev2002/parquet")
    }

    use_hs12 <- ifelse(hs12_count >= hs02_count, TRUE, FALSE)
    cat(".")
  } else {
    d <- open_dataset_partitioning("hs-rev2002/parquet")
    use_hs12 <- FALSE
    cat(".")
  }

  d <- d %>%
    filter(
      !!sym("year") == y,
      !!sym("trade_flow") == f,
      !!sym("aggregate_level") == 6
    ) %>%
    select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("reporter_code"), !!sym("partner_code"), !!sym("commodity_code"), !!sym("trade_value_usd")) %>%
    collect() %>%
    filter(!!sym("reporter_iso") != "wld", !!sym("partner_iso") != "wld")

  if (isTRUE(replace_unspecified_iso)) {
    d <- d %>%
      fix_missing_iso()
  }
  cat(".\n")

  # unique(nchar(c(d$reporter_iso, d$partner_iso)))

  if (isFALSE(use_hs12)) {
    cat("converting to hs12.")
    product_correlation_sbst <- uncomtrademisc::product_correlation %>%
      select(!!sym("hs02"), !!sym("hs12")) %>%
      arrange(!!sym("hs12")) %>%
      distinct(!!sym("hs02"), .keep_all = T)

    cat(".")

    d <- d %>%
      left_join(product_correlation_sbst, by = c("commodity_code" = "hs02"))
    cat(".")

    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"), commodity_code = !!sym("hs12"), !!sym("trade_value_usd")) %>%
      fix_missing_commodity() %>%
      group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
      summarise(trade_value_usd = sum(!!sym("trade_value_usd"), na.rm = T))
    cat(".\n")

    # missing_codes_count <- d %>%
    #   ungroup() %>%
    #   filter(is.na(commodity_code)) %>%
    #   count() %>%
    #   pull()
    #
    # stopifnot(missing_codes_count == 0)
  } else {
    cat("summarizing.")
    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code"), !!sym("trade_value_usd")) %>%
      fix_missing_commodity() %>%
      group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
      summarise(trade_value_usd = sum(!!sym("trade_value_usd"), na.rm = T))
    cat(".\n")
  }

  return(d)
}

#' @importFrom dplyr full_join
join_flows <- function(dimp, dexp) {
  d <- dimp %>%
    full_join(dexp, by = c("reporter_iso" = "partner_iso", "partner_iso" = "reporter_iso", "commodity_code")) %>%
    rename(trade_value_usd_imp = !!sym("trade_value_usd.x"), trade_value_usd_exp = !!sym("trade_value_usd.y")) %>%

    mutate(
      trade_value_usd_imp = ifelse(is.na(!!sym("trade_value_usd_imp")), 0, !!sym("trade_value_usd_imp")),
      trade_value_usd_exp = ifelse(is.na(!!sym("trade_value_usd_exp")), 0, !!sym("trade_value_usd_exp")),
      trade_value_usd_total = !!sym("trade_value_usd_imp") + !!sym("trade_value_usd_exp")
    ) %>%

    filter(!!sym("trade_value_usd_total") > 0) %>%
    select(-!!sym("trade_value_usd_total"))

  return(d)
}

filter_traded_value <- function(d, val) {
  d %>%
    filter(
      !!sym("trade_value_usd_imp") >= val & !!sym("trade_value_usd_exp") >= val
    )
}

fix_iso_codes <- function(val) {
  case_when(
    val == "rom" ~ "rou", # Romania
    val == "yug" ~ "scg", # Just for joins purposes, Yugoslavia splitted for the
    # analyzed period
    val == "tmp" ~ "tls", # East Timor
    val == "zar" ~ "cod", # Congo (Democratic Republic of the)
    TRUE ~ val
  )
}

fix_missing_iso <- function(d) {
  d %>%
    mutate(
      reporter_iso = case_when(
        reporter_code == 80 ~ "e-80", # br. antarctic terr.
        reporter_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
        reporter_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
        reporter_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
        reporter_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
        reporter_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
        reporter_code == 473 ~ "e-473", # laia, nes
        reporter_code == 490 ~ "e-490", # other asia, nes
        reporter_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
        reporter_code == 527 ~ "e-527", # oceania, nes
        reporter_code == 536 ~ "e-536", # neutral zone
        reporter_code == 568 ~ "e-568", # other europe, nes
        reporter_code == 577 ~ "e-577", # other africa, nes
        reporter_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
        reporter_code == 637 ~ "e-637", # north america and central america, nes
        reporter_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
        reporter_code == 837 ~ "e-837", # bunkers
        reporter_code == 838 ~ "e-838", # free zones
        reporter_code == 839 ~ "e-839", # special categories
        reporter_code == 849 ~ "e-849", # us misc. pacific isds
        reporter_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
        reporter_code == 899 ~ "e-899", # areas, nes
        TRUE ~ !!sym("reporter_iso")
      ),
      partner_iso = case_when(
        partner_code == 80 ~ "e-80", # br. antarctic terr.
        partner_code == 129 ~ "e-129", # Caribbean, not elsewhere specified
        partner_code == 221 ~ "e-221", # Eastern Europe, not elsewhere specified
        partner_code == 290 ~ "e-290", # Northern Africa, not elsewhere specified
        partner_code == 471 ~ "e-471", # Central American Common Market, not elsewhere specified
        partner_code == 472 ~ "e-472", # Africa CAMEU region, not elsewhere specified
        partner_code == 473 ~ "e-473", # laia, nes
        partner_code == 490 ~ "e-490", # other asia, nes
        partner_code == 492 ~ "e-492", # Europe EU, not elsewhere specified
        partner_code == 527 ~ "e-527", # oceania, nes
        partner_code == 536 ~ "e-536", # neutral zone
        partner_code == 568 ~ "e-568", # other europe, nes
        partner_code == 577 ~ "e-577", # other africa, nes
        partner_code == 636 ~ "e-636", # Rest of America, not elsewhere specified
        partner_code == 637 ~ "e-637", # north america and central america, nes
        partner_code == 697 ~ "e-697", # Europe EFTA, not elsewhere specified
        partner_code == 837 ~ "e-837", # bunkers
        partner_code == 838 ~ "e-838", # free zones
        partner_code == 839 ~ "e-839", # special categories
        partner_code == 849 ~ "e-849", # us misc. pacific isds
        partner_code == 879 ~ "e-879", # Western Asia, not elsewhere specified
        partner_code == 899 ~ "e-899", # areas, nes
        TRUE ~ !!sym("partner_iso")
      )
    )
}

fix_missing_commodity <- function(d) {
  d %>%
    mutate(
      commodity_code = case_when(
        is.na(!!sym("commodity_code")) ~ "999999",
        TRUE ~ !!sym("commodity_code")
      )
    )
}

#' @importFrom dplyr left_join
add_hs_section <- function(d) {
  d %>%
    left_join(
      uncomtrademisc::ots_commodities %>%
        select(!!sym("commodity_code"), !!sym("section_code")),
      by = "commodity_code"
    ) %>%
    mutate(section_code = ifelse(is.na(!!sym("section_code")), "999", !!sym("section_code")))
}

reported_by <- function(d) {
  d %>%
    mutate(
      reported_by = case_when(
        !!sym("trade_value_usd_exp") != 0 &
          !!sym("trade_value_usd_imp") != 0 ~ "both parties",
        !!sym("trade_value_usd_exp") != 0 &
          !!sym("trade_value_usd_imp") == 0 ~ "exporter only",
        !!sym("trade_value_usd_exp") == 0 &
          !!sym("trade_value_usd_imp") != 0 ~ "importer only"
      )
    )
}

#' @importFrom dplyr ends_with
subtract_re_imp_exp <- function(d) {
  d %>%
    mutate_if(is.numeric, function(x) ifelse(is.na(x), 0, x)) %>%
    mutate_if(is.numeric, function(x) ifelse(!is.finite(x), NA, x)) %>%
    mutate(
      trade_value_usd = pmax(!!sym("trade_value_usd.x") - !!sym("trade_value_usd.y"), 0, na.rm = T)
    ) %>%
    select(-ends_with(".x"), -ends_with(".y"))
}

#' Reshape UN COMTRADE bulk datasets to put these under tidy data format
#'
#' This wrapper takes the raw data and re-expresses it under the Tidy Data
#' logic: exports and imports are on different columns because those have
#' different units (FOB vs CIF) and each combination
#' year-origin-destination-product corresponds to a row. This function can also
#' discount re-imports and re-export and fix missing ISO-3 codes.
#'
#' @importFrom dplyr left_join
#'
#' @param year which year to reshape
#' @param subtract_re subtract re-imports (re-exports) from imports (exports),
#' when the result is negative, it is converted to 0
#' @param replace_unspecified_iso convert all ocurrences of `0-unspecified` to
#' `e-[0-9]` (i.e., `e-439` when corresponding)
#'
#' @export
tidy_flows <- function(year, subtract_re = TRUE, replace_unspecified_iso = TRUE) {
  messageline("Reading Imports data")
  dimp <- data_partitioned(y = year, f = "import", replace_unspecified_iso)
  cat(".")
  if (isTRUE(subtract_re)) {
    dreimp <- data_partitioned(y = year, f = "re-import", replace_unspecified_iso)
    cat(".")
    dimp <- dimp %>%
      left_join(dreimp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      subtract_re_imp_exp()
    cat(".")
    rm(dreimp)
    cat(".")
  }

  messageline("Exports")
  dexp <- data_partitioned(y = year, f = "export", replace_unspecified_iso)
  cat(".")

  if (isTRUE(subtract_re)) {
    dreexp <- data_partitioned(y = year, f = "re-export", replace_unspecified_iso)
    cat(".")
    dexp <- dexp %>%
      left_join(dreexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      subtract_re_imp_exp()
    cat(".")
    rm(dreexp)
    cat(".")
  }

  # print(dimp %>% filter(reporter_iso == "can", partner_iso == "usa"))
  # print(dexp %>% filter(reporter_iso == "can", partner_iso == "usa"))

  messageline("Joining imports and exports")
  dimp <- dimp %>%
    join_flows(dexp)
  cat(".")
  rm(dexp)
  cat(".")

  missing_codes <- dimp %>%
    filter(is.na(!!sym("commodity_code"))) %>%
    nrow()
  cat(".")

  stopifnot(missing_codes == 0)
  cat(".")

  unique_pairs <- dimp %>%
    select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
    distinct() %>%
    nrow()
  cat(".")

  stopifnot(unique_pairs == nrow(dimp))
  cat(".\n")

  messageline("Ordering columns.")
  dimp <- dimp %>%
    add_hs_section()
  cat(".\n")

  # dimp %>%
  #   filter(section_code == "999")

  dimp <- dimp %>%
    mutate(year = year)
  cat(".\n")

  dimp <- dimp %>%
    reported_by()
  cat(".\n")

  return(dimp)
}

# add_gravity_cols <- function(d, join = "left") {
#   # Geographic variables come from the previous version of Gravity (legacy
#   # version) and from Geodist. In the next version of BACI the more recent
#   # Gravity dataset will be used.
#
#   d2 <- dist_cepii %>%
#     select(reporter_iso = iso_o, partner_iso = iso_d, dist, contig, colony, comlang_off) %>%
#     mutate_if(is.character, tolower) %>%
#     mutate(
#       reporter_iso = fix_iso_codes(reporter_iso),
#       partner_iso = fix_iso_codes(partner_iso),
#       contig = as.integer(contig),
#       colony = as.integer(colony),
#       comlang_off = as.integer(comlang_off)
#     )
#
#   if (join == "left") {
#     d %>%
#       left_join(d2, by = c("reporter_iso", "partner_iso"))
#   }
#
#   if (join == "inner") {
#     d %>%
#       inner_join(d2, by = c("reporter_iso", "partner_iso"))
#   }
# }
#
# add_rta_col <- function(d) {
#   rtas <- open_dataset("../rtas-and-tariffs/rtas", partitioning = "year") %>%
#     collect()
#
#   d <- d %>%
#     mutate(
#       country1 = pmin(reporter_iso, partner_iso),
#       country2 = pmax(reporter_iso, partner_iso)
#     ) %>%
#     left_join(
#       rtas %>%
#         select(year, country1, country2, rta),
#       by = c("year", "country1", "country2")
#     ) %>%
#     mutate(
#       rta = as.integer(ifelse(is.na(rta), 0, rta))
#     ) %>%
#     select(-c(country1,country2))
#
#   return(d)
# }
#
# add_sanctions_cols <- function(d, y) {
#   dtrade <- gsdb_dyadic %>%
#     select(year, sanctioned_state_iso3, trade_sanction = descr_trade) %>%
#     filter(year == y) %>%
#     filter(trade_sanction != "") %>%
#     mutate(sanctioned_state_iso3 = tolower(sanctioned_state_iso3)) %>%
#     distinct() %>%
#     mutate(
#       trade_sanction = case_when(
#         trade_sanction == "exp_part" ~ "1_exports_partial",
#         trade_sanction == "imp_part" ~ "2_imports_partial",
#
#         trade_sanction == "exp_part,imp_part" ~ "3_exports_and_imports_partial",
#
#         trade_sanction == "exp_compl" ~ "4_exports_complete",
#         trade_sanction == "imp_compl" ~ "5_imports_complete",
#
#         trade_sanction == "exp_part,imp_compl" ~ "6_exports_partial_imports_complete",
#         trade_sanction == "exp_compl,imp_part" ~ "7_exports_complete_imports_partial",
#
#         trade_sanction == "exp_compl,imp_compl" ~ "8_exports_and_imports_complete",
#
#         TRUE ~ ""
#       )
#     ) %>%
#     select(-year) %>%
#     group_by(sanctioned_state_iso3) %>%
#     mutate(ind = substr(trade_sanction, 1, 1)) %>%
#     filter(ind == max(ind)) %>%
#     select(-ind)
#
#   # dtrade %>%
#   #   select(trade_sanction, trade_sanction2) %>%
#   #   distinct() %>%
#   #   arrange(trade_sanction)
#
#   dfinancial <- gsdb_dyadic %>%
#     select(year, sanctioned_state_iso3, financial_sanction = trade) %>%
#     filter(year == y, financial_sanction == 1) %>%
#     mutate(sanctioned_state_iso3 = tolower(sanctioned_state_iso3)) %>%
#     distinct() %>%
#     select(-year) %>%
#     mutate(financial_sanction = as.integer(financial_sanction))
#
#   dmilitary <- gsdb_dyadic %>%
#     select(year, sanctioned_state_iso3, military_sanction = trade) %>%
#     filter(year == y, military_sanction == 1) %>%
#     mutate(sanctioned_state_iso3 = tolower(sanctioned_state_iso3)) %>%
#     distinct() %>%
#     select(-year) %>%
#     mutate(military_sanction = as.integer(military_sanction))
#
#   darms <- gsdb_dyadic %>%
#     select(year, sanctioned_state_iso3, arms_sanction = trade) %>%
#     filter(year == y, arms_sanction == 1) %>%
#     mutate(sanctioned_state_iso3 = tolower(sanctioned_state_iso3)) %>%
#     distinct() %>%
#     select(-year) %>%
#     mutate(arms_sanction = as.integer(arms_sanction))
#
#   d %>%
#     left_join(dtrade, by = c("reporter_iso" = "sanctioned_state_iso3")) %>%
#     left_join(dfinancial, by = c("reporter_iso" = "sanctioned_state_iso3")) %>%
#     left_join(dmilitary, by = c("reporter_iso" = "sanctioned_state_iso3")) %>%
#     left_join(darms, by = c("reporter_iso" = "sanctioned_state_iso3")) %>%
#     rename(
#       reporter_trade_sanction = trade_sanction,
#       reporter_financial_sanction = financial_sanction,
#       reporter_military_sanction = military_sanction,
#       reporter_arms_sanction = arms_sanction
#     ) %>%
#
#     left_join(dtrade, by = c("partner_iso" = "sanctioned_state_iso3")) %>%
#     left_join(dfinancial, by = c("partner_iso" = "sanctioned_state_iso3")) %>%
#     left_join(dmilitary, by = c("partner_iso" = "sanctioned_state_iso3")) %>%
#     left_join(darms, by = c("partner_iso" = "sanctioned_state_iso3")) %>%
#     rename(
#       partner_trade_sanction = trade_sanction,
#       partner_financial_sanction = financial_sanction,
#       partner_military_sanction = military_sanction,
#       partner_arms_sanction = arms_sanction
#     ) %>%
#
#     mutate(
#       reporter_trade_sanction = ifelse(is.na(reporter_trade_sanction), "0_no_sanction", reporter_trade_sanction),
#       reporter_financial_sanction = ifelse(is.na(reporter_financial_sanction), 0L, reporter_financial_sanction),
#       reporter_military_sanction = ifelse(is.na(reporter_military_sanction), 0L, reporter_military_sanction),
#       reporter_arms_sanction = ifelse(is.na(reporter_arms_sanction), 0L, reporter_arms_sanction),
#
#       partner_trade_sanction = ifelse(is.na(partner_trade_sanction), "0_no_sanction", partner_trade_sanction),
#       partner_financial_sanction = ifelse(is.na(partner_financial_sanction), 0L, partner_financial_sanction),
#       partner_military_sanction = ifelse(is.na(partner_military_sanction), 0L, partner_military_sanction),
#       partner_arms_sanction = ifelse(is.na(partner_arms_sanction), 0L, partner_arms_sanction)
#     )
# }
#
# filter_flow_impute <- function(d, y, f, a) {
#   d %>%
#     filter(
#       year == y,
#       trade_flow == f,
#       aggregate_level == a
#     ) %>%
#     filter(
#       !reporter_iso %in% c("wld"),
#       !partner_iso %in% c("wld")
#     ) %>%
#     select(year, reporter_iso, partner_iso, reporter_code, partner_code,
#            commodity_code, trade_value_usd) %>%
#     collect() %>%
#     fix_missing_iso() %>%
#     select(-c(reporter_code, partner_code))
# }
#
# aggregate_by_hs_section <- function(d) {
#   d %>%
#     mutate(section_code = ifelse(is.na(section_code), "999", section_code)) %>%
#     group_by(reporter_iso, partner_iso, section_code) %>%
#     summarise_if(is.numeric, sum)
# }
#
# add_continent <- function(d) {
#   d %>%
#     left_join(
#       ots_countries %>%
#         select(reporter_iso = country_iso, reporter_continent = continent_name_english),
#       by = "reporter_iso"
#     ) %>%
#     left_join(
#       ots_countries %>%
#         select(partner_iso = country_iso, partner_continent = continent_name_english),
#       by = "partner_iso"
#     )
# }
#
# # five_years_interval <- function(d) {
# #   d %>%
# #     mutate(
# #       year_f = case_when(
# #         # year %in% 1980:1984 ~ "80-85",
# #         # year %in% 1985:1989 ~ "85-89",
# #         # year %in% 1990:1994 ~ "90-94",
# #         # year %in% 1995:1999 ~ "95-99",
# #         year %in% 2000:2004 ~ "00-04",
# #         year %in% 2005:2009 ~ "05-09",
# #         year %in% 2010:2014 ~ "10-14",
# #         year %in% 2015:2019 ~ "15-19"
# #       )
# #     )
# # }
#
# filter_inter_quantile_range <- function(d) {
#   Q   <- quantile(d$cif_fob_ratio, probs = c(.25, .75), na.rm = FALSE)
#   iqr <- IQR(d$cif_fob_ratio)
#   up  <- Q[2] + 1.5 * iqr # Upper Range
#   low <- Q[1] - 1.5 * iqr # Lower Range
#
#   d %>%
#     filter(
#       cif_fob_ratio > low & cif_fob_ratio < up
#     )
# }
