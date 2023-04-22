# Internal functions ----

#' Add CIF/FOB ratios to trade dataset
#' @param d dataset to add CIF/FOB ratios to
#' @param unit_values compute ratios after obtaining traded values in dollars
#'     per kilogram (defaults to FALSE)
#' @export
compute_ratios <- function(d, unit_values = FALSE) {
  d <- d %>%

    mutate(
      cif_fob_ratio = !!sym("trade_value_usd_imp") / !!sym("trade_value_usd_exp")
    ) %>%

    mutate(
      cif_fob_ratio_ok = ifelse(
        !!sym("cif_fob_ratio") >= 1 & is.finite(!!sym("cif_fob_ratio")), 1L, 0L)
    ) %>%

    # The unit or net CIF/FOB ratios can be weighted, or not, by the gap between
    # reported mirror quantities Min(Xij,Mji) / Max(Xij,Mji)
    mutate(
      cif_fob_weights = pmin(!!sym("trade_value_usd_imp"),
                             !!sym("trade_value_usd_exp"), na.rm = T) /
        pmax(!!sym("trade_value_usd_imp"),
             !!sym("trade_value_usd_exp"), na.rm = T)
    )

  if(isTRUE(unit_values)) {
    d <- d %>%
      mutate(
        cif_fob_ratio_unit = (!!sym("trade_value_usd_imp") / !!sym("qty_imp")) /
          (!!sym("trade_value_usd_exp") / !!sym("qty_exp"))
      ) %>%
      mutate(
        cif_fob_weights_unit = pmin((!!sym("trade_value_usd_imp") / !!sym("qty_imp")),
                                    (!!sym("trade_value_usd_exp") / !!sym("qty_exp")), na.rm = T) /
          pmax((!!sym("trade_value_usd_imp") / !!sym("qty_imp")),
               (!!sym("trade_value_usd_exp") / !!sym("qty_exp")), na.rm = T)
      ) %>%
      mutate(
        cif_fob_ratio_unit_ok = ifelse(
          !!sym("cif_fob_ratio_unit") >= 1 & is.finite(!!sym("cif_fob_ratio_unit")), 1L, 0L)
      )
  }

  return(d)
}

open_dataset_partitioning <- function(path) {
  open_dataset(path,
               partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))
}

#' @importFrom dplyr ungroup summarise
data_partitioned <- function(y, f, replace_unspecified_iso, include_qty,
                             aggregation_digits = 6,
                             path = "hs-rev2002/parquet",
                             path2 = "hs-rev2012/parquet") {
  cat("reading data.")

  if (y >= 2012) {
    hs02_count <- count_countries(path, y)
    hs12_count <- count_countries(path2, y)

    hs02_count <- length(unique(c(hs02_count$reporter_iso, hs02_count$partner_iso)))
    hs12_count <- length(unique(c(hs12_count$reporter_iso, hs12_count$partner_iso)))

    d <- if (hs12_count >= hs02_count) {
      open_dataset_partitioning(path2)
    } else {
      open_dataset_partitioning(path)
    }

    use_hs12 <- ifelse(hs12_count >= hs02_count, TRUE, FALSE)
    cat(".")
  } else {
    d <- open_dataset_partitioning(path)
    use_hs12 <- FALSE
    cat(".")
  }

  d <- d %>%
    filter(
      !!sym("year") == y,
      !!sym("trade_flow") == f,
      !!sym("aggregate_level") == aggregation_digits
    )

  if (isTRUE(include_qty)) {
    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("reporter_code"),
             !!sym("partner_code"), !!sym("commodity_code"),
             !!sym("qty_unit"), !!sym("qty"), !!sym("trade_value_usd"))
  } else {
    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("reporter_code"),
           !!sym("partner_code"), !!sym("commodity_code"),
           !!sym("trade_value_usd"))
  }

  d <- d %>%
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

    if (aggregation_digits == 4) {
      product_correlation_sbst <- product_correlation_sbst %>%
        mutate_if(is.character, function(x) { substr(x, 1, 4) }) %>%
        arrange(!!sym("hs12")) %>%
        distinct(!!sym("hs02"), .keep_all = T)
    }

    cat(".")

    d <- d %>%
      left_join(product_correlation_sbst, by = c("commodity_code" = "hs02"))
    cat(".")

    if (isTRUE(include_qty)) {
      d <- d %>%
        select(!!sym("reporter_iso"), !!sym("partner_iso"),
               commodity_code = !!sym("hs12"),
               !!sym("qty_unit"), !!sym("qty"),
               !!sym("trade_value_usd"))
    } else {
      d <- d %>%
        select(!!sym("reporter_iso"), !!sym("partner_iso"),
               commodity_code = !!sym("hs12"),
               !!sym("trade_value_usd"))
    }
  }

  if (isTRUE(include_qty)) {
    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"),
             !!sym("commodity_code"),
             !!sym("qty_unit"), !!sym("qty"),
             !!sym("trade_value_usd"))
  } else {
    d <- d %>%
      select(!!sym("reporter_iso"), !!sym("partner_iso"),
             !!sym("commodity_code"),
             !!sym("trade_value_usd"))
  }

  d <- d %>%
    fix_missing_commodity() %>%
    mutate(year = y)

  cat(".\n")

  # missing_codes_count <- d %>%
  #   ungroup() %>%
  #   filter(is.na(commodity_code)) %>%
  #   count() %>%
  #   pull()
  #
  # stopifnot(missing_codes_count == 0)

  cat("summarizing...\n")

  if (isFALSE(include_qty)) {
    d <- d %>%
      select(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso"),
             !!sym("commodity_code"),
             !!sym("trade_value_usd"))
  }

  d <- d %>%
    fix_missing_commodity(aggregation_digits)

  if (isTRUE(include_qty)) {
    d <- d %>%
      group_by(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso"),
               !!sym("qty_unit"), !!sym("commodity_code")) %>%
      summarise(
        trade_value_usd = sum(!!sym("trade_value_usd"), na.rm = T),
        qty = sum(!!sym("qty"), na.rm = T)
      ) %>%
      ungroup()

    dmis <- d %>%
      group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
      count() %>%
      filter(!!sym("n") > 1) %>%
      select(-!!sym("n"))

    dmis <- d %>%
      inner_join(dmis) %>%
      mutate(
        qty_unit = "mismatching units",
        qty = NA
      ) %>%
      select(-!!sym("year")) %>%
      group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code"),
               !!sym("qty_unit")) %>%
      summarise_if(is.numeric, function(x) sum(x, na.rm = TRUE)) %>%
      ungroup()

    d <- d %>%
      anti_join(
        dmis %>%
          select(-!!sym("qty_unit"), -!!sym("trade_value_usd"))
      ) %>%
      bind_rows(dmis)

    rm(dmis)
  } else {
    d <- d %>%
      group_by(!!sym("year"), !!sym("reporter_iso"), !!sym("partner_iso"),
               !!sym("commodity_code")) %>%
      summarise_if(is.numeric, function(x) sum(x, na.rm = TRUE)) %>%
      ungroup()
  }

  return(d)
}

#' @importFrom dplyr full_join
join_flows <- function(dimp, dexp, include_qty, for_imputation = F) {
  if (isFALSE(for_imputation)) {
    d <- dimp %>%
      full_join(dexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      rename(trade_value_usd_imp = !!sym("trade_value_usd.x"), trade_value_usd_exp = !!sym("trade_value_usd.y"))
  }

  if (isTRUE(for_imputation)) {
    d <- dimp %>%
      full_join(dexp, by = c("reporter_iso" = "partner_iso", "partner_iso" = "reporter_iso", "commodity_code")) %>%
      rename(trade_value_usd_imp = !!sym("trade_value_usd.x"), trade_value_usd_exp = !!sym("trade_value_usd.y"))
  }

  if (isTRUE(include_qty)) {
    d <- d %>%
      rename(qty_imp = !!sym("qty.x"), qty_unit_imp = !!sym("qty_unit.x"),
             qty_exp = !!sym("qty.y"), qty_unit_exp = !!sym("qty_unit.y"))
  }

  d <- d %>%
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

fix_missing_commodity <- function(d, aggregation_digits = 6) {
  c99 <- ifelse(aggregation_digits == 4, "9999", "999999")

  d %>%
    mutate(
      commodity_code = case_when(
        is.na(!!sym("commodity_code")) ~ c99,
        TRUE ~ !!sym("commodity_code")
      )
    )
}

#' @importFrom dplyr left_join
add_hs_section <- function(d) {
  d %>%
    left_join(
      tradestatistics::ots_commodities %>%
        select(!!sym("commodity_code"), !!sym("section_code")),
      by = "commodity_code"
    ) %>%
    mutate(section_code = ifelse(is.na(!!sym("section_code")), "99", !!sym("section_code")))
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
subtract_re_imp_exp <- function(d, include_qty) {
  d <- d %>%
    mutate_if(is.numeric, function(x) ifelse(is.na(x), 0, x)) %>%
    mutate_if(is.numeric, function(x) ifelse(!is.finite(x), NA, x))

  if (isTRUE(include_qty)) {
    d <- d %>%
      mutate(
        trade_value_usd = pmax(!!sym("trade_value_usd.x") - !!sym("trade_value_usd.y"), 0, na.rm = T),
        qty = pmax(!!sym("qty.x") - !!sym("qty.y"), 0, na.rm = T),
        qty_unit = case_when(
          !!sym("qty_unit.x") == !!sym("qty_unit.y") ~ !!sym("qty_unit.x"),
          (!!sym("qty_unit.x") != !!sym("qty_unit.y")) &
            (!!sym("trade_value_usd.y") > 0) ~ "mismatching units",
          TRUE ~ !!sym("qty_unit.x")
        )
      ) %>%
      select(-ends_with(".x"), -ends_with(".y"))
  } else {
    d <- d %>%
      mutate(
        trade_value_usd = pmax(!!sym("trade_value_usd.x") - !!sym("trade_value_usd.y"), 0, na.rm = T)
      ) %>%
      select(-ends_with(".x"), -ends_with(".y"))
  }

  return(d)
}

#' Add bilateral distances to trade dataset
#' @param d dataset to add distances to
#' @param y year to subset gravity variables for the join operation
#' @param path where to find distances dataset
#' @importFrom rlang is_installed sym
#' @importFrom dplyr tbl collect filter inner_join select
#' @importFrom utils data
#' @export
add_gravity_cols <- function(d, y = NULL, path = "attributes") {
  stopifnot(isTRUE(is_installed("usitcgravity")))
  stopifnot(isTRUE(is_installed("duckdb")))

  con <- usitcgravity::usitcgravity_connect()

  gravity <- tbl(con, "gravity") %>%
    filter(!!sym("year") == y) %>%
    collect() %>%
    select(-c("year", "dynamic_code_o", "dynamic_code_d")) %>%
    mutate(
      iso3_o = tolower(!!sym("iso3_o")),
      iso3_d = tolower(!!sym("iso3_d"))
    )

  duckdb::dbDisconnect(con, shutdown=TRUE)

  d <- d %>%
    inner_join(
      gravity,
      by = c("reporter_iso" = "iso3_d", "partner_iso" = "iso3_o")
    )

  return(d)
}

#' Filter trade dataset based on IQR
#' @param d dataset to filter
#' @param column column to filter (cif_fob_ratio or cif_fob_ratio_unit)
#' @importFrom stats IQR quantile
#' @export
filter_inter_quantile_range <- function(d, column = "cif_fob_ratio") {
  if (column == "cif_fob_ratio") {
    Q   <- quantile(d$cif_fob_ratio, probs = c(.25, .75), na.rm = FALSE)
    iqr <- IQR(d$cif_fob_ratio)
    up  <- Q[2] + 1.5 * iqr # Upper Range
    low <- Q[1] - 1.5 * iqr # Lower Range

    d <- d %>%
      filter(
        !!sym("cif_fob_ratio") > low & !!sym("cif_fob_ratio") < up
      )
  } else {
    Q   <- quantile(d$cif_fob_ratio_unit, probs = c(.25, .75), na.rm = FALSE)
    iqr <- IQR(d$cif_fob_ratio_unit)
    up  <- Q[2] + 1.5 * iqr # Upper Range
    low <- Q[1] - 1.5 * iqr # Lower Range

    d <- d %>%
      filter(
        !!sym("cif_fob_ratio_unit") > low & !!sym("cif_fob_ratio_unit") < up
      )
  }

  return(d)
}

#' Filter units in kilograms
#' @param d dataset with traded values
#' @param ton how many kilograms is a ton (1000 = BACI, 1016.0469 = UK, 907.18474 = USA)
filter_kg <- function(d, ton = 1000) {
  d %>%
    filter(
      !!sym("qty_unit_exp") == "weight in kilograms" &
        !!sym("qty_unit_imp") == "weight in kilograms",
      !!sym("trade_value_usd_exp") > 10 & !!sym("trade_value_usd_imp") > 10,
      !!sym("qty_exp") > 2 * ton & !!sym("qty_imp") > 2 * ton
    )
}
