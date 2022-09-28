fobization_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        fitted_cif_ratio >= 1 ~ "fobization",
        fitted_cif_ratio < 1 | is.na(fitted_cif_ratio) | !is.finite(fitted_cif_ratio) ~ "no fobization"
      ),
      fobization = case_when(
        is.na(dist) ~ "no fobization",
        TRUE ~ fobization
      )
    )
}

#' @importFrom tidyr drop_na pivot_wider
add_valuation <- function(d, y) {
  d %>%
    left_join(
      readRDS("models/trade_valuation_system_per_country.rds") %>%
        filter(!is.na(!!sym("iso3_digit_alpha")), !!sym("year") == y) %>%
        select(!!sym("year"), reporter_iso = !!sym("iso3_digit_alpha"),
               !!sym("trade_flow"), !!sym("valuation")) %>%
        drop_na() %>%
        distinct(!!sym("year"), !!sym("reporter_iso"), !!sym("trade_flow"),
                 .keep_all = T) %>%
        # group_by(year, reporter_iso, trade_flow, valuation) %>%
        # count() %>%
        # filter(n > 1) %>%
        pivot_wider(names_from = !!sym("trade_flow"),
                    values_from = !!sym("valuation")) %>%
        rename(exports_report = !!sym("Export"),
               imports_report = !!sym("Import")),
      by = c("year", "reporter_iso")
    )
}

fobization_system_val <- function(d) {
  d %>%
    mutate(
      fobization = case_when(
        imports_report != "CIF" ~ "no fobization",
        exports_report != "FOB" ~ "no fobization",
        TRUE ~ fobization
      )
    )
}

#' Conciliate flows vs mirrored flows
#' @param y which year to process
#' @param subtract_re substract re-imports/exports from data
#' @param filter_kg remove observations not expressed in kilograms
#'     (i.e., not available, liters, etc)
#' @param unit_values obtain traded unit values (i.e. kilograms per dollar)
#' @param replace_unspecified_iso convert all ocurrences of `0-unspecified` to
#' `e-[0-9]` (i.e., `e-439` when corresponding)
#' @param path the route to the folder with the parquet files for HS02 data
#' @param path2 the route to the folder with the parquet files for HS12 data
#' @importFrom forcats as_factor
#' @importFrom dplyr starts_with
#' @export
conciliate_flows <- function(y,
                             subtract_re = FALSE,
                             filter_kg = FALSE,
                             unit_values = FALSE,
                             include_qty = FALSE,
                             replace_unspecified_iso = TRUE,
                             path = "../uncomtrade-datasets-arrow/hs-rev2002/parquet/",
                             path2 = "../uncomtrade-datasets-arrow/hs-rev2012/parquet/") {
  # read ----
  cat("Exports...\n")
  dexp <- data_partitioned(y, f = "export", path = path, path2 = path2,
                           replace_unspecified_iso = replace_unspecified_iso,
                           include_qty = include_qty)

  # dexp %>% group_by(qty_unit) %>% count()

  # dexp %>%
  #   group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
  #   count() %>%
  #   filter(n > 1)

  cat("Imports...\n")
  dimp <- data_partitioned(y, f = "import", path = path, path2 = path2,
                           replace_unspecified_iso = replace_unspecified_iso,
                           include_qty = include_qty) %>%
    select(-!!sym("year"))

  # dimp %>% group_by(qty_unit) %>% count()

  # dimp %>%
  #   group_by(!!sym("reporter_iso"), !!sym("partner_iso"), !!sym("commodity_code")) %>%
  #   count() %>%
  #   filter(n > 1)

  if (isTRUE(subtract_re)) {
    cat("Re-Exports...\n")
    dreexp <- data_partitioned(y, f = "re-export",
                               replace_unspecified_iso = replace_unspecified_iso,
                               include_qty = include_qty,
                               path = path, path2 = path2) %>%
      select(-!!sym("year"))

    # dreexp %>% group_by(qty_unit) %>% count()

    cat(".")
    dexp <- dexp %>%
      left_join(dreexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      subtract_re_imp_exp(include_qty = include_qty)
    # dexp %>% group_by(qty_unit) %>% count()
    cat(".")
    rm(dreexp)
    cat(".\n")

    cat("Re-Imports...\n")
    dreimp <- data_partitioned(y, f = "re-import",
                               replace_unspecified_iso = replace_unspecified_iso,
                               include_qty = include_qty,
                               path = path, path2 = path2) %>%
      select(-!!sym("year"))

    cat(".")
    dimp <- dimp %>%
      left_join(dreimp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      subtract_re_imp_exp(include_qty = include_qty)
    cat(".")
    rm(dreimp)
    cat(".\n")
  }

  # join mirrored flows ----

  # we do a full join, so all trade flows reported either by the importer or the
  # exporter are present in our intermediary datasets
  cat("Joining flows...\n")
  dexp <- join_flows(dimp = dimp, dexp = dexp, include_qty = include_qty)
  dexp <- dexp %>%
    mutate(year = y)
  rm(dimp); gc()

  # work on obs. with kilograms ----

  # NO NEED TO RUN THIS COMMENTED SECTION, IT WAS RUN ONCE TO EXTRACT THE UNIQUE
  # CASES
  # units <- purrr::map_df(
  #   1988:2019,
  #   function(t) {
  #     open_dataset("../uncomtrade-datasets-arrow/hs-rev1992/parquet",
  #       partitioning = c("year", "trade_flow", "reporter_iso")) %>%
  #       filter(
  #         year == t,
  #         trade_flow == "import",
  #         aggregate_level == 4) %>%
  #       select(qty_unit) %>%
  #       collect() %>%
  #       distinct()
  #   }
  # )
  #
  # units <- units %>% distinct() %>% pull()
  #
  # [1]  "no quantity"
  # "number of items"
  # [3]  "number of pairs"
  # "area in square metres"
  # [5]  "weight in kilograms"
  # "length in metres"
  # [7]  "volume in litres"
  # "electrical energy in thousands of kilowatt-hours"
  # [9]  "weight in carats"
  # "volume in cubic meters"
  # [11] "thousands of items"
  # "dozen of items"
  # [13] "number of packages"

  # 1) Select the trade flows for which there is a quantity available,
  # expressed in kg by the importer and the exporter, and for which value is
  # above 10$ and quantity above 2 tonnes.

  n <- nrow(dexp)

  # dexp %>%
  #   group_by(!!sym("qty_unit_exp")) %>%
  #   count()

  if (isTRUE(filter_kg)) {
    dexp <- filter_kg(dexp)
  }

  # convert HS02 to HS12 ----

  if (y < 2012) {
    cat("Converting HS codes.")
    hs02_to_hs12 <- uncomtrademisc::product_correlation %>%
      select(!!sym("hs02"), !!sym("hs12")) %>%
      arrange(!!sym("hs12")) %>%
      distinct(!!sym("hs02"), .keep_all = T)

    cat(".")
    dexp <- dexp %>%
      left_join(hs02_to_hs12, by = c("commodity_code" = "hs02")) %>%
      select(-!!sym("commodity_code")) %>%
      rename(commodity_code = !!sym("hs12")) %>%
      mutate(
        commodity_code = case_when(
          is.na(!!sym("commodity_code")) ~ "999999",
          TRUE ~ !!sym("commodity_code")
        )
      )

    cat(".")
    dexp <- dexp %>%
      group_by(!!sym("year"), !!sym("reporter_iso"),
               !!sym("partner_iso"), !!sym("commodity_code")) %>%
      summarise(
        trade_value_usd_exp = sum(!!sym("trade_value_usd_exp"), na.rm = T),
        trade_value_usd_imp = sum(!!sym("trade_value_usd_imp"), na.rm = T),
        qty_exp = sum(!!sym("qty_exp"), na.rm = T),
        qty_imp = sum(!!sym("qty_imp"), na.rm = T)
      ) %>%
      ungroup()
  }

  # obtain unit values ----

  # 2) Compute unit values as reported by the exporter
  # See the comments in the helper function

  cat("Obtaining unit values...")
  dexp <- dexp %>%
    compute_ratios(unit_values = unit_values) %>%
    select(!!sym("year"), !!sym("reporter_iso"),
           !!sym("partner_iso"), !!sym("commodity_code"),
           starts_with("uv_"), starts_with("cif_fob_")) %>%
    mutate(year = as_factor(!!sym("year")))

  unique_pairs <- dexp %>%
    select(!!sym("reporter_iso"), !!sym("partner_iso"),
           !!sym("commodity_code")) %>%
    distinct() %>%
    nrow()

  # dexp %>%
  #   group_by(!!sym("reporter_iso"), !!sym("partner_iso"),
  #          !!sym("commodity_code")) %>%
  #   count() %>%
  #   filter(n > 1)

  stopifnot(unique_pairs == nrow(dexp))

  dexp <- dexp %>%
    add_gravity_cols() %>%
    # add_rta_col(y) %>%
    mutate(
      contig = as.integer(!!sym("contig")),
      comlang_off = as.integer(!!sym("comlang_off")),
      colony = as.integer(!!sym("colony"))
    ) %>%
    rename(
      exporter_iso = !!sym("reporter_iso"),
      importer_iso = !!sym("partner_iso")
    )

  p <- nrow(dexp) / n
  message(paste("Proportion of filtered rows / total rows", p))

  return(list(
    year = y,
    initial_rows = n,
    final_rows = nrow(dexp),
    proportion = p,
    data = dexp
  ))
}

#' @importFrom dplyr rowwise
impute_trade <- function(d, compare_flows = TRUE) {
  d <- d %>%
    mutate(trade_value_usd_imp_fob = !!sym("trade_value_usd_imp") /
             !!sym("fitted_cif_ratio"))

  if (compare_flows) {
    d <- d %>%
      rowwise() %>%
      mutate(
        direct_mismatch = abs(!!sym("trade_value_usd_imp") -
                                !!sym("trade_value_usd_exp")),
        fobization_mismatch = abs(!!sym("trade_value_usd_imp_fob") -
                                    !!sym("trade_value_usd_exp")),
      ) %>%
      ungroup() %>%

      mutate(
        fobization = case_when(
          fobization_mismatch <= direct_mismatch ~ "fobization",
          TRUE ~ "no fobization"
        )
      )
  }

  d %>%
    mutate(
      trade_value_usd_imp_imputed = case_when(
        fobization == "fobization" ~ trade_value_usd_imp_fob,
        TRUE ~ trade_value_usd_imp
      )
    )
}
