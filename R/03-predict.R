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
