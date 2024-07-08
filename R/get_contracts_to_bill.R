#' check_contract_starts
#'
#' @param df_current_contracts The df with the contracts, each contract and product has on row
#' @param reale_starts_df The df which contains the real running times, given the campaigns started
#' @param this_period this should be a lubridate interval in which the period of starts is given
#' @param planned_start when true it returns campaigns based on the contracted start date, when false for the real starting date
#' @return The function returns the all the contracts that started
#'
#' @export
check_contract_starts <- function (df_current_contracts,
                                   df_reale_starts,
                                   this_period,
                                   planned_start = TRUE) {

  df_current_contracts<- df_current_contracts  %>%
    dplyr::filter(Laufzeit_Start %within% this_period) %>%
    dplyr::left_join(
      df_reale_starts %>%
        tidyr::drop_na(tatsächlicherStart) %>%
        dplyr::group_by(confirmation_number,Laufzeit_Start) %>%
        dplyr::slice_min(tatsächlicherStart) %>%
        dplyr::distinct(confirmation_number,Laufzeit_Start,tatsächlicherStart),
      by = c("confirmation_number",
             "Laufzeit_Start")
    ) %>%
    dplyr::mutate(delayed = tatsächlicherStart > Laufzeit_Start|is.na(tatsächlicherStart))

  if (planned_start) {

    df_current_contracts %>%
      dplyr::filter(Laufzeit_Start %within% this_period)

  } else {

    df_current_contracts %>%
      dplyr::filter(tatsächlicherStart %within% this_period)
  }
}

#' get_confirmations_2_bill
#' @param df_reale_starts The df which contains the real running times, given the campaigns started
#' @param df_confirmations The df with the confirmations
#' @param df_invoices The df with the invoices
#' @param df_confirmation_items The df with confirmation items
#' @param df_confirmation_number_bill_date The df from asana that contains the number of bills per confirmation
#' @param billomatDB The billomat DB location
#' @param encryption_key_db The key to decrypt the billomat db
#' @param create_delayed_7_days True when a confirmation should get an invoice when at least 7 days old.
#'  When false the confirmations will be filtered depending on the Starting date of the campaign
#' @return The function returns the all the confirmations that need to get an invoice draft.
#'  Confirmations that include CPC, are split or already have an invoice are excluded. These confirmations
#'  should be handled manually.
#'
#' @export
get_confirmations_2_bill <- function(df_reale_starts,
                                     df_confirmations,
                                     df_invoices,
                                     df_confirmation_items,
                                     df_confirmation_number_bill_date,
                                     billomatDB,
                                     encryption_key_db = keys$billomat[1],
                                     create_delayed_7_days = FALSE) {

  df_reale_starts <- df_reale_starts %>%
    dplyr::ungroup() %>%
    dplyr::group_by(confirmation_number) %>%
    dplyr::slice_min(Laufzeit_Start,with_ties = FALSE) %>%
    # I only keep contracts that have started after 1.1.2024
    dplyr::filter(Laufzeit_Start >= lubridate::floor_date(lubridate::today(), unit = "years"),
           # I only keep contracts that have started until the end of this month
           Laufzeit_Start <= lubridate::ceiling_date(lubridate::today(), unit = "weeks") - lubridate::days(1))

  # find all split bills
  split_bills <- df_confirmation_number_bill_date %>%
    dplyr::filter(Anzahl_Rechnungen > 1)

  # find all cpc campaigns that have to be billed manually
  cpc_confirmations <- df_confirmation_items %>%
    dplyr::filter(article_id == "1248678" | stringr::str_detect(description,"CPC|Cost per")) %>%
    filter(total_net == 0) %>%
    dplyr::distinct(confirmation_id,.keep_all = TRUE)

  if (create_delayed_7_days) {
    df_confirmations <- df_confirmations %>%
      dplyr::filter(date >= today() - days(7),
             status == "COMPLETED")
  } else {

    df_confirmations <- df_confirmations %>%
      dplyr::filter(confirmation_number %in% df_reale_starts$confirmation_number,
             status == "COMPLETED")

  }
  # find all invoices that are part of a confirmation
  df_invoices <- df_invoices %>%
    dplyr::filter(status %in% c("DRAFT","OPEN","PAID","OVERDUE")) %>%
    dplyr::filter(confirmation_id %in% df_confirmations$id)

  # remove all confirmations that already have >= 1 invoice

  df_confirmations <- df_confirmations %>%
    dplyr::filter(!id %in% df_invoices$confirmation_id) %>%

    # remove all confirmations that include CPC positions
    dplyr::filter(!id %in% cpc_confirmations$id) %>%

    # remove all confirmations that are split
    dplyr::filter(!confirmation_number %in% split_bills$confirmation_number)

  df_confirmations
}


#' get_cpc_document
#'
#'@param df_items the df with the items which can contain a cpc product
#'@param document_type The type of main document (invoice, confirmation).
#'@param field The field the information about the product is stored.
#'
#'@return The function returns all document_ids that contain a CPC product
#'
#'@export
get_cpc_document <- function(df_items,
                          document_type = "confirmation",
                          field = "description") {

  df_items <- df_items %>%
    dplyr::mutate(document_id = get(paste0(document_type,"_id")))

  df_items %>%
    dplyr::filter(article_id == "1248678" |
             stringr::str_detect(get(field), "CPC|Cost per")) %>%
    dplyr::distinct(document_id, .keep_all = TRUE)
}
