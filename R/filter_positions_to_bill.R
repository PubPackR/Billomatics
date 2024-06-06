
#' filter_positions2bill
#'
#' @param sap_posten_confirmation The dataframe containing the amounts that have been billed
#' @param all_positions The dataframe containing all positions
#' @return The function returns a dataframe containing all positions to bill
#' @export
filter_positions2bill <- function(sap_posten_confirmation, all_positions) {

  last_posting_date <- sap_posten_confirmation %>%
    group_by(confirmation_number) %>%
    summarise(last_posting_date = max(Posting.Date))

  all_positions %>%
    left_join(last_posting_date, by = join_by("Kundenreferenz" == "confirmation_number")) %>%
    filter(Startdatum_Vertrag > last_posting_date, Startdatum_Vertrag <= today())

}
