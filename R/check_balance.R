
#' set_label_invoice
#'
#' @param invoices_checked_df The df with the checked invoices, this means that all invoices for a contract
#' have are in sum equal to the contracts total net
#' @param invoice_draft_df The df which should be processed to add the label check or remove the label check
#' @return The function returns the invoice_draft_df with the mutated label
#'
#' @export
set_label_invoice <- function(invoices_checked_df,
                              invoice_draft_df) {

  invoice_draft_df %>%
    ## add the checked to the label
    mutate(label = if_else(id %in% invoices_checked_df$id, paste(label,"/ checked"),
                           stringr::str_remove_all(label,"\\/ checked")),
           is_checked = id %in% invoices_checked_df$id)

}
