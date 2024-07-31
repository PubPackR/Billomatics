
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
    mutate(label = if_else(id %in% invoices_checked_df$id & !str_detect(label,"/ checked"), paste(label,"/ checked"),
                           stringr::str_remove_all(label,"\\/ checked")),
           is_checked = id %in% invoices_checked_df$id)

}


#' put_invoice_value
#'
#' This function puts a value in a selected field via Billomat API for each invoice in a df
#'
#' @param billomat_api_key The Billomat API key
#' @param invoice_draft_df The df containing the invoices
#' @param field The field (e.g., "label", "intro", etc.) that should be put to Billomat
#'
#' @export
put_invoice_value <- function(billomat_api_key, invoice_draft_df, field) {

  billomat_id <- "k16917022"

  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/json")

  entity_id <- invoice_draft_df$id

  field_text <- invoice_draft_df[[field]]

  for (i in 1:nrow(invoice_draft_df)) {

    api_endpoint <- paste0("https://", billomat_id, ".billomat.net/api/invoices/", entity_id[i])

    body_values <- list()
    body_values[["invoices"]][[field]] <- field_text[i]

    response <- httr::PUT(url = api_endpoint,
                          httr::add_headers(header),
                          body = jsonlite::toJSON(body_values),
                          httr::accept("application/json")
    )

    content <- httr::content(response)

    print(paste("Der Statuscode:", response[["status_code"]]))

    if(response[["status_code"]] != 200){
      print(content[["errors"]])
    }
  }
}
