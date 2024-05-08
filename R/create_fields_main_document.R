#' create_document_level_fields
#' This function takes a dataframe with the positions and creates the fields for the template
#' @param df_positions The dataframe with all the positions
#' @param bills_created_from What are the bills created from? invoice or confirmation.
#' @return The function returns a df with all the reported clicks in the comment
#' @export
create_document_level_fields <- function(df_positions,
                                         bills_created_from) {

  if (bills_created_from == "invoice") {

    df_positions %>%
      mutate(
        ist_Gutschrift = !is.na(invoice_id),
        Belegnummer_documentno = as.integer(str_remove_all(invoice_number, pattern = "[:Alpha:]")),
        # checking if it is a gutschrift, which would be Korrekturrechnung
        Auftragsart_order_type = if_else(is.na(invoice_id), "ZLRA", "ZGRA"),
        Referenz = confirmation_number, ## here maybe the old invoice number too? How do we get this into the jp5?,
        Kundenreferenz = confirmation_number,
        Zuordnung_18__assignment = confirmation_number
      )
  } else {

    df_positions <- df_positions %>%
      mutate(
        Belegnummer_documentno = as.integer(
          str_remove_all(confirmation_number, pattern = "[:Alpha:]")
        ),
        # checking if it is a gutschrift, which would be Korrekturrechnung
        Auftragsart_order_type =  "ZLRA",
        Referenz = confirmation_number,
        Kundenreferenz = confirmation_number,
        Zuordnung_18__assignment = confirmation_number
      )
  }

}
