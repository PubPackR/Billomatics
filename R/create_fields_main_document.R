### this script contains all functions that are used to format the data according to the template

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

#' create_header_billing_info_text
#'
#' this function uses are table with all fields for billing information and creates a wide table to show it on the billing doc
#' @param df_positions the positions that need to get the billing information attached to
#' @param df_information_bill The df containing the billing information after comments and intro/ note were consolidated
#' @return The function returns the information added to the fields and shortened to 50 characters
#'
#' @export
create_header_billing_info_text <- function(df_positions,
                                            df_information_bill) {

  #### truncating the fields length
  fields_with_billing_information <-  df_information_bill %>%
    filter(!str_detect(key, "Versand|ddresse|Zahlungsziel")) %>%
    mutate(key = str_replace_all(
      key,
      pattern = c(
        "Ansprechpartner" = "ASP",
        "Auftrags" = "Auftr.",
        "Kostenstelle" = "Kstst.",
        "Leistungsempfänger" = "Lstgsempf.",
        "Marketing für" = "Marketing f."
      )
    ))

    fields_with_billing_information <-
      fields_with_billing_information %>%
      group_by(document_id) %>%
      arrange(-desc(key)) %>%
      mutate(
        field_content = paste0(key, ":", value),
        field_content = str_trunc(field_content, 49, "right", ellipsis = "."),
        field_length = nchar(field_content),
        field_name = paste0(
          "Kopftext_",
          row_number() + 4,
          "_50__header_text_",
          row_number() + 4
        )
      ) %>%
      pivot_wider(id_cols = "document_id",
                  names_from = field_name,
                  values_from = field_content)


    df_positions %>%
      left_join(fields_with_billing_information,
                by = c("document_id"))
}


