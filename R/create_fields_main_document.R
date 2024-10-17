### this script contains all functions that are used to format the data according to the template

#' get_alternative_debitor
#' This function takes the billing information provided and extracts the Debitor or deviating recipient
#' @param df_positions The dataframe with all the positions
#' @param Information_bill_df The dataframe containing the information about billing details from comments and document
#' @return The function returns a df with all Debitors and their document id
#' @export
get_alternative_debitor <- function(Information_bill_df) {
  # If a new debitor alternative main debitor is passed via the comments
  if("Debitor"  %in% unique(Information_bill_df$key)) {
    Information_bill_df %>%
      filter(key == "Debitor") %>%
      pivot_wider(id_cols = document_id,
                  values_from = value,
                  values_fn = function(x) unique(x),
                  names_from = key) %>%
      mutate(Debitor = as.numeric(Debitor)) } else {
        tibble(Debitor = as.numeric(NA),
               document_id = as.numeric(NA))
      }
}


#' get_deviating_invoice_recipient
#' This function takes the billing information provided and extracts the Debitor or deviating recipient
#' @param df_positions The dataframe with all the positions
#' @param Information_bill_df The dataframe containing the information about billing details from comments and document
#' @return The function returns a df with all deviating Debitors and their document id
#' @export
get_deviating_invoice_recipient <- function(Information_bill_df) {

  Information_bill_df <- Information_bill_df %>%
    mutate(key = str_replace_all(key,"Abweichender","abweichender"))

  if("abweichender Rechnungsempfänger" %in% unique(Information_bill_df$key)  ) {
    # If a deviating invoice recipient besided the main debitor is passed via the comments
    Information_bill_df %>%
      filter(key == "abweichender Rechnungsempfänger") %>%
      pivot_wider(id_cols = document_id,
                  values_from = value,
                  names_from = key) %>%
      mutate(`abweichender Rechnungsempfänger` = as.numeric(`abweichender Rechnungsempfänger`))
  } else {
    tibble(`abweichender Rechnungsempfänger` = as.numeric(NA),
           document_id = as.numeric(NA))
  }
}


#' create_invoice_recipient
#' This function takes the billing information provided and extracts the Debitor or deviating recipient
#' @param df_positions The dataframe with all the positions
#' @param varname_dev_invoice_recipient 'string' with the name of the variable were the deviating deb number is stored
#' @param varname_debitor 'string' with the name of the variable where the main deb num is stored
#' @return The function returns a df with all deviating Debitors and their document id
#' @export
create_invoice_recipient <- function(df_positions,
                                     varname_dev_invoice_recipient = "deviating_invoice_recipient",
                                     varname_debitor = "Debitor") {
  df_positions %>%
    mutate(document_id = as.character(document_id),
           Auftraggeber_customer = as.numeric(.data[[varname_debitor]]),
           Rechnungs_empfänger_billto_party = coalesce(as.numeric(.data[[varname_dev_invoice_recipient]]),
                                                       as.numeric(.data[[varname_debitor]])))
}



#' create_document_level_fields
#' This function takes a dataframe with the positions and creates the fields for the template
#' @param df_positions The dataframe with all the positions
#' @param bills_created_from What are the bills created from? invoice or confirmation.
#' @return The function returns a df with all the reported clicks in the comment
#' @export
create_document_level_fields <- function(df_positions,
                                         bills_created_from) {

  if (bills_created_from %in% c("invoice","credit_note")) {

    df_positions %>%
      mutate(
        ist_Gutschrift = !is.na(invoice_id),
        Belegnummer_documentno = as.integer(str_remove_all(invoice_number, pattern = "[:Alpha:]")),
        # checking if it is a gutschrift, which would be Korrekturrechnung
        Auftragsart_order_type = if_else(is.na(invoice_id), "ZLRA", "ZGRA"),
        Referenz = document_id, ## Here we use the document id of the invoice in billomat - this is unique
        Kundenreferenz = reference_customer, # as this can also the number of a previous bill I have to adjust this before creating the fields
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
        Referenz =  format(Sys.time(), "%y%m%d%H%M"), ## here we use the timestamp as document_id of confirmations can be entered multiple times
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
    filter(!str_detect(key, "Versand|ddresse|Zahlungsziel|Rechnungsempfänger|Debitor")) %>%
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


