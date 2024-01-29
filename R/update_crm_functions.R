# -------------------------- Start script --------------------------------

library(dplyr)
library(tidyr)
library(tidyverse)

#' remove_crm_tag
#'
#' This function calls the CRM API and starts a delete request for tags,
#' attached to companies or persons.

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the current tag
#'  action - optional, has to be value "remove"
#'  attachable_type - "people" or "companies"
#'  field_name - name of the tag
#'  field_type - optional, has to be value "tag"
#' @return no return values

#' @export
remove_crm_tag <- function(headers, df) {
  #' the df can include add and remove requests for tags and custom fields
  #' through the optional fields "action" and "field_type" the df gets filtered
  #' before the request excecution

  df <- df %>%
    filter({if("field_type" %in% names(.)) field_type else NULL} == "tag") %>%
    filter({if("action" %in% names(.)) action else NULL} == "remove")

  # iterate over every row in to_remove and generate DELETE Request
  for(r in 1:nrow(df)){
    url <-
      paste0(
        "https://api.centralstationcrm.net/api/",
        df$attachable_type[r],
        "/",
        df$attachable_id[r],
        "/tags/",
        df$custom_fields_id[r]
      )

    # execute DELETE Request with predefined variables
    httr::DELETE(
      url,
      httr::add_headers(headers),
      encode = "raw"
    )
  }
}


#' add_crm_tag
#'
#' This function calls the CRM API and starts a post request for new tags,
#' attached to companies or persons.

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#'  attachable_id - id of the person / company
#'  custom_fields_id - optional, id of the current tag
#'  action - optional, has to be value "add"
#'  attachable_type - "people" or "companies"
#'  field_name - name of the tag
#'  field_type - optional, has to be value "tag"
#' @return no return values

#' @export
add_crm_tag <- function(headers, df) {
  #' the df can include add and remove requests for tags and custom fields
  #' through the optional fields "action" and "field_type" the df gets filtered
  #' before the request excecution

  df <- df %>%
    filter({if("field_type" %in% names(.)) field_type else NULL} == "tag") %>%
    filter({if("action" %in% names(.)) action else NULL} == "add") %>%
    mutate(attachable_type = ifelse(
      attachable_type == "companies",
      "Company",
      ifelse(attachable_type == "people", "Person", attachable_type)
    ))

  #iterate over every row in imported data frame
    for(p in 1:nrow(df)){

      #generate body string with person_id and pool_name
      body_string <- paste0(
          '{
            "tag": {
              "attachable_id": ', df$attachable_id[p], ',
              "attachable_type": "', df$attachable_type[p],'",
              "name": "', df$field_name[p], '",
              "api_input": true
              }
            }'
          )

      # execute post request with predefined header and body
      httr::POST(
        "https://api.centralstationcrm.net/api/tags",
        httr::add_headers(headers),
        body = body_string,
        encode = "raw"
      )
    }
}


#' remove_crm_custom_fields
#'
#' This function calls the CRM API and starts an empty request for
#' custom_fields, attached to companies or persons.

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the current custom_field
#'  action - optional, has to be value "remove"
#'  attachable_type - "people" or "companies"
#'  field_name - id of the custom_field
#'  field_type - optional, has to be value "custom_field"
#' @return no return values

#' @export
remove_crm_custom_fields <- function(headers, df) {
  #' the df can include add and remove requests for tags and custom fields
  #' through the optional fields "action" and "field_type" the df gets filtered
  #' before the request excecution

  df <- df %>%
    filter({if("field_type" %in% names(.)) field_type else NULL} == "custom_field") %>%
    filter({if("action" %in% names(.)) action else NULL} == "remove")

  for (r in 1:nrow(df)) {

    url <-
      paste0(
        "https://api.centralstationcrm.net/api/",
        df$attachable_type[r],
        "/",
        df$attachable_id[r],
        "/custom_fields/",
        df$custom_fields_id[r]
      )

    # execute DELETE Request with predefined variables
    httr::DELETE(url,
           httr::add_headers(headers),
           encode = "raw")
  }
}


#' add_crm_custom_fields
#'
#' This function calls the CRM API and starts an post request for
#' custom_fields, attached to companies or persons.

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#'  attachable_id - id of the person / company
#'  custom_fields_id - optional, id of the current custom_field
#'  action - optional, has to be value "add"
#'  attachable_type - "people" or "companies"
#'  field_name - id of the custom_field
#'  field_type - optional, has to be value "custom_field"
#' @return no return values

#' @export
add_crm_custom_fields <- function(headers, df) {
  #' the df can include add and remove requests for tags and custom fields
  #' through the optional fields "action" and "field_type" the df gets filtered
  #' before the request excecution

  df <- df %>%
    filter({if("field_type" %in% names(.)) field_type else NULL} == "custom_field") %>%
    filter({if("action" %in% names(.)) action else NULL} == "add") %>%
    mutate(attachable_type_string = ifelse(
      attachable_type == "companies", "Company",
      ifelse(attachable_type == "people", "Person", attachable_type)
  ))

  for(a in 1:nrow(df)){

    #generate body string with person_id and pool_name
    body_string <- paste0(
        '{
          "custom_field": {
              "attachable_type": "', df$attachable_type_string[a],'",
              "attachable_id": ', df$attachable_id[a], ',
              "custom_fields_type_id": ', df$field_name[a], ',
              "name": "', df$value[a], '",
              "api_input": true
            }
          }'
        )

    # execute post request with predefined header and body
    httr::POST(
      paste0("https://api.centralstationcrm.net/api/", df$attachable_type[r], "/", df$attachable_id[a],"/custom_fields"),
      httr::add_headers(headers),
      body = body_string,
      encode = "raw"
    )
  }
}



