################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Contact Details API endpoint.
# Includes functions to add and remove contact details (emails, phone numbers, etc.)
# from people and companies.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' add_contact_details
#'
#' This function calls the CRM API and starts a post request for new contact details,
#' attached to companies or persons.
#'  attachable_id - id of the person / company
#'  action - has to be value "add"
#'  attachable_type - "people" or "companies"
#'  field_name - the contact detail value (e.g., email address or phone number)
#'  atype - type of contact detail (e.g., "office", "work", "mobile")
#'  contact_detail_type - type of resource ("email", "tel", "homepage", "sm" etc.)
#'  field_type - has to be value "contact_details"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
add_contact_details <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "contact_details", "add")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "field_name", "atype", "contact_detail_type"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_field_name(df)
  validate_atype(df)

  df <- df %>%
    mutate(attachable_type_string = transform_attachable_type(attachable_type))

  # iterate over every row in imported data frame
  for (p in 1:nrow(df)) {
    # generate body string with contact detail data
    body_string <- paste0(
      '{
        "', df$contact_detail_type[p], '": {
          "attachable_type": "', df$attachable_type_string[p], '",
          "attachable_id": "', df$attachable_id[p], '",
          "atype": "', df$atype[p], '",
          "name": "', df$field_name[p], '",
          "api_input": true
          }
        }'
    )

    # execute post request with predefined header and body
    response <- crm_POST(
      paste0("https://api.centralstationcrm.net/api/", df$attachable_type[p], "/", df$attachable_id[p], "/contact_details"),
      headers,
      body_string,
      "raw"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201)) {
      warning(paste0("⚠️ Failed to add contact detail (", df$contact_detail_type[p], ": ", df$field_name[p],
                     ") to ID ", df$attachable_id[p],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' remove_contact_details
#'
#' This function calls the CRM API and starts a delete request for contact details,
#' attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the current contact detail
#'  action - optional, has to be value "remove"
#'  attachable_type - "people" or "companies"
#'  field_type - optional, has to be value "contact_details"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
remove_contact_details <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "contact_details", "remove")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "custom_fields_id"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_custom_fields_id(df)

  # iterate over every row and generate DELETE Request
  for (r in 1:nrow(df)) {
    url <- build_crm_delete_url(
      df$attachable_type[r],
      df$attachable_id[r],
      "contact_details",
      df$custom_fields_id[r]
    )

    # execute DELETE Request with predefined variables
    response <- crm_DELETE(url, headers, "raw")

    # Check response status
    if (!httr::status_code(response) %in% c(200, 204)) {
      warning(paste0("⚠️ Failed to remove contact detail ID ", df$custom_fields_id[r],
                     " from ", df$attachable_type[r], " ID ", df$attachable_id[r],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' get_crm_contact_details
#'
#' Get all contact details for a person or company
#' Returns emails, phone numbers, homepages, and social media
#' Required columns: attachable_id, attachable_type
#'
#' @param headers API headers with authentication
#' @param df Dataframe with columns: attachable_id, attachable_type
#' @return Dataframe with all contact details including id, atype, name, contact_type, and original attachable_id
#'
#' @export
get_crm_contact_details <- function(headers, df) {

  # Validate required columns
  validate_required_columns(df, c("attachable_id", "attachable_type"))
  validate_attachable_id(df)
  validate_attachable_type(df)

  # Initialize result dataframe
  all_results <- tibble::tibble()

  # Iterate over every row
  for (p in 1:nrow(df)) {
    attachable_id <- df$attachable_id[p]
    attachable_type <- df$attachable_type[p]

    # Build URL
    url <- paste0(
      "https://api.centralstationcrm.net/api/",
      attachable_type, "/",
      attachable_id,
      "/contact_details?order=id-asc&perpage=250&page=1"
    )

    # Make GET request
    response <- httr::GET(url, httr::add_headers(headers))

    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch contact details for ", attachable_type, " ID ", attachable_id,
                     " - Status: ", httr::status_code(response)))
      next
    }

    # Parse response
    contact_data <- jsonlite::fromJSON(httr::content(response, "text"))

    if (!is.data.frame(contact_data) || nrow(contact_data) == 0) {
      next
    }

    # Extract each contact type
    contact_types <- c("email", "tel", "homepage", "sm")

    for (type in contact_types) {
      type_cols <- grep(paste0("^", type), names(contact_data), value = TRUE)

      if (length(type_cols) > 0) {
        tryCatch({
          # Select only columns for this type, unnest, and add type label
          type_data <- contact_data %>%
            dplyr::select(dplyr::all_of(type_cols)) %>%
            tidyr::unnest(cols = dplyr::everything(), names_repair = "minimal") %>%
            dplyr::filter(!is.na(id)) %>%
            dplyr::mutate(
              contact_type = type,
              attachable_id = attachable_id,
              attachable_type = attachable_type
            )

          if (nrow(type_data) > 0) {
            all_results <- dplyr::bind_rows(all_results, type_data)
          }
        }, error = function(e) {
          warning(paste0("⚠️ Failed to parse ", type, " contact details for ID ", attachable_id, ": ", e$message))
        })
      }
    }
  }

  all_results
}
