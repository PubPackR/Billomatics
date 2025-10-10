################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Custom Fields API endpoint.
# Includes functions to add and remove custom fields from people and companies.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_crm_custom_fields
#'
#' Get all custom fields for a person or company
#' Required columns: attachable_id, attachable_type
#'
#' @param headers API headers with authentication
#' @param df Dataframe with columns: attachable_id, attachable_type
#' @return Dataframe with all custom fields including id, custom_fields_type_id, name (value), etc.
#'
#' @export
get_crm_custom_fields <- function(headers, df) {
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
      "/custom_fields"
    )

    # Make GET request
    response <- httr::GET(url, httr::add_headers(headers))

    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch custom fields for ", attachable_type, " ID ", attachable_id,
                     " - Status: ", httr::status_code(response)))
      next
    }

    # Parse response
    custom_fields_data <- jsonlite::fromJSON(httr::content(response, "text"))

    if (!is.data.frame(custom_fields_data) || nrow(custom_fields_data) == 0) {
      next
    }

    # Unnest custom_field if needed
    if ("custom_field" %in% names(custom_fields_data)) {
      tryCatch({
        custom_fields_data <- custom_fields_data %>%
          tidyr::unnest(custom_field, names_sep = "_") %>%
          dplyr::mutate(
            attachable_id = attachable_id,
            attachable_type = attachable_type
          ) %>%
          dplyr::rename(
            custom_fields_id = custom_field_id,
            custom_fields_type_id = custom_field_custom_fields_type_id,
            value = custom_field_name
          )

        if (nrow(custom_fields_data) > 0) {
          all_results <- dplyr::bind_rows(all_results, custom_fields_data)
        }
      }, error = function(e) {
        warning(paste0("⚠️ Failed to parse custom fields for ID ", attachable_id, ": ", e$message))
      })
    } else {
      # Already in flat format
      custom_fields_data <- custom_fields_data %>%
        dplyr::mutate(
          attachable_id = attachable_id,
          attachable_type = attachable_type
        ) %>%
        dplyr::rename(
          custom_fields_id = id,
          value = name
        )

      if (nrow(custom_fields_data) > 0) {
        all_results <- dplyr::bind_rows(all_results, custom_fields_data)
      }
    }
  }

  all_results
}


#' add_crm_custom_fields
#'
#' This function calls the CRM API and starts an post request for
#' custom_fields, attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - optional, id of the current custom_field
#'  action - optional, has to be value "add"
#'  attachable_type - "people" or "companies"
#'  field_name - id of the custom_field
#'  field_type - optional, has to be value "custom_field"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return Tibble with created custom_field data (id, custom_fields_type_id, name, attachable_id, etc.) or NULL if no rows processed

#' @export
add_crm_custom_fields <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "custom_field", "add")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "field_name", "value"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_value(df)

  df <- df %>%
    mutate(attachable_type_string = transform_attachable_type(attachable_type))

  # Iterate over every row and collect responses
  all_responses <- tibble::tibble()

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
    response <- httr::POST(
      paste0("https://api.centralstationcrm.net/api/", df$attachable_type[a], "/", df$attachable_id[a],"/custom_fields"),
      httr::add_headers(headers),
      body = body_string,
      encode = "raw"
    )

    # Check response status
    if (httr::status_code(response) %in% c(200, 201)) {
      new_response <- jsonlite::fromJSON(httr::content(response, "text"))
      custom_field_tibble <- as_tibble(
        lapply(new_response$custom_field, function(x) if (length(x) == 0) NA else x),
        .name_repair = "unique"
      )
      all_responses <- bind_rows(all_responses, custom_field_tibble)
    } else {
      warning(paste0("⚠️ Failed to add custom field (type_id: ", df$field_name[a],
                     ") to ID ", df$attachable_id[a],
                     " - Status: ", httr::status_code(response)))
    }
  }

  # Combine all successful responses
  if (length(all_responses) > 0) {
    return(all_responses)
  } else {
    return(NULL)
  }
}


#' remove_crm_custom_fields
#'
#' This function calls the CRM API and starts an empty request for
#' custom_fields, attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the current custom_field
#'  action - optional, has to be value "remove"
#'  attachable_type - "people" or "companies"
#'  field_name - id of the custom_field
#'  field_type - optional, has to be value "custom_field"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
remove_crm_custom_fields <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "custom_field", "remove")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "custom_fields_id"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_custom_fields_id(df)

  for (r in 1:nrow(df)) {
    url <- build_crm_delete_url(
      df$attachable_type[r],
      df$attachable_id[r],
      "custom_fields",
      df$custom_fields_id[r]
    )

    # execute DELETE Request with predefined variables
    response <- httr::DELETE(url,
           httr::add_headers(headers),
           encode = "raw")

    # Check response status
    if (!httr::status_code(response) %in% c(200, 204)) {
      warning(paste0("⚠️ Failed to remove custom field ID ", df$custom_fields_id[r],
                     " from ", df$attachable_type[r], " ID ", df$attachable_id[r],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' update_crm_custom_fields
#'
#' This function calls the CRM API and updates existing custom_fields,
#' attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the custom_field instance to update
#'  action - has to be value "update"
#'  attachable_type - "people" or "companies"
#'  field_name - custom_fields_type_id (the field type ID)
#'  value - new value for the custom field
#'  field_type - has to be value "custom_field"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
update_crm_custom_fields <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "custom_field", "update")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "custom_fields_id", "field_name", "value"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_custom_fields_id(df)
  validate_value(df)

  for(u in 1:nrow(df)){

    # Generate body string for update
    body_string <- paste0(
        '{
          "custom_field": {
              "custom_fields_type_id": ', df$field_name[u], ',
              "name": "', df$value[u], '"
            }
          }'
        )

    # Execute PUT request with predefined header and body
    response <- httr::PUT(
      paste0("https://api.centralstationcrm.net/api/", df$attachable_type[u], "/", df$attachable_id[u],"/custom_fields/", df$custom_fields_id[u]),
      httr::add_headers(headers),
      body = body_string,
      encode = "raw"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201, 204)) {
      warning(paste0("⚠️ Failed to update custom field ID ", df$custom_fields_id[u],
                     " for ID ", df$attachable_id[u],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' manage_crm_custom_fields
#'
#' Smart CRUD function that automatically determines which operation to perform
#' (CREATE, UPDATE, DELETE) based on old_value and new_value.
#'
#' Decision logic:
#'  - old_value is NA/empty + new_value has value → CREATE (add)
#'  - old_value has value + new_value has different value → UPDATE
#'  - old_value has value + new_value is NA/empty → DELETE (remove)
#'  - old_value is NA/empty + new_value is NA/empty → SKIP
#'
#' Required columns:
#'  attachable_id - id of the person / company
#'  attachable_type - "people" or "companies"
#'  field_name - custom_fields_type_id (the field type ID)
#'  old_value - current value (NA if field doesn't exist)
#'  new_value - desired value (NA to delete field)
#'  custom_fields_id - id of the custom_field instance (NA if doesn't exist, required for UPDATE/DELETE)

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the required fields
#' @return no return values

#' @export
manage_crm_custom_fields <- function(headers, df) {
  # Validate required columns
  validate_required_columns(df, c("attachable_id", "attachable_type", "field_name", "old_value", "new_value"))
  validate_attachable_id(df)
  validate_attachable_type(df)

  # Helper function to check if value is empty
  is_empty_value <- function(val) {
    is.na(val) | trimws(as.character(val)) == ""
  }

  # Determine action for each row
  df <- df %>%
    mutate(
      old_empty = is_empty_value(old_value),
      new_empty = is_empty_value(new_value),
      action = case_when(
        old_empty & !new_empty ~ "add",           # CREATE: no old value, has new value
        !old_empty & !new_empty & old_value != new_value ~ "update",  # UPDATE: different values
        !old_empty & new_empty ~ "remove",        # DELETE: has old value, no new value
        TRUE ~ "skip"                             # SKIP: both empty or same value
      ),
      field_type = "custom_field"
    )

  # Split into action groups
  df_add <- df %>% filter(action == "add")
  df_update <- df %>% filter(action == "update")
  df_remove <- df %>% filter(action == "remove")

  # Execute CREATE operations
  if (nrow(df_add) > 0) {
    df_add_prepared <- df_add %>%
      mutate(value = new_value) %>%
      select(attachable_id, attachable_type, field_name, value, action, field_type)

    add_crm_custom_fields(headers, df_add_prepared)
  }

  # Execute UPDATE operations
  if (nrow(df_update) > 0) {
    # Validate that custom_fields_id exists for updates
    if (any(is.na(df_update$custom_fields_id))) {
      stop("❌ custom_fields_id is required for UPDATE operations but is NA in some rows")
    }

    df_update_prepared <- df_update %>%
      mutate(value = new_value) %>%
      select(attachable_id, attachable_type, field_name, value, custom_fields_id, action, field_type)

    update_crm_custom_fields(headers, df_update_prepared)
  }

  # Execute DELETE operations
  if (nrow(df_remove) > 0) {
    # Validate that custom_fields_id exists for deletes
    if (any(is.na(df_remove$custom_fields_id))) {
      stop("❌ custom_fields_id is required for DELETE operations but is NA in some rows")
    }

    df_remove_prepared <- df_remove %>%
      select(attachable_id, attachable_type, custom_fields_id, action, field_type)

    remove_crm_custom_fields(headers, df_remove_prepared)
  }

  # Report skipped rows
  skipped_count <- sum(df$action == "skip")
  if (skipped_count > 0) {
    message(paste0("ℹ️ Skipped ", skipped_count, " row(s) (no changes needed)"))
  }

  invisible(NULL)
}
