################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Addresses API endpoint.
# Includes functions to get, add, and remove addresses from people and companies.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/10
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_crm_addresses
#'
#' Get all addresses for a person or company
#' Required columns: attachable_id, attachable_type
#'
#' @param headers API headers with authentication
#' @param df Dataframe with columns: attachable_id, attachable_type
#' @return Dataframe with all addresses including id, atype, street, city, zip, country, state, etc.
#'
#' @export
get_crm_addresses <- function(headers, df) {
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
      "/addrs"
    )

    # Make GET request
    response <- httr::GET(url, httr::add_headers(headers))

    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch addresses for ", attachable_type, " ID ", attachable_id,
                     " - Status: ", httr::status_code(response)))
      next
    }

    # Parse response
    address_data <- jsonlite::fromJSON(httr::content(response, "text"))

    if (!is.data.frame(address_data) || nrow(address_data) == 0) {
      next
    }

    # Unnest addr if needed
    if ("addr" %in% names(address_data)) {
      tryCatch({
        address_data <- address_data %>%
          tidyr::unnest(addr, names_sep = "_") %>%
          dplyr::mutate(
            attachable_id = attachable_id,
            attachable_type = attachable_type
          ) %>%
          dplyr::rename(
            address_id = addr_id,
            atype = addr_atype,
            street = addr_street,
            zip = addr_zip,
            city = addr_city,
            state_code = addr_state_code,
            country_code = addr_country_code
          )

        if (nrow(address_data) > 0) {
          all_results <- dplyr::bind_rows(all_results, address_data)
        }
      }, error = function(e) {
        warning(paste0("⚠️ Failed to parse addresses for ID ", attachable_id, ": ", e$message))
      })
    } else {
      # Already in flat format
      address_data <- address_data %>%
        dplyr::mutate(
          attachable_id = attachable_id,
          attachable_type = attachable_type
        ) %>%
        dplyr::rename(address_id = id)

      if (nrow(address_data) > 0) {
        all_results <- dplyr::bind_rows(all_results, address_data)
      }
    }
  }

  all_results
}


#' add_crm_addresses
#'
#' Add new addresses to people or companies
#' Required columns: attachable_id, attachable_type, atype, street, city, zip, country, action, field_type
#' Optional columns: additional, state
#' action must be "add", field_type must be "address"
#'
#' @param headers API headers with authentication
#' @param df Dataframe with address information
#' @return Tibble with created address data (id, atype, street, city, zip, country, etc.) or NULL if no rows processed
#'
#' @export
add_crm_addresses <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "address", "add")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id", "attachable_type", "atype", "street", "city", "zip", "country"))
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_atype(df)

  # Validate address fields are not empty
  validate_not_empty(df, "street")
  validate_not_empty(df, "city")
  validate_not_empty(df, "zip")
  validate_not_empty(df, "country")

  df <- df %>%
    mutate(attachable_type_string = transform_attachable_type(attachable_type))

  # Iterate over every row and collect responses
  all_responses <- tibble::tibble()

  for (p in 1:nrow(df)) {
    # Build address data
    addr_data <- list(
      addr = list(
        attachable_type = df$attachable_type_string[p],
        attachable_id = as.numeric(df$attachable_id[p]),
        atype = df$atype[p],
        street = df$street[p],
        city = df$city[p],
        zip = df$zip[p],
        country = df$country[p],
        api_input = TRUE
      )
    )

    # Add optional fields
    if (has_valid_value(df, "additional", p)) {
      addr_data$addr$additional <- df$additional[p]
    }
    if (has_valid_value(df, "state", p)) {
      addr_data$addr$state <- df$state[p]
    }

    body_string <- jsonlite::toJSON(addr_data, auto_unbox = TRUE)

    # Execute POST request
    response <- crm_POST(
      paste0("https://api.centralstationcrm.net/api/", df$attachable_type[p], "/", df$attachable_id[p], "/addrs"),
      headers,
      body_string,
      "json"
    )

    # Check response status
    if (httr::status_code(response) %in% c(200, 201)) {
      new_response <- jsonlite::fromJSON(httr::content(response, "text"))
      addr_tibble <- as_tibble(
        lapply(new_response$addr, function(x) if (length(x) == 0) NA else x),
        .name_repair = "unique"
      )
      all_responses <- bind_rows(all_responses, addr_tibble)
    } else {
      warning(paste0("⚠️ Failed to add address (", df$street[p], ", ", df$city[p],
                     ") to ID ", df$attachable_id[p],
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


#' remove_crm_addresses
#'
#' Remove addresses from people or companies
#' Required columns: attachable_id, attachable_type, address_id, action, field_type
#' action must be "remove", field_type must be "address"
#'
#' @param headers API headers with authentication
#' @param df Dataframe with address information
#' @return no return values
#'
#' @export
remove_crm_addresses <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "address", "remove")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id", "attachable_type", "address_id"))
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_positive_id(df, "address_id", "address_id")

  # Iterate over every row
  for (r in 1:nrow(df)) {
    # Build URL
    url <- paste0(
      "https://api.centralstationcrm.net/api/",
      df$attachable_type[r], "/",
      df$attachable_id[r], "/addrs/",
      df$address_id[r]
    )

    # Execute DELETE request
    response <- crm_DELETE(url, headers, "raw")

    # Check response status
    if (!httr::status_code(response) %in% c(200, 204)) {
      warning(paste0("⚠️ Failed to remove address ID ", df$address_id[r],
                     " from ", df$attachable_type[r], " ID ", df$attachable_id[r],
                     " - Status: ", httr::status_code(response)))
    }
  }
}
