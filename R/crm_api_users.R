################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Users API endpoint.
# Includes functions to get CRM users (active and inactive).
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/10
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_crm_users
#'
#' Get all CRM users including active/inactive status
#'
#' @param api_key CRM API key for authentication
#' @param active_only Logical, if TRUE only return active users (default: FALSE for all users)
#' @return Dataframe with user information including user_id, user_first_name, user_name, user_login, is_active, is_deleted
#'
#' @export
get_crm_users <- function(api_key, active_only = FALSE) {
  # Define headers
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # If only active users requested, simpler logic (only 1 API call needed)
  if (active_only) {
    # Create an empty data frame to store all active users
    all_data <- tibble::tibble()

    # Start with page 1
    page <- 1

    # Loop to download all pages
    repeat {
      # Construct the URL for the current page
      url <- paste0("https://api.centralstationcrm.net/api/users?perpage=250&page=", page, "&active=active")

      # Send GET request
      response <- httr::GET(url, httr::add_headers(headers))

      # Check response status
      if (httr::status_code(response) != 200) {
        warning(paste0("⚠️ Failed to fetch users - Status: ", httr::status_code(response)))
        break
      }

      # Parse the response
      data <- jsonlite::fromJSON(httr::content(response, "text"))

      # Check if data is valid
      if (!is.data.frame(data) || nrow(data) == 0) {
        break
      }

      # Unnest user data
      if ("user" %in% names(data)) {
        data <- data %>%
          tidyr::unnest(user)
      }

      # Append to the all_data data frame
      all_data <- dplyr::bind_rows(all_data, data)

      # Check if the number of entries is less than 250 (indicating the last page)
      if (nrow(data) < 250) {
        break
      }

      # Move to the next page
      page <- page + 1
    }

    # Rename columns and add status flags
    all_data <- all_data %>%
      dplyr::rename(
        user_first_name = first,
        user_name = name,
        user_login = login,
        crm_user_id = id
      ) %>%
      dplyr::mutate(
        is_active = TRUE,
        is_deleted = FALSE
      )

    return(all_data)
  }

  # Get ALL users (active and inactive)
  # Create an empty data frame to store all the data
  all_data <- tibble::tibble()

  # Start with page 1
  page <- 1

  # Loop to download all pages
  repeat {
    # Construct the URL for the current page
    url <- paste0("https://api.centralstationcrm.net/api/users?perpage=250&page=", page)

    # Send GET request
    response <- httr::GET(url, httr::add_headers(headers))

    # Check response status
    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch users - Status: ", httr::status_code(response)))
      break
    }

    # Parse the response
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # Check if data is valid
    if (!is.data.frame(data) || nrow(data) == 0) {
      break
    }

    # Unnest user data
    if ("user" %in% names(data)) {
      data <- data %>%
        tidyr::unnest(user)
    }

    # Append to the all_data data frame
    all_data <- dplyr::bind_rows(all_data, data)

    # Check if the number of entries is less than 250 (indicating the last page)
    if (nrow(data) < 250) {
      break
    }

    # Move to the next page
    page <- page + 1
  }

  # Rename columns
  all_data <- all_data %>%
    dplyr::rename(
      user_first_name = first,
      user_name = name,
      user_login = login,
      crm_user_id = id
    ) %>%
    dplyr::mutate(is_deleted = FALSE)

  # Get active users to determine is_active status
  # Create an empty data frame to store all active users
  active_users <- tibble::tibble()

  # Start with page 1
  page <- 1

  # Loop to download all pages
  repeat {
    # Construct the URL for the current page
    url <- paste0("https://api.centralstationcrm.net/api/users?perpage=250&page=", page, "&active=active")

    # Send GET request
    response <- httr::GET(url, httr::add_headers(headers))

    # Check response status
    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch active users - Status: ", httr::status_code(response)))
      break
    }

    # Parse the response
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # Check if data is valid
    if (!is.data.frame(data) || nrow(data) == 0) {
      break
    }

    # Unnest user data
    if ("user" %in% names(data)) {
      data <- data %>%
        tidyr::unnest(user)
    }

    # Append to the active_users data frame
    active_users <- dplyr::bind_rows(active_users, data)

    # Check if the number of entries is less than 250 (indicating the last page)
    if (nrow(data) < 250) {
      break
    }

    # Move to the next page
    page <- page + 1
  }

  # Mark active status based on active_users list
  all_data <- all_data %>%
    dplyr::mutate(is_active = ifelse(crm_user_id %in% active_users$id, TRUE, FALSE))

  return(all_data)
}
