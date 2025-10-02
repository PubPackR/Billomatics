################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Positions API endpoint.
# Includes functions to update positions (job titles) for people.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_crm_positions
#'
#' Get all positions for a person or multiple people
#' Required columns: attachable_id
#'
#' @param headers API headers with authentication
#' @param df Dataframe with column: attachable_id
#' @return Dataframe with all positions including position_id, company_id, name, department, etc.
#'
#' @export
get_crm_positions <- function(headers, df) {
  # Validate required columns
  validate_required_columns(df, c("attachable_id"))
  validate_attachable_id(df)

  # Initialize result dataframe
  all_results <- tibble::tibble()

  # Iterate over every row
  for (p in 1:nrow(df)) {
    person_id <- df$attachable_id[p]

    # Build URL
    url <- paste0("https://api.centralstationcrm.net/api/people/", person_id, "/positions")

    # Make GET request
    response <- httr::GET(url, httr::add_headers(headers))

    if (httr::status_code(response) != 200) {
      warning(paste0("⚠️ Failed to fetch positions for person ID ", person_id,
                     " - Status: ", httr::status_code(response)))
      next
    }

    # Parse response
    positions_data <- jsonlite::fromJSON(httr::content(response, "text"))

    if (!is.data.frame(positions_data) || nrow(positions_data) == 0) {
      next
    }

    # Unnest position data and add person_id
    positions_data <- positions_data %>%
      tidyr::unnest(position, names_sep = "_") %>%
      dplyr::mutate(attachable_id = person_id)

    all_results <- dplyr::bind_rows(all_results, positions_data)
  }

  all_results
}


#' update_crm_position
#'
#' This function calls the CRM API and updates position information
#' (job title, department) for a person at a company.
#'  attachable_id - id of the person (required)
#'  position_id - id of the position to update (required)
#'  action - has to be value "update"
#'  field_type - has to be value "position"
#'  position_name - new position/job title (optional)
#'  department - department name (optional)
#'  company_id - company ID (optional, kept from existing if not provided)
#'  company_name - company name (optional, kept from existing if not provided)
#'  primary_function - boolean, if this is primary position (optional, default TRUE)
#'  former - boolean, if this is a former position (optional, default FALSE)

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
update_crm_position <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "position", "update")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id", "position_id"))
  validate_attachable_id(df)
  validate_positive_id(df, "position_id", "position_id")

  # Iterate over every row
  for (p in 1:nrow(df)) {
    # Build position data list with defaults
    position_data <- list(
      position = list(
        person_id = as.numeric(df$attachable_id[p]),
        primary_function = TRUE,
        former = FALSE
      )
    )

    # Add optional fields only if they exist and are not NA/empty
    if (has_valid_value(df, "position_name", p)) {
      position_data$position$name <- get_optional_value(df, "position_name", p)
    }
    if (has_valid_value(df, "department", p)) {
      position_data$position$department <- get_optional_value(df, "department", p)
    }
    if (has_valid_value(df, "company_id", p)) {
      position_data$position$company_id <- get_optional_value(df, "company_id", p, as.numeric)
    }
    if (has_valid_value(df, "company_name", p)) {
      position_data$position$company_name <- get_optional_value(df, "company_name", p)
    }
    if (has_valid_value(df, "primary_function", p)) {
      position_data$position$primary_function <- get_optional_value(df, "primary_function", p, as.logical)
    }
    if (has_valid_value(df, "former", p)) {
      position_data$position$former <- get_optional_value(df, "former", p, as.logical)
    }

    # Check if we have any updates besides person_id, primary_function, former
    has_updates <- length(position_data$position) > 3

    if (!has_updates) {
      warning(paste0("⚠️ No position data to update for row ", p, ", skipping"))
      next
    }

    # Convert to JSON
    body_string <- jsonlite::toJSON(position_data, auto_unbox = TRUE)

    # Execute PUT request
    response <- httr::PUT(
      paste0("https://api.centralstationcrm.net/api/people/", df$attachable_id[p], "/positions/", df$position_id[p]),
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201, 204)) {
      warning(paste0("⚠️ Failed to update position ID ", df$position_id[p],
                     " for person ID ", df$attachable_id[p],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' create_crm_position
#'
#' Create new position relationship between person and company
#' Required columns: attachable_id (person_id), company_id, action, field_type
#' Optional columns: position_name, department, primary_function, former
#' action must be "create", field_type must be "position"
#'
#' @param headers API headers with authentication
#' @param df Dataframe with position information
#' @return Tibble with created position data (id, person_id, company_id, name, department, etc.) or NULL if no rows processed
#'
#' @export
create_crm_position <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "position", "create")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id", "company_id"))
  validate_attachable_id(df)
  validate_positive_id(df, "company_id", "company_id")

  # Iterate over every row and collect responses
  all_responses <- tibble::tibble()

  for (p in 1:nrow(df)) {
    # Build position data list with defaults
    position_data <- list(
      position = list(
        person_id = as.numeric(df$attachable_id[p]),
        company_id = as.numeric(df$company_id[p])
      )
    )

    # Add optional fields only if they exist and are not NA/empty
    if (has_valid_value(df, "position_name", p)) {
      position_data$position$name <- get_optional_value(df, "position_name", p)
    }
    if (has_valid_value(df, "department", p)) {
      position_data$position$department <- get_optional_value(df, "department", p)
    }
    if (has_valid_value(df, "primary_function", p)) {
      position_data$position$primary_function <- get_optional_value(df, "primary_function", p, as.logical)
    }
    if (has_valid_value(df, "former", p)) {
      position_data$position$former <- get_optional_value(df, "former", p, as.logical)
    }

    body_string <- jsonlite::toJSON(position_data, auto_unbox = TRUE)

    # Create position using POST /api/people/{id}/positions
    response <- httr::POST(
      paste0("https://api.centralstationcrm.net/api/people/", df$attachable_id[p], "/positions"),
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )

    # Check response status
    if (httr::status_code(response) %in% c(200, 201)) {
      new_response <- jsonlite::fromJSON(httr::content(response, "text"))
      position_tibble <- as_tibble(
        lapply(new_response$position, function(x) if (length(x) == 0) NA else x),
        .name_repair = "unique"
      )
      all_responses <- bind_rows(all_responses, position_tibble)
    } else {
      warning(paste0("⚠️ Failed to create position for person ID ", df$attachable_id[p],
                     " at company ID ", df$company_id[p],
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
