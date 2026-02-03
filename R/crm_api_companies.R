################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Companies API endpoint.
# Includes functions to get companies and prepare company data.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_central_station_companies
#'
#' this function calls the crm api and downloads the general informations about all companies.

#' @param api_key the api key you have to provide
#' @param company_id optional company_id to load a single company (default: NULL loads all companies)
#' @param positions True if the company positions (and person_ids) should also get exported
#' @param pages the setting to set a maximum number of pages to download, with 2000 pages as default (one page includes 250 entries)
#' @param includes character vector of includes to request from the API (e.g., c("custom_fields", "addrs", "tels", "emails"))
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_companies <- function (api_key, company_id = NULL, positions = TRUE, pages = 2000, includes = c("custom_fields", "addrs", "tels", "emails")) {
  # if no page number is set then a maximum of 2000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <-
    c(`content-type` = "application/json",
      `X-apikey` = api_key,
      Accept = "*/*"
  )

  # if company_id is provided, load single company
  if (!is.null(company_id)) {
    # Build includes string from parameter
    includes_string <- ""
    if (length(includes) > 0) {
      includes_string <- paste0("?includes=", paste(includes, collapse = "%20"))
    }

    # Add positions if needed
    if(positions) {
      if(includes_string == "") {
        includes_string <- "?includes=positions"
      } else {
        includes_string <- paste0(includes_string, "%20positions")
      }
    }

    url <- paste0("https://api.centralstationcrm.net/api/companies/", company_id, includes_string)
    response <- httr::GET(url, httr::add_headers(headers))
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # data$company contains the actual company data with nested data frames
    # wrap only multi-value elements in list to create list-columns where needed
    data$company <- lapply(data$company, function(x) {
      if(is.null(x)) {
        NA
      } else if(is.data.frame(x) || is.list(x) || length(x) > 1) {
        list(x)
      } else {
        x
      }
    })

    companies_crm <- tibble::as_tibble(data$company)

    if(positions && "positions" %in% names(companies_crm)) {
      companies_crm <- tidyr::unnest(companies_crm, positions, names_sep = "_", keep_empty = TRUE)
    }

    return(companies_crm)
  }

  #get number of companies in CRM and limit download to this number (calculate number of pages)
  response <- httr::GET("https://api.centralstationcrm.net/api/companies/count", httr::add_headers(headers))
  data <- jsonlite::fromJSON(httr::content(response, "text"))
  pages <- ceiling(data$total_entries/250)

  companies <- tidyr::tibble()

  if(positions){
    pos <- "%20positions"
  } else {
    pos <- ""
  }

  # Build includes string from parameter
  includes_string <- ""
  if (length(includes) > 0) {
    includes_string <- paste0("&includes=", paste(includes, collapse = "%20"))
  }

  #request every page of companies from CRM and load them to companies table
  for (i in 1:pages) {
    response <-
      httr::GET(
        paste0(
          "https://api.centralstationcrm.net/api/companies?perpage=250&page=",
          i,
          includes_string,
          pos
        ),
        httr::add_headers(headers)
      )
    data <- jsonlite::fromJSON(httr::content(response, "text"))
    if (!identical(data, companies[nrow(companies),])) {
      companies <- dplyr::bind_rows(companies, data)
    }
  }

  companies_crm <- companies %>%
    tidyr::unnest(company) %>%
    mutate(custom_fields = map(custom_fields, as.data.frame))

  if(positions){
    companies_crm <- tidyr::unnest(companies_crm, positions, names_sep = "_", keep_empty = TRUE)
  }

  return(companies_crm)

}


#' prepare_companies_crm
#'
#' @param companies_crm companies file from crm main_data_files
#' @return companies file from crm main_data_files (prepared with prepare_companies_crm())

#' @export
prepare_companies_crm <- function(companies_crm) {
  companies <- companies_crm %>%
    distinct() %>%
    select(custom_fields_custom_fields_type_name, custom_fields_name, id, positions_person_id, name) %>%
    rename(person_id = positions_person_id) %>%
    filter(custom_fields_custom_fields_type_name == "Billomat-Kundennummer") %>%
    select(-custom_fields_custom_fields_type_name) %>%
    rename(billomat_kd = custom_fields_name) %>%
    rename(crm_name = name,
           crm_company_id = id)
}


#' search_company_by_name
#'
#' Search for company by name in CRM
#'
#' @param headers API headers with authentication
#' @param company_name Name of the company to search for
#' @return List with company information if found, or NULL if not found
#'
#' @export
search_company_by_name <- function(headers, company_name) {

  tryCatch({
    # URL encode the company name for the search query
    encoded_name <- utils::URLencode(company_name, reserved = TRUE)

    # Search for company using the search endpoint
    search_url <- paste0("https://api.centralstationcrm.net/api/companies/search?perpage=50&page=1&name=", encoded_name)

    response <- httr::GET(search_url, httr::add_headers(headers))

    if (httr::status_code(response) == 200) {
      companies <- jsonlite::fromJSON(httr::content(response, "text"))

      if (is.data.frame(companies) && nrow(companies) > 0) {
        # Look for exact match (case insensitive)
        exact_matches <- companies[tolower(companies$name) == tolower(company_name), ]

        if (nrow(exact_matches) > 0) {
          cat(paste("  Found existing company:", exact_matches$name[1], "(ID:", exact_matches$id[1], ")\n"))
          return(list(
            found = TRUE,
            company = exact_matches[1, ]
          ))
        }
      }
    }

    cat(paste("  Company not found:", company_name, "\n"))
    return(list(found = FALSE, company = NULL))

  }, error = function(e) {
    warning(paste("Error searching for company:", e$message))
    return(list(found = FALSE, company = NULL))
  })
}


#' create_crm_company
#'
#' Create new company in CRM
#' Required columns: company_name, action, field_type
#' action must be "create", field_type must be "company"
#'
#' @param headers API headers with authentication
#' @param df Dataframe with company information
#' @return Tibble with created company data (id, name, created_at, etc.) or NULL if no rows processed
#'
#' @export
create_crm_company <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "company", "create")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("company_name"))
  validate_not_empty(df, "company_name")

  # Iterate over every row and collect responses
  all_responses <- tibble::tibble()

  for (p in 1:nrow(df)) {
    # Prepare company data
    company_data <- list(
      company = list(
        name = df$company_name[p]
      )
    )

    body_string <- jsonlite::toJSON(company_data, auto_unbox = TRUE)

    # Create company using POST /api/companies
    response <- crm_POST(
      "https://api.centralstationcrm.net/api/companies",
      headers,
      body_string,
      "json"
    )

    # Check response status
    if (httr::status_code(response) %in% c(200, 201)) {
      new_response <- jsonlite::fromJSON(httr::content(response, "text"))
      company_tibble <- as_tibble(
        lapply(new_response$company, function(x) if (length(x) == 0) NA else x),
        .name_repair = "unique"
      )
      all_responses <- bind_rows(all_responses, company_tibble)
    } else {
      warning(paste0("⚠️ Failed to create company '", df$company_name[p],
                     "' - Status: ", httr::status_code(response)))
    }
  }

  # Combine all successful responses
  if (length(all_responses) > 0) {
    return(all_responses)
  } else {
    return(NULL)
  }
}


#' update_crm_company
#'
#' This function calls the CRM API and updates basic company information
#' (name and potentially other fields).
#'  attachable_id - id of the company (required)
#'  action - has to be value "update"
#'  field_type - has to be value "company"
#'  name - optional (company name)
#'
#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values
#'
#' @export
update_crm_company <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "company", "update")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id"))
  validate_attachable_id(df)

  # Iterate over every row
  for (p in 1:nrow(df)) {
    # Build company data list - only with fields that have values
    company_data <- list(company = list())

    # Add optional fields only if they exist and are not NA/empty
    if (has_valid_value(df, "name", p)) {
      company_data$company$name <- df$name[p]
    }

    # Check if we have any updates
    if (length(company_data$company) == 0) {
      warning(paste0("⚠️ No company data to update for row ", p, ", skipping"))
      next
    }

    # Convert to JSON
    body_string <- jsonlite::toJSON(company_data, auto_unbox = TRUE)

    # Execute PUT request
    response <- crm_PUT(
      paste0("https://api.centralstationcrm.net/api/companies/", df$attachable_id[p]),
      headers,
      body_string,
      "json"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201, 204)) {
      warning(paste0("⚠️ Failed to update company ID ", df$attachable_id[p],
                     " - Status: ", httr::status_code(response)))
    }
  }
}
