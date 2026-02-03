################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Tags API endpoint.
# Includes functions to get, add, and remove tags from people and companies.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_central_station_single_tags
#'
#' this function calls the crm api and downloads the all person_ids with one defined tag and the time it was set.

#' @param api_key the api key you have to provide
#' @param tag_to_export the setting to set a tag-string for which all person_ids should be loaded
#' @param pages the setting to set a maximum number of pages to download, with 1000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all person_ids with the defined tag, the dates when it was created and updated and the method and account who assigned it

#' @export
get_central_station_single_tags <- function (api_key, tag_to_export, pages = 1000) {
  # if no page number is set then a maximum of 1000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for the tag data
  tag_data <- tidyr::tibble()

  for (i in 1:pages) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        paste0(
          "https://api.centralstationcrm.net/api/tags?perpage=250&page=",
          i,
          "&filter%5Bname%5D[equal]=",
          tag_to_export
        ),
        httr::add_headers(headers)
      )

    # make response answer readable with jsonlite::fromJSON
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # check if data could be loaded for requested page
    if (length(data) == 0) {
      # stop export if no data could be loaded or everything is already loaded
      break
    } else {
      #get correct number of exported data
      print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " tags"))
      # check if the data already exists in "tag_data" to avoid duplicates
      if (!identical(data, tag_data[nrow(tag_data), ])) {
        # if the data is not yet available then add to tag_data
        tag_data <- dplyr::bind_rows(tag_data, data)
      }
    }

    if (i == pages) {
      # stop export after maximum number of pages was loaded
      print("Only part of the available data was exported")
      break
    }
  }

  # unnest people and filter only tag_data with customers as participants
  tag_data <- tag_data %>%
    tidyr::unnest(tag) %>%
    dplyr::rename(people_id = attachable_id)
}


#' add_crm_tag
#'
#' This function calls the CRM API and starts a post request for new tags,
#' attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - optional, id of the current tag
#'  action - optional, has to be value "add"
#'  attachable_type - "people" or "companies"
#'  field_name - name of the tag
#'  field_type - optional, has to be value "tag"
#'
#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return Tibble with created tag data (id, name, attachable_id, attachable_type, created_at, etc.) or NULL if no rows processed
#' 
#' @export
add_crm_tag <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "tag", "add")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "field_name"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_field_name(df)

  df <- df %>%
    mutate(attachable_type = transform_attachable_type(attachable_type))

  # Iterate over every row and collect responses
  all_responses <- tibble::tibble()

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
    response <- crm_POST(
      "https://api.centralstationcrm.net/api/tags",
      headers,
      body_string,
      "raw"
    )

    # Check response status
    if (httr::status_code(response) %in% c(200, 201)) {
      new_response <- jsonlite::fromJSON(httr::content(response, "text"))
      tag_tibble <- as_tibble(
        lapply(new_response$tag, function(x) if (length(x) == 0) NA else x),
        .name_repair = "unique"
      )
      all_responses <- bind_rows(all_responses, tag_tibble)
    } else {
      warning(paste0("⚠️ Failed to add tag '", df$field_name[p],
                     "' to ID ", df$attachable_id[p],
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


#' remove_crm_tag
#'
#' This function calls the CRM API and starts a delete request for tags,
#' attached to companies or persons.
#'  attachable_id - id of the person / company
#'  custom_fields_id - id of the current tag
#'  action - optional, has to be value "remove"
#'  attachable_type - "people" or "companies"
#'  field_name - name of the tag
#'  field_type - optional, has to be value "tag"

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
remove_crm_tag <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "tag", "remove")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns (after filter to ensure they exist)
  validate_required_columns(df, c("attachable_id", "attachable_type", "custom_fields_id"))

  # Validate using helper functions
  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_custom_fields_id(df)

  # iterate over every row in to_remove and generate DELETE Request
  for(r in 1:nrow(df)){
    url <- build_crm_delete_url(
      df$attachable_type[r],
      df$attachable_id[r],
      "tags",
      df$custom_fields_id[r]
    )

    # execute DELETE Request with predefined variables
    response <- crm_DELETE(url, headers, "raw")

    # Check response status
    if (!httr::status_code(response) %in% c(200, 204)) {
      warning(paste0("⚠️ Failed to remove tag ID ", df$custom_fields_id[r],
                     " from ", df$attachable_type[r], " ID ", df$attachable_id[r],
                     " - Status: ", httr::status_code(response)))
    }
  }
}
