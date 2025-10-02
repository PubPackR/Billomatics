################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM People API endpoint.
# Includes functions to get contacts and determine responsible persons.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_central_station_contacts
#'
#' this function calls the crm api and downloads all fields that belong to a contact.
#' The notes and protocols are NOT downloaded.

#' @param api_key the api key you have to provide
#' @param person_id optional person_id to load a single contact (default: NULL loads all contacts)
#' @param pages the setting to call all pages (default), or a specific number of pages (one page includes 250 entries)
#' @param includes optional includes parameter (default: "all"). Examples: "all", "tags custom_fields", "tags"
#' @param methods optional methods parameter (default: "all")
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_contacts <- function (api_key, pages = "all", person_id = NULL, includes = "all", methods = "all") {
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # if person_id is provided, load single contact
  if (!is.null(person_id)) {
    url <- paste0("https://api.centralstationcrm.net/api/people/", person_id)
    response <- httr::GET(url, httr::add_headers(headers), query = list(includes = includes, methods = methods))
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # data$person contains the actual person data with nested data frames
    # wrap only multi-value elements in list to create list-columns where needed
    data$person <- lapply(data$person, function(x) {
      if(is.null(x)) {
        NA
      } else if(is.data.frame(x) || is.list(x) || length(x) > 1) {
        list(x)
      } else {
        x
      }
    })
    return(tibble::as_tibble(data$person))
  }

  # if the pages are set to "all" then all pages will be downloaded
  # if pages is set to a specific number, then this number of pages will be downloaded

  #define url to count all people
  url <- "https://api.centralstationcrm.net/api/people/count"

  # create access with httr GET-function
  response <- httr::GET(url, httr::add_headers(headers))

  # make response answer readable with jsonlite::fromJSON
  data <- jsonlite::fromJSON(httr::content(response, "text"))

  # save the answer in the variable number_of_persons
  number_of_persons <- data$total_entries
  # if the pages is set to "all" then the ceiling of all persons / 50 is the max number of pages
  if (pages == "all") {
    pages <- ceiling(number_of_persons / 250)
  } else {
    # if the pages is set to a specific number
    pages <- pages
  }

  #number_of_persons <- 200
  ################################################################################

  persons <- tidyr::tibble() # create an data.frame for persons

  # set url with "page=" so you can add endpoints
  url <-
    "https://api.centralstationcrm.net/api/people?perpage=250&page="

  for (i in 1:pages) {
    print(paste0("Page:", i, " of ", ceiling(number_of_persons / 250)))
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        paste0(url, i),
        httr::add_headers(headers),
        query = list(includes = includes, methods = methods)
      )

    # make response answer readable with jsonlite::fromJSON
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # check if the data already exists in "persons" to avoid duplicates
    if (!identical(data, persons[nrow(persons),])) {
      # if the data is not yet available then add to persons
      persons <- dplyr::bind_rows(persons, data)
    }
  }
  persons %>% tidyr::unnest(person)
}

#' get_responsible_person
#'
#' This function extracts the responsible person for each entity based on the applied tags.
#' It prioritizes tags that start with 'W', followed by 'U', and finally 'O'.

#' @param persons Dataframe containing persons with associated tags.
#' @param employees Dataframe containing Vertriebler and Tierpool.
#' @param o_tag_responsible Dataframe containing O-Tags and zuständig_group (Zuständiger fuer O-Tag)
#' @return persons_to_include A dataframe containing `id` and `responsible`, which indicates the
#' responsible person or group for each `id`, determined by the tag priority ('W', 'U', 'O').

#' @export
get_responsible_person <- function(persons, employees, o_tag_responsible) {

  persons_to_include <- persons %>%
    mutate(id = as.integer(id)) %>%
    mutate(created_at = as_datetime(created_at, tz = "Europe/Berlin")) %>%
    unnest(tags, names_sep = "_") %>%
    select(id, tags_name) %>%
    filter(grepl("^W ", tags_name) & !grepl("Termin", tags_name) | grepl("^U ", tags_name) | grepl("^O ", tags_name)) %>%
    mutate(w_tag = ifelse(grepl("^W", tags_name), sub("W ", "", tags_name), NA)) %>%
    mutate(u_tag = ifelse(grepl("^U", tags_name), tags_name, NA)) %>%
    mutate(o_tag = ifelse(grepl("^O", tags_name), tags_name, NA)) %>%

    left_join(employees, by = c("u_tag" = "Tierpool")) %>%
    filter(!(!is.na(u_tag) & is.na(Vertriebler))) %>%
    mutate(u_tag = Vertriebler) %>%
    select(-Vertriebler) %>%

    left_join(o_tag_responsible, by = c("o_tag" = "O-Tags")) %>%
    filter(!(!is.na(o_tag) & is.na(zuständig_group))) %>%
    mutate(o_tag = zuständig_group) %>%
    select(-zuständig_group) %>%

    mutate(prio = ifelse(is.na(w_tag), ifelse(is.na(u_tag), 3, 2), 1)) %>%
    group_by(id) %>%
    slice_min(prio, with_ties = TRUE) %>%
    ungroup() %>%

    mutate(responsible = ifelse(is.na(w_tag), ifelse(is.na(u_tag), o_tag, u_tag), w_tag)) %>%
    distinct(id, responsible)

}


#' update_crm_person
#'
#' This function calls the CRM API and updates basic person information
#' (salutation, first_name, last_name, background).
#'  attachable_id - id of the person (required)
#'  action - has to be value "update"
#'  field_type - has to be value "person"
#'  salutation - optional
#'  first_name - optional
#'  last_name - optional (API field: 'name')
#'  background - optional (description/notes)

#' @param headers the header informations you have to send with your request
#' @param df the dataframe which should include the following fields:
#' @return no return values

#' @export
update_crm_person <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "person", "update")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("attachable_id"))
  validate_attachable_id(df)

  # Iterate over every row
  for (p in 1:nrow(df)) {
    # Build person data list - only with fields that have values
    person_data <- list(person = list())

    # Add optional fields only if they exist and are not NA/empty
    if (has_valid_value(df, "salutation", p)) {
      person_data$person$salutation <- df$salutation[p]
    }
    if (has_valid_value(df, "first_name", p)) {
      person_data$person$first_name <- df$first_name[p]
    }
    if (has_valid_value(df, "last_name", p)) {
      person_data$person$name <- df$last_name[p]  # API uses 'name' for last name
    }
    if (has_valid_value(df, "background", p)) {
      person_data$person$background <- df$background[p]
    }

    # Check if we have any updates
    if (length(person_data$person) == 0) {
      warning(paste0("⚠️ No person data to update for row ", p, ", skipping"))
      next
    }

    # Convert to JSON
    body_string <- jsonlite::toJSON(person_data, auto_unbox = TRUE)

    # Execute PUT request
    response <- httr::PUT(
      paste0("https://api.centralstationcrm.net/api/people/", df$attachable_id[p]),
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201, 204)) {
      warning(paste0("⚠️ Failed to update person ID ", df$attachable_id[p],
                     " - Status: ", httr::status_code(response)))
    }
  }
}


#' create_person
#'
#' Create new person in CRM
#' Required columns: action, field_type
#' Optional columns: salutation, first_name, last_name, background
#' action must be "create", field_type must be "person"
#'
#' @param headers API headers with authentication
#' @param df Dataframe with person information
#' @return no return values
#'
#' @export
create_crm_person <- function(headers, df) {
  # Filter by field_type and action
  df <- filter_by_field_and_action(df, "person", "create")

  if (is.null(df)) {
    return(invisible(NULL))
  }

  # Iterate over every row
  for (p in 1:nrow(df)) {
    # Build person data list
    person_data <- list(
      person = list()
    )

    # Add optional fields only if they exist and are not NA/empty
    if (has_valid_value(df, "salutation", p)) {
      person_data$person$salutation <- get_optional_value(df, "salutation", p)
    }
    if (has_valid_value(df, "first_name", p)) {
      person_data$person$first_name <- get_optional_value(df, "first_name", p)
    }
    if (has_valid_value(df, "last_name", p)) {
      person_data$person$name <- get_optional_value(df, "last_name", p)  # API uses 'name' for last name
    }
    if (has_valid_value(df, "background", p)) {
      person_data$person$background <- get_optional_value(df, "background", p)
    }

    body_string <- jsonlite::toJSON(person_data, auto_unbox = TRUE)

    # Create person using POST /api/people
    response <- httr::POST(
      "https://api.centralstationcrm.net/api/people",
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )

    # Check response status
    if (!httr::status_code(response) %in% c(200, 201)) {
      warning(paste0("⚠️ Failed to create person in row ", p,
                     " - Status: ", httr::status_code(response)))
    }
  }
}
