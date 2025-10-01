################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Protocols and Attachments API endpoints.
# Includes functions to get protocols and their attachments.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_central_station_protocols
#'
#' this function calls the crm api and downloads the protocols (notices and mails) to all contact.

#' @param api_key the api key you have to provide
#' @param filter_by setting to prefilter export by a list of "company_id" or "person_id", if all protocols are needed use FALSE
#' @param filter_vector vector to filter by "company_id" or "person_id" (decided by filter_by), not needed if all protocols should get downloaded
#' @param pages the setting to set a maximum number of pages to download, with 2000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_protocols <- function (api_key, filter_by = FALSE, filter_vector = NA, pages = 2000) {
  # if no page number is set then a maximum of 2000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # define filter for request
  filter_option <- paste0("&", filter_by, "=")

  # create an data.frame for protocols
  protocols <- tidyr::tibble()

  if(filter_by == FALSE){
    for (i in 1:pages) {
      # create an response with httr and the GET function. In the function you paste
      # url and your endpoint and you also need the headers
      response <-
        httr::GET(
          # set url with "page=" so you can add endpoints
          paste0(
            "https://api.centralstationcrm.net/api/protocols?perpage=250&page=",
            i,
            "&includes=comments"
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
        print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " protocols"))
        # check if the data already exists in "protocols" to avoid duplicates
        if (!identical(data, protocols[nrow(protocols), ])) {
          # if the data is not yet available then add to protocols
          protocols <- dplyr::bind_rows(protocols, data)
        }
      }

      if (i == pages) {
        # stop export after maximum number of pages was loaded
        print("Only part of the available data was exported")
        break
      }
    }
  } else {
    for (i in 1:length(filter_vector)) {
      # create an response with httr and the GET function. In the function you paste
      # url and your endpoint and you also need the headers
      tryCatch({
        response <-
          httr::GET(
            # set url with "page=" so you can add endpoints and defined filter
            paste0(
              "https://api.centralstationcrm.net/api/protocols?perpage=250&page=1",
              filter_option,
              filter_vector[i],
              "&includes=comments"
            ),
            httr::add_headers(headers)
          )

        data <-
          jsonlite::fromJSON(httr::content(response, "text"))
        protocols <- dplyr::bind_rows(protocols, data)
      },
      error = function(cond) {
        message("Person does not exist")
        # Choose a return value in case of error
        NA
      })
      if (i %% 100 == 0) {
        print(i)
      }
    }
  }

 return(protocols)
}

#' get_central_station_attachments
#'
#' this function calls the crm api and downloads the data of attachments for a
#' given vector of protocol-ids

#' @param api_key the api key you have to provide
#' @param protocols_vector vector with protocols, where we want to export the attachments from
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_attachments <- function (api_key, protocols_vector) {

  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for attachments
  attachments <- tidyr::tibble()

  for (i in 1:length(protocols_vector)) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    tryCatch({
      response <-
        httr::GET(
          # set url with "page=" so you can add endpoints and defined filter
          paste0(
            "https://api.centralstationcrm.net/api/protocols/",
            protocols_vector[i],
            "/attachments?perpage=250&page=1"
          ),
          httr::add_headers(headers)
        )

      data <- jsonlite::fromJSON(httr::content(response, "text")) %>%
        select(-data)
      attachments <- dplyr::bind_rows(attachments, data)
    },
    error = function(cond) {
      message("Attachments do not exist")
      # Choose a return value in case of error
      NA
    })
    if (i %% 10 == 0) {
      print(i)
    }
  }

  return(attachments)
}
