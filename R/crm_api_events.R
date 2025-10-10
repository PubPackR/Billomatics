################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Calendar Events API endpoint.
# Includes functions to get calendar events.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' get_central_station_cal_events
#'
#' this function calls the crm api and downloads the cal_events (past and future) to all contact.

#' @param api_key the api key you have to provide
#' @param pages the setting to set a maximum number of pages to download, with 1000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all information of every member at the cal_events

#' @export
get_central_station_cal_events <- function (api_key, pages = 1000) {
  # if no page number is set then a maximum of 1000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for cal_events
  cal_events <- tidyr::tibble()

  for (i in 1:pages) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        # set url with "page=" so you can add endpoints
        paste0(
          "https://api.centralstationcrm.net/api/cal_events?perpage=250&page=",
          i,
          "&includes=people"
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
      print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " cal_events"))
      # check if the data already exists in "cal_events" to avoid duplicates
      if (!identical(data, cal_events[nrow(cal_events), ])) {
        # if the data is not yet available then add to cal_events
        cal_events <- dplyr::bind_rows(cal_events, data)
      }
    }

    if (i == pages) {
      # stop export after maximum number of pages was loaded
      print("Only part of the available data was exported")
      break
    }
  }

  # unnest people and export every participant for every cal event
  cal_events <- cal_events %>%
    tidyr::unnest(cal_event) %>%
    tidyr::unnest("people",names_sep = "_") %>%
    dplyr::select(id, attachable_id, name, location, description, created_at, updated_at, starts_at, ends_at, people_id, updated_by_user_id)
}
