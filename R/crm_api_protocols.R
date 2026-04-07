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
      response <- crm_GET2(
        paste0(
          "https://api.centralstationcrm.net/api/protocols?perpage=250&page=",
          i,
          "&includes=comments"
        ),
        headers
      )

      data <- jsonlite::fromJSON(suppressWarnings(httr2::resp_body_string(response)))

      # check if data could be loaded for requested page
      if (length(data) == 0) {
        break
      } else {
        print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " protocols"))
        if (!identical(data, protocols[nrow(protocols), ])) {
          protocols <- dplyr::bind_rows(protocols, data)
        }
      }

      if (i == pages) {
        print("Only part of the available data was exported")
        break
      }
    }
  } else {
    for (i in 1:length(filter_vector)) {
      tryCatch({
        response <- crm_GET2(
          paste0(
            "https://api.centralstationcrm.net/api/protocols?perpage=250&page=1",
            filter_option,
            filter_vector[i],
            "&includes=comments"
          ),
          headers
        )

        status_code <- httr2::resp_status(response)
        if (status_code != 200) {
          warning(sprintf("HTTP %d for %s=%s", status_code, filter_by, filter_vector[i]))
          next
        }

        data <-
          jsonlite::fromJSON(suppressWarnings(httr2::resp_body_string(response)))
        protocols <- dplyr::bind_rows(protocols, data)
      },
      error = function(cond) {
        warning(sprintf("Error for %s=%s: %s", filter_by, filter_vector[i], cond$message))
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
    tryCatch({
      response <- crm_GET2(
        paste0(
          "https://api.centralstationcrm.net/api/protocols/",
          protocols_vector[i],
          "/attachments?perpage=250&page=1"
        ),
        headers
      )

      status_code <- httr2::resp_status(response)
      if (status_code != 200) {
        warning(sprintf("HTTP %d for protocol_id=%s", status_code, protocols_vector[i]))
        next
      }

      data <- jsonlite::fromJSON(suppressWarnings(httr2::resp_body_string(response)))

      # API sometimes returns {"message": "..."} instead of attachment data → skip
      if (!is.data.frame(data) || "message" %in% names(data)) {
        next
      }

      data <- data %>% dplyr::select(-data)
      attachments <- dplyr::bind_rows(attachments, data)
    },
    error = function(cond) {
      warning(sprintf("Error for protocol_id=%s: %s", protocols_vector[i], cond$message))
    })
    if (i %% 10 == 0) {
      print(i)
    }
  }

  return(attachments)
}


#' Move CRM Protocol to Another Person
#'
#' Moves a protocol to a different person by updating the attachable_id via PUT.
#' This is needed as a workaround because the CRM API does not allow direct deletion
#' of protocols — they must first be moved to a "graveyard" person before deletion.
#'
#' @note KNOWN ISSUE (2026-04): The CRM API accepts the request (200/204) but
#'   the CRM UI does not reflect the change. This is a CRM vendor bug.
#'   The function is currently not usable for production workflows.
#'
#' @param headers Named vector with request headers (content-type, X-apikey, Accept)
#' @param protocol_ids Vector of CRM protocol IDs (the API-facing IDs, not internal DB IDs)
#' @param target_person_id The CRM person ID to move the protocols to
#'
#' @return Tibble with columns: protocol_id, success (logical), status_code, error_message
#' @export
move_crm_protocols <- function(headers, protocol_ids, target_person_id) {
  if (length(protocol_ids) == 0) {
    warning("No protocol IDs provided")
    return(tibble::tibble(
      protocol_id = character(),
      success = logical(),
      status_code = integer(),
      error_message = character()
    ))
  }

  results <- tibble::tibble(
    protocol_id = as.character(protocol_ids),
    success = NA,
    status_code = NA_integer_,
    error_message = NA_character_
  )

  for (i in seq_along(protocol_ids)) {
    tryCatch({
      body_string <- paste0(
        '{"protocol": {"person_ids": [', target_person_id, ']}}'
      )

      response <- crm_PUT2(
        paste0("https://api.centralstationcrm.net/api/protocols/", protocol_ids[i]),
        headers,
        body_string
      )

      status <- httr2::resp_status(response)
      results$status_code[i] <- status
      results$success[i] <- status %in% c(200, 204)

      if (!results$success[i]) {
        results$error_message[i] <- paste0("HTTP ", status)
      }
    },
    error = function(cond) {
      results$success[i] <<- FALSE
      results$error_message[i] <<- cond$message
    })

    if (i %% 50 == 0) {
      message(sprintf("Moved %d / %d protocols", i, length(protocol_ids)))
    }
  }

  succeeded <- sum(results$success, na.rm = TRUE)
  failed <- sum(!results$success, na.rm = TRUE)
  message(sprintf("Move complete: %d succeeded, %d failed", succeeded, failed))

  return(results)
}


#' Delete CRM Protocols
#'
#' Deletes protocols from the CRM via the API.
#' Important: Protocols should first be moved to a "graveyard" person using
#' move_crm_protocols() before deletion to ensure clean removal.
#'
#' @note KNOWN ISSUE (2026-04): The CRM API accepts the request (200/204) but
#'   the CRM UI does not reflect the change. This is a CRM vendor bug.
#'   The function is currently not usable for production workflows.
#'
#' @param headers Named vector with request headers (content-type, X-apikey, Accept)
#' @param protocol_ids Vector of CRM protocol IDs to delete
#'
#' @return Tibble with columns: protocol_id, success (logical), status_code, error_message
#' @export
delete_crm_protocols <- function(headers, protocol_ids) {
  if (length(protocol_ids) == 0) {
    warning("No protocol IDs provided")
    return(tibble::tibble(
      protocol_id = character(),
      success = logical(),
      status_code = integer(),
      error_message = character()
    ))
  }

  results <- tibble::tibble(
    protocol_id = as.character(protocol_ids),
    success = NA,
    status_code = NA_integer_,
    error_message = NA_character_
  )

  for (i in seq_along(protocol_ids)) {
    tryCatch({
      response <- crm_DELETE2(
        paste0("https://api.centralstationcrm.net/api/protocols/", protocol_ids[i]),
        headers
      )

      status <- httr2::resp_status(response)
      results$status_code[i] <- status
      results$success[i] <- status %in% c(200, 204)

      if (!results$success[i]) {
        results$error_message[i] <- paste0("HTTP ", status)
      }
    },
    error = function(cond) {
      results$success[i] <<- FALSE
      results$error_message[i] <<- cond$message
    })

    if (i %% 50 == 0) {
      message(sprintf("Deleted %d / %d protocols", i, length(protocol_ids)))
    }
  }

  succeeded <- sum(results$success, na.rm = TRUE)
  failed <- sum(!results$success, na.rm = TRUE)
  message(sprintf("Delete complete: %d succeeded, %d failed", succeeded, failed))

  return(results)
}
