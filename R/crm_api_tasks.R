################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Tasks API endpoint.
# Includes functions to create tasks and to mark tasks as finished.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/01
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' Mark CRM Tasks as Finished
#'
#' This function calls the CRM API and marks tasks as finished using PUT requests.
#' It sets the "finished" field to TRUE for each task.
#'
#' @param headers Named vector with API headers. Must include:
#'   - "content-type" = "application/json"
#'   - "X-apikey" = your_api_key
#'   - "Accept" = "*/*"
#' @param df Data frame containing task information. Required columns:
#'   - task_id: External CRM task ID (positive integer)
#'
#' @return Tibble with results for each task:
#'   - task_id: The task ID
#'   - success: Boolean indicating if the task was marked as finished
#'   - status_code: HTTP status code from the API
#'   - error_message: Error message if failed, NA otherwise
#'
#' @details
#' The function iterates over each row in the dataframe and sends a PUT request

#' to the CRM API endpoint: PUT /api/tasks/{task_id}
#' with body: {"task": {"finished": true}}
#'
#' Successful status codes: 200, 204
#'
#' @examples
#' \dontrun{
#' headers <- c(
#'   "content-type" = "application/json",
#'   "X-apikey" = keys$crm,
#'   "Accept" = "*/*"
#' )
#'
#' tasks_to_finish <- data.frame(
#'   task_id = c(18782531, 18782532, 18782533)
#' )
#'
#' results <- mark_crm_task_finished(headers, tasks_to_finish)
#' }
#'
#' @export
mark_crm_task_finished <- function(headers, df) {

  # Validate headers
  if (!is.character(headers) || length(headers) == 0) {
    stop("headers must be a named character vector with API credentials")
  }

  required_headers <- c("X-apikey", "content-type")
  missing_headers <- setdiff(required_headers, names(headers))
  if (length(missing_headers) > 0) {
    stop(paste0("Missing required headers: ", paste(missing_headers, collapse = ", ")))
  }

  # Validate dataframe
  if (!is.data.frame(df) || nrow(df) == 0) {
    warning("No tasks to process (empty dataframe)")
    return(invisible(NULL))
  }

  # Validate required columns
  validate_required_columns(df, c("task_id"))

  # Validate task_id values

  validate_positive_id(df, "task_id", "task_id")

  # Remove duplicates
  df <- df %>%
    dplyr::distinct(task_id, .keep_all = TRUE)

  message("Marking ", nrow(df), " tasks as finished...")

  # Prepare body (same for all requests)
  body_string <- '{"task": {"finished": true}}'

  # Initialize results tibble
  results <- tibble::tibble(
    task_id = integer(),
    success = logical(),
    status_code = integer(),
    error_message = character()
  )

  # Iterate over each task

  for (i in seq_len(nrow(df))) {
    task_id <- df$task_id[i]

    # Build URL
    url <- paste0("https://api.centralstationcrm.net/api/tasks/", task_id)

    # Execute PUT request
    tryCatch({
      response <- crm_PUT(url, headers, body_string, "raw")

      status_code <- httr::status_code(response)

      if (status_code %in% c(200, 204)) {
        # Success
        results <- dplyr::bind_rows(results, tibble::tibble(
          task_id = task_id,
          success = TRUE,
          status_code = status_code,
          error_message = NA_character_
        ))
      } else {
        # API error
        error_content <- httr::content(response, "text", encoding = "UTF-8")
        warning(paste0("Failed to mark task ", task_id, " as finished. ",
                       "Status: ", status_code, ", Response: ", error_content))

        results <- dplyr::bind_rows(results, tibble::tibble(
          task_id = task_id,
          success = FALSE,
          status_code = status_code,
          error_message = error_content
        ))
      }

    }, error = function(e) {
      # Network or other error
      warning(paste0("Error marking task ", task_id, " as finished: ", e$message))

      results <<- dplyr::bind_rows(results, tibble::tibble(
        task_id = task_id,
        success = FALSE,
        status_code = NA_integer_,
        error_message = e$message
      ))
    })
  }

  # Summary message
  success_count <- sum(results$success, na.rm = TRUE)
  fail_count <- nrow(results) - success_count

  if (success_count > 0) {
    message("Successfully marked ", success_count, " tasks as finished")
  }
  if (fail_count > 0) {
    warning(paste0("Failed to mark ", fail_count, " tasks as finished"))
  }

  return(results)
}

#' Create CRM Tasks
#'
#' Creates one task per row via POST /api/tasks and returns the ID of every
#' created task, so that an import can be traced and rolled back later.
#'
#' @param headers Named vector with API headers. Must include:
#'   - "content-type" = "application/json"
#'   - "X-apikey" = your_api_key
#' @param df Data frame with one row per task. Required columns:
#'   - attachable_id: External CRM ID of the person or company (>= 100000)
#'   - attachable_type: "people" or "companies"
#'   - user_id: External CRM user ID the task is assigned to
#'   - name: Task text
#'   - precise_time: Due time as POSIXct (sent to the API in UTC)
#'   Optional columns:
#'   - badge: one of "task", "call", "email", "visit", "preparation",
#'     "important" (API default: "task")
#'
#' @return Tibble with one row per sent task (rows for protected test leads
#'   are skipped and do not appear, so match results by attachable_id, user_id
#'   and name, not by position):
#'   - attachable_id, user_id, name: echoed from the input
#'   - success: TRUE if the API answered 200/201, i.e. the task exists
#'   - status_code: HTTP status code (NA on network error)
#'   - task_id: ID of the created task, NA if it failed or the response
#'     carried no ID
#'   - error_message: API or network error, NA on success
#'
#' @details
#' Protected test leads are skipped (see filter_protected_leads). One failed
#' row does not stop the remaining rows; failures are reported in the result.
#'
#' @examples
#' \dontrun{
#' headers <- c(
#'   "content-type" = "application/json",
#'   "X-apikey" = keys$crm,
#'   "Accept" = "*/*"
#' )
#'
#' tasks <- data.frame(
#'   attachable_id = 12345678,
#'   attachable_type = "people",
#'   user_id = 123456,
#'   name = "Lead nachfassen",
#'   precise_time = as.POSIXct("2026-10-27 09:00", tz = "Europe/Berlin")
#' )
#'
#' results <- create_crm_task(headers, tasks)
#' }
#'
#' @export
create_crm_task <- function(headers, df) {
  # ---- start ---- #

  if (!is.character(headers) || length(headers) == 0) {
    stop("headers must be a named character vector with API credentials")
  }

  required_headers <- c("X-apikey", "content-type")
  missing_headers <- setdiff(required_headers, names(headers))
  if (length(missing_headers) > 0) {
    stop(paste0("Missing required headers: ", paste(missing_headers, collapse = ", ")))
  }

  if (!is.data.frame(df) || nrow(df) == 0) {
    warning("No tasks to create (empty dataframe)")
    return(invisible(NULL))
  }

  validate_required_columns(df, c("attachable_id", "attachable_type", "user_id", "name", "precise_time"))

  if (!inherits(df$precise_time, "POSIXct")) {
    stop("precise_time must be POSIXct")
  }
  if (any(is.na(df$precise_time))) {
    stop("precise_time must not be NA")
  }
  if (any(is.na(df$name) | trimws(df$name) == "")) {
    stop("name must not be empty")
  }

  df <- filter_protected_leads(df)
  if (is.null(df)) {
    return(invisible(NULL))
  }

  validate_attachable_id(df)
  validate_attachable_type(df)
  validate_positive_id(df, "user_id", "user_id")

  if (!"badge" %in% names(df)) {
    df$badge <- NA_character_
  }
  allowed_badges <- c("task", "call", "email", "visit", "preparation", "important")
  invalid_badges <- setdiff(df$badge[!is.na(df$badge)], allowed_badges)
  if (length(invalid_badges) > 0) {
    stop(paste0("badge must be one of ", paste(allowed_badges, collapse = ", "),
                ". Found: ", paste(invalid_badges, collapse = ", ")))
  }

  message("Creating ", nrow(df), " tasks...")

  results <- lapply(seq_len(nrow(df)), function(i) {
    task <- list(
      name = df$name[i],
      user_id = df$user_id[i],
      attachable_id = df$attachable_id[i],
      attachable_type = transform_attachable_type(df$attachable_type[i]),
      precise_time = format(df$precise_time[i], "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    )
    if (!is.na(df$badge[i])) {
      task$badge <- df$badge[i]
    }
    body_string <- as.character(jsonlite::toJSON(list(task = task), auto_unbox = TRUE))

    row <- tibble::tibble(
      attachable_id = df$attachable_id[i],
      user_id = df$user_id[i],
      name = df$name[i],
      success = FALSE,
      status_code = NA_integer_,
      task_id = NA_integer_,
      error_message = NA_character_
    )

    tryCatch({
      response <- crm_POST("https://api.centralstationcrm.net/api/tasks", headers, body_string, "raw")
      row$status_code <- as.integer(httr::status_code(response))
      content <- httr::content(response, "text", encoding = "UTF-8")

      if (row$status_code %in% c(200L, 201L)) {
        # The task exists from here on, even if its ID cannot be read back
        row$success <- TRUE
        task_id <- tryCatch(jsonlite::fromJSON(content)$task$id, error = function(e) NULL)
        if (length(task_id) == 1) {
          row$task_id <- as.integer(task_id)
        } else {
          row$error_message <- paste0("Task created, but no task id in response: ", content)
        }
      } else {
        row$error_message <- content
      }
      row
    }, error = function(e) {
      row$error_message <- e$message
      row
    })
  })

  results <- dplyr::bind_rows(results)

  success_count <- sum(results$success)
  fail_count <- nrow(results) - success_count
  message("Created ", success_count, " tasks")
  if (fail_count > 0) {
    warning(paste0("Failed to create ", fail_count, " tasks"))
  }

  return(results)
}
