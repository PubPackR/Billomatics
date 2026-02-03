################################################################################-
# ----- Description -------------------------------------------------------------
#
# Functions for interacting with the CRM Tasks API endpoint.
# Includes functions to mark tasks as finished.
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
