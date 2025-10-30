#' Start a Job Event
#'
#' Records a 'start' event for a job in the data_job_events table.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_start(con, "my_export_job")
#' }
job_start <- function(con, job_name) {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action) VALUES ($1, 'start')",
    params = list(job_name)
  )
}

#' Stop a Job Event
#'
#' Records a completion event for a job in the data_job_events table.
#' Can record different completion statuses: 'end', 'success', or 'failure'.
#' The run_data_job() function uses this automatically with appropriate status.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param status Character string for the event type. One of 'end', 'success', or 'failure' (default: 'end')
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_stop(con, "my_export_job")
#' job_stop(con, "my_export_job", status = "success")
#' job_stop(con, "my_export_job", status = "failure")
#' }
job_stop <- function(con, job_name, status = "end") {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action) VALUES ($1, $2)",
    params = list(job_name, status)
  )
}

#' Send Job Heartbeat
#'
#' Records a 'heartbeat' event for a long-running job to indicate it's still active.
#' This prevents the job from being considered stuck or timed out.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_heartbeat(con, "my_long_running_job")
#' }
job_heartbeat <- function(con, job_name) {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action) VALUES ($1, 'heartbeat')",
    params = list(job_name)
  )
}

#' Check if Job is Currently Running
#'
#' Determines if a job is currently running by checking the most recent event.
#' A job is considered running if the last event was 'start' or 'heartbeat'.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return Logical. TRUE if job is running, FALSE otherwise
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' if (job_is_running(con, "my_job")) {
#'   message("Job is still running")
#' }
#' }
job_is_running <- function(con, job_name) {
  last_event <- DBI::dbGetQuery(
    con,
    "
    SELECT action
    FROM processed.data_job_events
    WHERE job_name = $1
    ORDER BY executed_at DESC
    LIMIT 1
    ",
    params = list(job_name)
  )
  
  if (nrow(last_event) == 0) return(FALSE)
  
  last_event$action %in% c("start", "heartbeat")
}

#' Get Last Completed Run Time
#'
#' Returns the timestamp of when the job was last completed (ended, succeeded, or failed).
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return POSIXct timestamp of last completion, or NA if job has never completed
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' last_run <- job_last_run(con, "my_job")
#' if (!is.na(last_run)) {
#'   message("Last run: ", last_run)
#' }
#' }
job_last_run <- function(con, job_name) {
  last_end <- DBI::dbGetQuery(
    con,
    "
    SELECT executed_at
    FROM processed.data_job_events
    WHERE job_name = $1
      AND action IN ('end', 'success', 'failure')
    ORDER BY executed_at DESC
    LIMIT 1
    ",
    params = list(job_name)
  )
  
  if (nrow(last_end) == 0) return(NA)
  
  last_end$executed_at
}

#' Start Job Safely with Queue Management
#'
#' Starts a job only after any previously running instance has completed.
#' If a job with the same name is already running, this function will wait
#' until it completes or until the timeout is reached.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param poll_interval Numeric. Seconds to wait between checks (default: 5)
#' @param timeout Numeric. Maximum seconds to wait for previous job (default: 600)
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_start_safe(con, "my_job", poll_interval = 10, timeout = 300)
#' }
job_start_safe <- function(con, job_name, poll_interval = 5, timeout = 600) {
  start_time <- Sys.time()
  
  # warten, bis der vorherige Job beendet ist
  if (job_is_running(con, job_name)) {
    message(paste(
      "Job",
      job_name,
      "is already running. Request is getting queued..."
    ))

    while (job_is_running(con, job_name)) {
      if (
        as.numeric(difftime(Sys.time(), start_time, units = "secs")) > timeout
      ) {
        stop(
          "Timeout: Previous job running for too long, without heartbeat. Aborting new job start."
        )
      }
      Sys.sleep(poll_interval)
    }
  }
  
  # jetzt kann der Job sicher starten
  job_start(con, job_name)
}

#' Run Data Job with Queue Management and Error Handling
#'
#' Executes a data job function with automatic queue management, error handling,
#' and event logging. Ensures only one instance of the job runs at a time.
#' Records success/failure status in the database and re-throws errors to the caller.
#'
#' @param con Database connection object (DBI connection)
#' @param job_function Function to execute (no arguments)
#' @param job_name Character string identifying the job. If NULL, uses the function name
#' @param poll_interval Numeric. Seconds to wait between queue checks (default: 5)
#' @param timeout Numeric. Maximum seconds to wait for previous job (default: 600)
#'
#' @return Invisibly returns TRUE on success, throws error on failure
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#'
#' my_export_function <- function() {
#'   # Your data export logic here
#'   message("Exporting data...")
#' }
#'
#' run_data_job(con, my_export_function, job_name = "daily_export")
#' }
run_data_job <- function(con, job_function, job_name = NULL, poll_interval = 5, timeout = 600) {
  if (is.null(job_name)) {
    job_name <- deparse(substitute(job_function))
  }

  job_start_safe(con, job_name, poll_interval = poll_interval, timeout = timeout)

  # tryCatch gibt Error-Objekt zurück bei Fehler, NULL bei Erfolg
  result <- tryCatch({
    job_function()      # hier läuft der Job
    NULL                # kein Error = NULL
  }, error = function(e) {
    message("Job failed: ", e$message)
    e                   # Error zurückgeben
  })

  # Status basierend auf result bestimmen und Job stoppen
  status <- if (is.null(result)) "success" else "failure"
  job_stop(con, job_name, status)

  # Error re-throwen, damit Aufrufer informiert wird
  if (status == "failure") {
    stop(result)
  }

  invisible(TRUE)
}

