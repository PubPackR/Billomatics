#' Start a Job Event
#'
#' Records a 'start' event for a job in the data_job_events table.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param request_id UUID identifying this specific job run (optional for backward compatibility)
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_start(con, "my_export_job")
#'
#' # With request_id for queue management
#' request_id <- uuid::UUIDgenerate()
#' job_start(con, "my_export_job", request_id)
#' }
job_start <- function(con, job_name, request_id = NULL) {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action, request_id) VALUES ($1, 'start', $2)",
    params = list(job_name, request_id)
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
#' @param request_id UUID identifying this specific job run (optional for backward compatibility)
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_stop(con, "my_export_job")
#' job_stop(con, "my_export_job", status = "success")
#' job_stop(con, "my_export_job", status = "failure")
#'
#' # With request_id for queue management
#' request_id <- uuid::UUIDgenerate()
#' job_stop(con, "my_export_job", "success", request_id = request_id)
#' }
job_stop <- function(con, job_name, status = "end", request_id = NULL) {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action, request_id) VALUES ($1, $2, $3)",
    params = list(job_name, status, request_id)
  )
}

#' Send Job Heartbeat
#'
#' Records a 'heartbeat' event for a long-running job to indicate it's still active.
#' This prevents the job from being considered stuck or timed out.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param request_id UUID identifying this specific job run (optional for backward compatibility)
#'
#' @return Number of rows affected (from DBI::dbExecute)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' job_heartbeat(con, "my_long_running_job")
#'
#' # With request_id for queue management
#' request_id <- uuid::UUIDgenerate()
#' job_heartbeat(con, "my_long_running_job", request_id)
#' }
job_heartbeat <- function(con, job_name, request_id = NULL) {
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action, request_id) VALUES ($1, 'heartbeat', $2)",
    params = list(job_name, request_id)
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

#' Insert Job Request into Queue
#'
#' Creates a new 'request' event for a job, marking its place in the queue.
#' Returns a unique request_id that links all subsequent events for this job run.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return UUID string identifying this request
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' request_id <- insert_job_request(con, "daily_export")
#' # Later: job_start(con, "daily_export", request_id)
#' }
insert_job_request <- function(con, job_name) {
  # Generate unique request ID
  request_id <- uuid::UUIDgenerate()

  # Insert request event
  DBI::dbExecute(
    con,
    "INSERT INTO processed.data_job_events (job_name, action, request_id) VALUES ($1, 'request', $2)",
    params = list(job_name, request_id)
  )

  return(request_id)
}

#' Get First Queued Request
#'
#' Finds the oldest unprocessed request for a given job.
#' A request is considered unprocessed if it has no corresponding 'start' event.
#' Automatically cleans up stuck requests older than max_age_seconds.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param max_age_seconds Numeric. Maximum age in seconds for stuck requests before auto-cleanup (default: NULL, no cleanup)
#'
#' @return List with request_id and request_time, or NULL if queue is empty
#'
#' @details
#' If max_age_seconds is provided, this function will automatically mark stuck requests
#' as 'failure' if they are older than the specified age and have no corresponding
#' 'start' event. This prevents the queue from getting permanently blocked by
#' requests that crashed before starting.
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' first <- get_first_queued_request(con, "daily_export")
#' if (!is.null(first)) {
#'   message("Next in queue: ", first$request_id)
#' }
#'
#' # With automatic cleanup of requests older than 1 hour
#' first <- get_first_queued_request(con, "daily_export", max_age_seconds = 3600)
#' }
get_first_queued_request <- function(con, job_name, max_age_seconds = NULL) {
  # Clean up stuck requests if max_age_seconds is specified
  if (!is.null(max_age_seconds)) {
    DBI::dbExecute(
      con,
      "
      INSERT INTO processed.data_job_events (job_name, action, request_id)
      SELECT job_name, 'failure', request_id
      FROM processed.data_job_events
      WHERE job_name = $1
        AND action = 'request'
        AND executed_at < NOW() - INTERVAL '1 second' * $2
        AND request_id NOT IN (
          SELECT COALESCE(request_id, '00000000-0000-0000-0000-000000000000')
          FROM processed.data_job_events
          WHERE job_name = $1
            AND action IN ('start', 'success', 'failure', 'end')
            AND request_id IS NOT NULL
        )
      ",
      params = list(job_name, max_age_seconds)
    )
  }

  # Get the first queued request
  result <- DBI::dbGetQuery(
    con,
    "
    SELECT request_id, executed_at as request_time
    FROM processed.data_job_events
    WHERE job_name = $1
      AND action = 'request'
      AND request_id NOT IN (
        SELECT request_id
        FROM processed.data_job_events
        WHERE job_name = $1
          AND action = 'start'
          AND request_id IS NOT NULL
      )
    ORDER BY executed_at ASC
    LIMIT 1
    ",
    params = list(job_name)
  )

  if (nrow(result) == 0) {
    return(NULL)
  }

  list(
    request_id = result$request_id,
    request_time = result$request_time
  )
}

#' Start Job Safely with Queue Management
#'
#' Starts a job only after being first in queue and acquiring advisory lock.
#' Implements True FIFO queue using request events and race-safe lock acquisition.
#' If a job is already running, this function will wait in queue until it's this
#' request's turn AND the lock can be acquired.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#' @param poll_interval Numeric. Seconds to wait between queue checks (default: 5)
#' @param timeout Numeric. Maximum seconds to wait AFTER becoming first in queue (default: 600)
#'
#' @return UUID string (request_id) identifying this job run
#'
#' @details
#' Queue mechanism:
#' 1. Inserts a 'request' event with unique request_id
#' 2. Polls until this request is first in queue (FIFO order by timestamp)
#' 3. Tries to acquire PostgreSQL advisory lock
#' 4. Once first in queue AND lock acquired, inserts 'start' event
#' 5. Returns request_id to link all subsequent events for this run
#'
#' Timeout behavior:
#' - The timeout only applies AFTER this request becomes first in queue
#' - While waiting behind other requests, there is NO timeout
#' - Timer resets each time queue position changes
#' - This ensures fair queuing even with many requests ahead
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#' request_id <- job_start_safe(con, "my_job", poll_interval = 10, timeout = 300)
#' # Use request_id for job_stop(), job_heartbeat(), etc.
#' }
job_start_safe <- function(con, job_name, poll_interval = 5, timeout = 600) {
  # Insert our request into the queue
  request_id <- insert_job_request(con, job_name)

  first_in_queue_since <- NULL  # Track when we became first
  previous_position <- NULL      # Track queue position changes
  message_shown <- FALSE

  # Poll until we're first in queue AND can acquire lock
  repeat {
    # Check if we're first in queue (with automatic cleanup of stuck requests older than timeout * 5)
    first_request <- get_first_queued_request(con, job_name, max_age_seconds = timeout * 5)

    if (is.null(first_request)) {
      # No pending requests (shouldn't happen since we just added one)
      stop("Error: Request not found in queue")
    }

    # Are we first in line?
    if (first_request$request_id == request_id) {
      # We're first! Record when we became first (for timeout tracking)
      if (is.null(first_in_queue_since)) {
        first_in_queue_since <- Sys.time()
        if (message_shown) {
          message("Now first in queue for job '", job_name, "', waiting for lock...")
        }
      }

      # Try to acquire the lock
      if (acquire_job_lock(con, job_name)) {
        # Success! We have both queue priority and the lock
        break
      }

      # Lock held by another process - check timeout since becoming first
      elapsed_at_front <- as.numeric(difftime(Sys.time(), first_in_queue_since, units = "secs"))
      if (elapsed_at_front > timeout) {
        stop(
          "Timeout: Job '", job_name, "' lock held for ", timeout,
          " seconds. Another instance may be stuck."
        )
      }
    } else {
      # Not our turn yet - reset the "first in queue" timer
      first_in_queue_since <- NULL

      # Show queue position message
      if (!message_shown) {
        message("Job '", job_name, "' is already running. Request is getting queued...")
        message_shown <- TRUE
      }
    }

    Sys.sleep(poll_interval)
  }

  # We have the lock and we're first in queue - start the job
  job_start(con, job_name, request_id = request_id)

  return(request_id)
}

#' Acquire Advisory Lock for Job
#'
#' Attempts to acquire a PostgreSQL advisory lock for the given job name.
#' Advisory locks are session-level locks stored in PostgreSQL memory that
#' prevent race conditions when multiple processes try to start the same job.
#' The lock is automatically released when the database connection closes.
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return Logical. TRUE if lock was successfully acquired, FALSE if lock is
#'   already held by another session
#'
#' @details
#' This function uses PostgreSQL's pg_try_advisory_lock() which:
#' - Returns immediately (non-blocking)
#' - Uses a numeric lock ID derived from CRC32 hash of job_name
#' - Is automatically released on connection close or manual release
#' - Is session-scoped (not transaction-scoped)
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#'
#' if (acquire_job_lock(con, "daily_export")) {
#'   message("Lock acquired, running job...")
#'   # ... run job ...
#'   release_job_lock(con, "daily_export")
#' } else {
#'   message("Lock already held by another process")
#' }
#' }
acquire_job_lock <- function(con, job_name) {
  # Convert job name to integer lock ID using CRC32 hash
  # CRC32 produces a 32-bit hash suitable for PostgreSQL bigint
  lock_id <- as.numeric(paste0("0x", digest::digest(job_name, algo = "crc32", serialize = FALSE)))

  # Try to acquire advisory lock (non-blocking)
  result <- tryCatch({
    DBI::dbGetQuery(
      con,
      "SELECT pg_try_advisory_lock($1) as acquired",
      params = list(lock_id)
    )
  }, error = function(e) {
    stop("Failed to acquire advisory lock: ", e$message)
  })

  # Ensure we have a valid result
  if (is.null(result) || nrow(result) == 0 || is.na(result$acquired)) {
    stop("Advisory lock query returned invalid result")
  }

  return(result$acquired)
}

#' Release Advisory Lock for Job
#'
#' Releases a PostgreSQL advisory lock previously acquired for the given job name.
#' Should be called after job completion or in cleanup handlers (on.exit).
#'
#' @param con Database connection object (DBI connection)
#' @param job_name Character string identifying the job
#'
#' @return invisible(NULL)
#'
#' @details
#' This function uses PostgreSQL's pg_advisory_unlock(). The lock ID must match
#' the one used in acquire_job_lock() (derived from CRC32 hash of job_name).
#' Attempting to release a lock not held by the current session will generate
#' a warning from PostgreSQL but will not error.
#'
#' @examples
#' \dontrun{
#' con <- postgres_connect(needed_tables = c("processed.data_job_events"))
#'
#' if (acquire_job_lock(con, "daily_export")) {
#'   on.exit(release_job_lock(con, "daily_export"))
#'   # ... run job ...
#' }
#' }
release_job_lock <- function(con, job_name) {
  # Convert job name to same lock ID used in acquire
  lock_id <- as.numeric(paste0("0x", digest::digest(job_name, algo = "crc32", serialize = FALSE)))

  # Release the advisory lock
  DBI::dbExecute(
    con,
    "SELECT pg_advisory_unlock($1)",
    params = list(lock_id)
  )

  invisible(NULL)
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
#' @param timeout Numeric. Maximum seconds to wait in queue (applies only when first in queue, default: 600)
#' @param hard_timeout Logical. If TRUE, enforces execution timeout equal to queue timeout (default: FALSE)
#'
#' @return Invisibly returns a list with elements: success (TRUE), status ("success"),
#'   result (return value from job_function), duration_minutes (numeric), and request_id (UUID).
#'   Throws error on failure.
#'
#' @details
#' Timeout behavior:
#' - `timeout` applies when waiting for lock acquisition (only after becoming first in queue)
#' - `hard_timeout = TRUE` also enforces this timeout during job execution
#' - This prevents runaway jobs from consuming resources indefinitely
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
#' # Basic usage
#' run_data_job(con, my_export_function, job_name = "daily_export")
#'
#' # With hard timeout (kills job if it runs longer than 600 seconds)
#' run_data_job(con, my_export_function, job_name = "daily_export",
#'              timeout = 600, hard_timeout = TRUE)
#' }
#' @export
run_data_job <- function(con, job_function, job_name = NULL, poll_interval = 5, timeout = 600, hard_timeout = FALSE) {
  # Validate parameters early
  if (!is.function(job_function)) {
    stop("job_function must be a function")
  }

  if (is.null(job_name)) {
    job_name <- deparse(substitute(job_function))
  }

  # Use job_start_safe which now handles advisory locks + queue + request_id
  request_id <- job_start_safe(con, job_name, poll_interval = poll_interval, timeout = timeout)

  # Use environment to share state between on.exit and function body
  # This avoids using <<- which violates coding guidelines
  state <- new.env()
  state$job_stopped <- FALSE
  state$lock_acquired <- !is.null(request_id)

  # Ensure cleanup happens even on interrupt (Ctrl+C)
  # on.exit runs even when function is interrupted
  on.exit({
    # Release advisory lock first
    if (state$lock_acquired) {
      release_job_lock(con, job_name)
    }

    # Then handle job stop
    if (!state$job_stopped) {
      tryCatch({
        job_stop(con, job_name, status = "failure", request_id = request_id)
        message("\nJob interrupted - marked as failed in database")
      }, error = function(e) {
        message("\nWarning: Could not mark job as failed in database: ", e$message)
      })
    }
  }, add = TRUE)

  start_time <- Sys.time()

  # Execute job with optional hard timeout
  job_result <- tryCatch({
    if (hard_timeout) {
      # Enforce execution timeout using R.utils::withTimeout()
      R.utils::withTimeout({
        job_function()
      }, timeout = timeout, onTimeout = "error")
    } else {
      # No execution timeout, just run the job
      job_function()
    }
  }, TimeoutException = function(e) {
    # Specific handling for timeout
    timeout_error <- simpleError(
      paste0("Job execution exceeded hard timeout of ", timeout, " seconds")
    )
    message("Job timed out: ", conditionMessage(timeout_error))
    timeout_error
  }, error = function(e) {
    # General error handling
    message("Job failed: ", e$message)
    e                   # Return error
  })

  # Determine status based on result and stop job
  status <- if (inherits(job_result, "error")) "failure" else "success"

  end_time <- Sys.time()
  duration <- round(
    as.numeric(difftime(end_time, start_time, units = "mins")),
    2
  )
  message(paste0(job_name, " completed in ", duration, " minutes with status: ", status))

  # Compare with defined timeout
  if (duration > (timeout / 60)) {
    message(paste0(
      "Warning: Job duration (", duration, " mins) exceeded timeout (", timeout / 60, " mins)."
    ))
  }

  job_stop(con, job_name, status, request_id = request_id)
  state$job_stopped <- TRUE  # Mark as stopped to prevent on.exit from running

  # Re-throw error to inform caller
  if (status == "failure") {
    stop(job_result)
  }

  invisible(list(
    success = TRUE,
    status = status,
    result = job_result,
    duration_minutes = duration,
    request_id = request_id
  ))
}

