################################################################################-
# ----- Description -------------------------------------------------------------
#
# Serverside Sanity Check Reporter
# Logs failed sanity checks to DB and creates Asana tickets for new/reopened errors.
#
################################################################################-
# ----- Settings ----------------------------------------------------------------

## ----- libraries -----
library(dplyr)
library(DBI)
library(jsonlite)
library(log4r)

################################################################################-
# ----- Sanity Check Reporting --------------------------------------------------

#' report_sanity_check
#'
#' Reports a failed sanity check to the database and creates an Asana ticket
#' if the error is new or the previous ticket has been closed.
#'
#' Only active in non-interactive mode (i.e., on the server). When called
#' locally (interactive session), the function returns silently without action.
#'
#' The sanity check itself must be performed outside this function.
#' Pass its boolean result as \code{check_passed}.
#'
#' @param check_passed logical. Result of the sanity check (TRUE = OK, FALSE = failed).
#' @param check_name character. Unique identifier for the check
#'   (e.g. \code{"revenue_rows_not_empty"}). Used as primary key in DB.
#' @param check_message character. Human-readable description of the failure.
#' @param con A DBI database connection.
#' @param asana_api_token character. Asana Personal Access Token.
#' @param asana_project_gid character. GID of the target Asana project
#'   (e.g. the "Interne Prozesse" project).
#' @param context list. Optional named list of additional metadata
#'   (e.g. \code{list(rows = 0, table = "processed.revenue")}). Stored as JSONB.
#' @param logger A log4r logger object. If NULL, messages are written via \code{message()}.
#'
#' @return \code{invisible(check_passed)}
#' @export
report_sanity_check <- function(
  check_passed,
  check_name,
  check_message,
  con,
  asana_api_token,
  asana_project_gid,
  context = list(),
  logger  = NULL
) {

  # ----- 1. Skip in interactive sessions (local development) ------------------
  #if (interactive()) return(invisible(check_passed))

  # ----- 2. Skip if check passed ----------------------------------------------
  if (isTRUE(check_passed)) return(invisible(check_passed))

  # ----- Internal logging helper ----------------------------------------------
  .log <- function(level, msg) {
    if (!is.null(logger)) {
      switch(level,
        info  = log4r::info(logger, msg),
        warn  = log4r::warn(logger, msg),
        error = log4r::error(logger, msg)
      )
    } else {
      base::message(paste0("[", toupper(level), "] sanity_check: ", msg))
    }
  }

  # ----- 3. Determine script name ---------------------------------------------
  script_name <- tryCatch(name_of_app(), error = function(e) "unknown")

  .log("warn", paste0("Sanity check failed | script=", script_name, " | check=", check_name))

  # ----- 4. Ensure DB table exists --------------------------------------------
  tryCatch({
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS raw.metadata_sanity_check_log (
        check_name     TEXT      NOT NULL,
        script_name    TEXT      NOT NULL,
        message        TEXT,
        context        JSONB,
        asana_task_gid TEXT,
        last_seen      TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (check_name, script_name)
      );
    ")
  }, error = function(e) {
    .log("error", paste("Could not ensure DB table exists:", e$message))
  })

  # ----- 5. Read current row to preserve asana_task_gid ----------------------
  existing_gid <- tryCatch({
    existing <- tbl(con, I("raw.metadata_sanity_check_log")) |>
      filter(check_name == !!check_name, script_name == !!script_name) |>
      select(asana_task_gid) |>
      collect()
    if (nrow(existing) == 0) NA_character_ else existing$asana_task_gid[[1]]
  }, error = function(e) {
    .log("warn", paste("Could not read existing sanity check row:", e$message))
    NA_character_
  })

  # ----- 6. Check Asana: is there an open ticket? -----------------------------
  needs_new_ticket <- TRUE
  final_gid        <- existing_gid

  if (!is.na(existing_gid) && nchar(existing_gid) > 0) {
    task_details <- tryCatch(
      get_asana_task_details(existing_gid, asana_api_token, logger),
      error = function(e) NULL
    )

    if (!is.null(task_details)) {
      completed <- isTRUE(task_details$data$completed)
      if (!completed) {
        .log("info", paste0("Asana ticket still open (GID: ", existing_gid, ") — skipping new ticket"))
        needs_new_ticket <- FALSE
      } else {
        .log("info", paste0("Asana ticket was closed (GID: ", existing_gid, ") — creating new ticket"))
      }
    } else {
      .log("warn", paste0("Could not retrieve Asana ticket (GID: ", existing_gid, ") — creating new ticket as fallback"))
    }
  }

  # ----- 7. Create new Asana ticket if needed ---------------------------------
  if (needs_new_ticket) {
    task_title <- paste0("[SANITY] ", script_name, " | ", check_name)

    context_text <- if (length(context) > 0) {
      paste0("\n\nContext:\n", jsonlite::toJSON(context, auto_unbox = TRUE, pretty = TRUE))
    } else {
      ""
    }

    task_notes <- paste0(
      "Script: ", script_name,
      "\nCheck: ", check_name,
      "\nMessage: ", check_message,
      "\nTime: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      context_text
    )

    due_tomorrow <- format(Sys.Date() + 1, "%Y-%m-%d")

    new_gid <- tryCatch(
      create_asana_task(
        asana_project_gid, task_title, task_notes, asana_api_token, logger,
        due_on        = due_tomorrow,
        custom_fields = list("1209106213751972" = 1)
      ),
      error = function(e) {
        .log("error", paste("Failed to create Asana task:", e$message))
        NULL
      }
    )

    if (!is.null(new_gid)) final_gid <- new_gid
  }

  # ----- 8. Single upsert with final state ------------------------------------
  context_json <- tryCatch(
    jsonlite::toJSON(context, auto_unbox = TRUE),
    error = function(e) "{}"
  )

  tryCatch({
    DBI::dbExecute(con, "
      INSERT INTO raw.metadata_sanity_check_log
        (check_name, script_name, message, context, asana_task_gid, last_seen)
      VALUES ($1, $2, $3, $4::jsonb, $5, $6)
      ON CONFLICT (check_name, script_name) DO UPDATE SET
        message        = EXCLUDED.message,
        context        = EXCLUDED.context,
        asana_task_gid = EXCLUDED.asana_task_gid,
        last_seen      = EXCLUDED.last_seen;
    ", list(
      check_name,
      script_name,
      check_message,
      as.character(context_json),
      final_gid,
      format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    ))
  }, error = function(e) {
    .log("error", paste("Could not upsert sanity check log:", e$message))
  })

  return(invisible(check_passed))
}
