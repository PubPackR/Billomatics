################################################################################-
# ----- Description -------------------------------------------------------------
#
# This Script contains various functions for easier database management
#

################################################################################-
# ----- Start -------------------------------------------------------------------

#' custom_decrypt_db
#'
#' This function is used to decrypt a dataframe that is saved within a sqlite database.
#' You are able to specifiy which columns you want to decrypt.
#'
#' @param df The dataframe to be decrypted.
#' @param columns_to_decrypt The columns that need to be decrpyted.
#' @param key The key.
#' @return The specified columns of the database table gets decrypted
#' @export
custom_decrypt_db <- function(df, columns_to_decrypt, key = NULL) {
  df_decrypted <- df
  columns_to_decrpyt <- columns_to_decrypt %||% names(df)

  df_decrypted[columns_to_decrypt] <- lapply(df[columns_to_decrypt], function(col) {
    sapply(col, function(value) {
      if (!is.na(value)) {
        data <- base64enc::base64decode(value)
        iv <- data[1:16]
        encrypted_data <- data[-(1:16)]
        decrypted <- openssl::aes_cbc_decrypt(encrypted_data, key = charToRaw(key), iv = iv)
        rawToChar(decrypted)
      } else {
        NA
      }
    })
  })
  return(as.data.frame(df_decrypted, stringsAsFactors = FALSE))
}

#' postgres_upsert_data
#'
#' Performs an UPSERT (insert or update) operation into a PostgreSQL table.
#' Optionally, rows that no longer exist in the provided data frame can be deleted,
#' and automatically generated IDs (e.g., serial) can be returned if requested.
#'
#' @param con An active database connection (e.g., created with `DBI::dbConnect()`).
#' @param schema The schema name in the database (e.g., "raw").
#' @param table The table name (e.g., "my_table" or "raw.my_table").
#' @param data A `data.frame` containing the data to be inserted or updated.
#' @param match_cols Character vector of column names used as conflict keys.
#' @param delete_missing Logical. If `TRUE`, rows that are no longer present in the data frame
#'        will be deleted (based on the conflict keys).
#' @param returning_cols Optional character vector of columns to return after the UPSERT.
#'
#' @return A `data.frame` with the returned columns (if `returning_cols` is set), otherwise `invisible(NULL)`.
#'
#' @details
#' - If `match_cols` is not specified, the function looks for a typical ID column (e.g., "id").
#' - Columns in `data` that do not exist in the target table are ignored with a warning.
#' - Only columns other than `created_at`, `updated_at`, and `match_cols` are updated.
#' - A temporary table is created and used to perform the UPSERT.
#' - If `delete_missing = TRUE`, all entries not found in the current data based on `match_cols` will be deleted.
#'
#' @examples
#' \dontrun{
#'   postgres_upsert_data(
#'     con = my_connection,
#'     schema = "raw",
#'     table = "my_table",
#'     data = my_dataframe,
#'     match_cols = c("id"),
#'     delete_missing = TRUE,
#'     returning_cols = c("id", "updated_at")
#'   )
#' }
#'
#' @export
postgres_upsert_data <- function(con, schema, table, data,
                                 match_cols = NULL,
                                 delete_missing = FALSE,
                                 returning_cols = NULL) {

  # Überprüfen, ob das Argument 'data' ein DataFrame ist
  if (!is.data.frame(data)) {
    stop("Das Argument 'data' muss ein Data Frame sein.")
  }

  # Wenn 'data' keine Zeilen enthält, gibt es keine Änderungen
  if (nrow(data) == 0) {
    message("Hinweis: 'data' enthält keine Zeilen – es wurden keine Änderungen vorgenommen.")
    return(data)
  }

  # Tabelle vollständig qualifizieren
  full_table <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = schema, table = table))
  colnames_data <- colnames(data)
  table_columns <- DBI::dbListFields(con, DBI::Id(schema = schema, table = table))
  missing_cols <- setdiff(colnames_data, table_columns)
  if (length(missing_cols) > 0) {
    message("Folgende Spalten sind nicht in der Zieltabelle vorhanden und werden ignoriert: ",
            paste(missing_cols, collapse = ", "))
  }
  data <- data[, intersect(colnames_data, table_columns), drop = FALSE]
  colnames_data <- setdiff(colnames(data), c("id", "updated_at"))  # Aktualisieren, da sich data geändert hat

  # Spalten, die geupdatet werden sollen (ohne created_at und match_cols)
  update_cols <- setdiff(colnames_data, c("created_at", "updated_at", match_cols))

  # Automatically handle 'is_deleted' if present
  if ("is_deleted" %in% table_columns && !"is_deleted" %in% colnames_data) {
    data$is_deleted <- FALSE
    colnames_data <- c(colnames_data, "is_deleted")
    update_cols <- c(update_cols, "is_deleted")
  }

  # Wenn keine 'match_cols' angegeben sind, prüfen, ob es eine typische ID-Spalte gibt
  if (is.null(match_cols)) {
    match_cols <- intersect(colnames_data, c("id"))
    if (length(match_cols) == 0) stop("Keine 'match_cols' angegeben und keine typische ID-Spalte gefunden.")
  }

  # Temporäre Tabelle erstellen, um die Daten zu übertragen
  temp_table <- paste0("temp_upsert_", format(Sys.time(), "%Y%m%d%H%M%S"), "_",
                       round(as.numeric(Sys.time()) * 1000) %% 1000, "_",
                       sample.int(999999, 1))
  DBI::dbWriteTable(con, temp_table, data, temporary = TRUE, overwrite = TRUE)

  # Update-Klausel erstellen
  update_clause <- if (length(update_cols) > 0) {
    paste0(update_cols, " = EXCLUDED.", update_cols, collapse = ",\n  ")
  } else {
    ""
  }

  # Konfliktbehandlung (DO UPDATE oder DO NOTHING)
  conflict_action <- if (length(update_cols) > 0) {
    glue::glue("DO UPDATE SET\n  {update_clause}")
  } else {
    message("Keine Spalten zum Aktualisieren vorhanden. Es wird 'DO NOTHING' verwendet. (Nur Unique-Spalten)")
    "DO NOTHING"
  }

  # Die endgültige SQL-Abfrage zusammenstellen
  query <- glue::glue("
    INSERT INTO {full_table} ({paste(colnames_data, collapse = ', ')})
    SELECT {paste(colnames_data, collapse = ', ')}
    FROM {DBI::dbQuoteIdentifier(con, temp_table)}
    ON CONFLICT ({paste(match_cols, collapse = ', ')})
    {conflict_action};
  ")

  # Ausführen der Abfrage
  tryCatch({
    DBI::dbExecute(con, query)
  }, error = function(e) {
    if (grepl("no unique or exclusion constraint matching the ON CONFLICT specification",
              e$message)) {
      stop(
        "Fehler beim Upsert: Für die angegebenen 'match_cols' existiert kein UNIQUE- oder PRIMARY KEY-Constraint.\n",
        glue::glue("  CREATE UNIQUE INDEX ON {schema}.{table}({paste(match_cols, collapse = ', ')});\n")
        )
    } else {
      stop("Fehler beim Upsert: ", e$message)
    }
  })

  # Temporäre Tabelle entfernen
  DBI::dbRemoveTable(con, temp_table)

  # Optional: Löschen von nicht mehr vorhandenen Einträgen
  if (delete_missing) {
    temp_table <- paste0("temp_delete_", format(Sys.time(), "%Y%m%d%H%M%S"), "_",
                         round(as.numeric(Sys.time()) * 1000) %% 1000, "_",
                         sample.int(999999, 1))

    # Temporäre Tabelle erstellen und befüllen
    DBI::dbWriteTable(con, temp_table, unique(data[match_cols]), temporary = TRUE, overwrite = TRUE)

    # Bedingung für den JOIN bauen
    join_condition <- paste(
      sprintf("t.%s = temp.%s", match_cols, match_cols),
      collapse = " AND "
    )

    delete_query <- glue::glue("
      DELETE FROM {full_table} AS t
      WHERE NOT EXISTS (
        SELECT 1
        FROM {`temp_table`} AS temp
        WHERE {join_condition}
      );
    ")

    DBI::dbExecute(con, delete_query)
  }

  # Rückgabe-Abfrage nach match_cols (optional)
  if (!is.null(returning_cols)) {

    # SQL-Query ohne WHERE-Klausel
    select_query <- glue::glue("
      SELECT {paste(returning_cols, collapse = ', ')}
      FROM {full_table};
    ")

    return(DBI::dbGetQuery(con, select_query))
  }

  # Das Ergebnis (die zurückgegebenen Spalten) zurückgeben
  return(invisible(NULL))
}


#' postgres_add_metadata
#'
#' Adds or updates metadata in the PostgreSQL database. If the `is_new` argument is set to `TRUE`, it ensures that
#' the metadata with the given key does not already exist in the database. If the metadata exists, an error is raised.
#' If `is_new` is `FALSE` (default), it performs an upsert (insert or update), updating the metadata value for the
#' given key.
#'
#' @param con The database connection object, created using DBI.
#' @param key The metadata key. This is a unique identifier for the metadata.
#' @param value The metadata value that will be associated with the given key.
#' @param is_new A logical value indicating whether the metadata should be treated as new. If set to `TRUE`, an error
#'               will be raised if the metadata with the given key already exists. The default value is `FALSE`, in which
#'               case the metadata will be updated if it already exists.
#' @return A message indicating whether the metadata was successfully added or updated, or an error message if the
#'         metadata already exists and `is_new` is `TRUE`.
#' @details
#' - The function checks whether a metadata entry already exists for the given key if `is_new == TRUE`.
#' - If the metadata exists and `is_new == TRUE`, an error is raised with a message indicating the conflict.
#' - If `is_new == FALSE`, the function uses an `INSERT ... ON CONFLICT` statement to either insert new metadata or update
#'   the existing metadata value.
#' - The table `raw.metadata_jobs_and_datafiles` is used to store the metadata. The table should have at least two columns:
#'   `key` and `value`.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_add_metadata(connection, "job_1", "Completed", is_new = TRUE)
#' }
#'
#' @export
postgres_add_metadata <- function(con, key, value, is_new = FALSE) {
  # Check if metadata already exists (if is_new is TRUE)
  if (is_new) {
    check_query <- sprintf("
      SELECT COUNT(*) FROM raw.metadata_jobs_and_datafiles WHERE key = '%s';
    ", key)
    count_result <- DBI::dbGetQuery(con, check_query)

    if (count_result$count > 0) {
      stop(sprintf("Error: Metadata with key '%s' already exists and is not allowed to be updated.
                   If you want to only update the values, use is_new == FALSE as Parameter.", key))
    }
  }

  # Perform the insert or update operation
  query <- sprintf("
    INSERT INTO raw.metadata_jobs_and_datafiles (key, value)
    VALUES ('%s', '%s')
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
  ", key, value)

  DBI::dbExecute(con, query)
  message(sprintf("Metadata for key '%s' has been successfully added/updated.", key))
}

#' postgres_read_metadata
#'
#' Reads metadata from the PostgreSQL database based on a specified key.
#' This function retrieves the `key`, `value`, and `updated_at` of the metadata entry associated with the provided key.
#' If the key doesn't exist in the metadata table, it returns NULL.
#'
#' @param con The database connection object, created using DBI.
#' @param key The metadata key to retrieve.
#' @return A data frame containing the metadata `key`, `value`, and `updated_at`.
#'         If the key doesn't exist, it returns `NULL` and prints a message indicating that no metadata was found.
#' @details
#' - The function performs a `SELECT` query to retrieve the metadata entry associated with the provided `key` from the table
#'   `raw.metadata_jobs_and_datafiles`.
#' - The returned data frame contains three columns: `key`, `value`, and `updated_at`.
#' - If no metadata entry is found for the specified key, the function returns `NULL` and prints a message indicating that no
#'   metadata was found for the given key.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' metadata <- postgres_read_metadata(connection, "job_1")
#' print(metadata)
#' }
#'
#' @export
postgres_read_metadata <- function(con, key) {
  query <- sprintf("
    SELECT key, value, updated_at FROM raw.metadata_jobs_and_datafiles WHERE key = '%s';
  ", key)

  result <- DBI::dbGetQuery(con, query)

  # Check if result is empty
  if (nrow(result) == 0) {
    message(sprintf("No metadata found for key '%s'.", key))
    return(NULL)
  }

  return(result)
}

#' postgres_select
#'
#' Selects data from a PostgreSQL table and logs the access in raw.metadata_data_selection_log.
#' This function performs a safe SQL `SELECT` operation on the specified table and optionally applies a `WHERE` clause and row limit.
#' It also logs the data selection operation for audit purposes.
#'
#' @param con The database connection object, created using DBI. This object should represent a valid connection to a PostgreSQL database.
#' @param schema The schema of the table where the data will be selected from. This is a required parameter if `table` is provided with a schema (e.g., `public.my_table`).
#' @param table The name of the table from which data is selected. The table name should be specified as `schema.table` or just `table` if the schema is provided separately.
#' @param columns A character vector of column names to select. If not specified, all columns (`*`) are selected by default.
#'                Example: `columns = c("column1", "column2")`. Default is `*`.
#' @param where Optional SQL `WHERE` clause (without the word WHERE). This is a string that specifies the condition for filtering the rows to be selected.
#'              It should not include the word `WHERE`. Example: `where = "age > 30"`.
#' @param limit Optional number of rows to limit the result. If not provided, no limit is applied. Example: `limit = 10` will return at most 10 rows.
#'
#' @return A data frame containing the selected rows from the specified table. If the query fails, an empty data frame is returned,
#'         and an error message is logged.
#'
#' @details
#' - This function builds a SQL `SELECT` query by combining the specified columns, table, optional `WHERE` clause, and row limit.
#' - The function logs the successful execution of the query along with the number of rows retrieved in the `raw.metadata_data_selection_log` table.
#' - In case of an error, the failure is logged, including the SQL query and the error message.
#' - SQL parameters (such as in the `WHERE` clause) should be sanitized and handled appropriately to avoid SQL injection risks.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' # Select all data from the 'public.users' table
#' result <- postgres_select(connection, "public", "users")
#' # Select specific columns with a WHERE clause
#' result <- postgres_select(connection, "public", "users", columns = c("id", "name"), where = "age > 30", limit = 10)
#' }
#'
#' @export
postgres_select <- function(con, schema, table, columns = "*", where = NULL, limit = NULL) {
  # Determine full schema.table name for logging
  original_input <- if (!is.null(schema)) paste0(schema, ".", table) else table

  # Extract schema and table if combined in `table`
  if (grepl("\\.", table)) {
    parts <- strsplit(table, "\\.")[[1]]
    if (length(parts) == 2) {
      schema <- parts[1]
      table <- parts[2]
    } else {
      stop("Invalid table format. Use 'schema.table' or specify schema separately.")
    }
  } else if (is.null(schema)) {
    stop("Schema must be provided either via `schema` argument or in `table` as 'schema.table'.")
  }

  # Prepare query
  cols <- if (length(columns) == 1 && columns == "*") "*" else paste(columns, collapse = ", ")
  query <- "SELECT %s FROM %s.%s"
  query <- sprintf(query, cols, schema, table)

  if (!is.null(where)) {
    query <- paste(query, "WHERE", where)
  }
  if (!is.null(limit)) {
    query <- paste(query, "LIMIT", limit)
  }

  # Ausführung + Logging
  result <- tryCatch({
    df <- DBI::dbGetQuery(con, query)
    postgres_log_data_selection(con, original_input, success = TRUE)
    df
  }, error = function(e) {
    postgres_log_data_selection(con, original_input, success = FALSE, error_message = e$message)
    message("Error in postgres_select: ", e$message)
    return(NULL)
  })

  return(result)
}

#' postgres_log_data_selection
#'
#' Logs access to a PostgreSQL table in `raw.metadata_data_selection_log` for audit and tracking purposes.
#'
#' @param con Database connection object, created using DBI. This object should represent a valid connection to a PostgreSQL database.
#' @param original_input Full schema.table name as a string. This is the table name and schema where the data was selected from.
#' @param success Logical value indicating whether the data access was successful. Default is `TRUE`.
#' @param error_message Optional error message, which will be logged if `success` is `FALSE`. If the operation was successful, this can be omitted (default is `NA_character_`).
#'
#' @details
#' - This function logs the access of a table in the `raw.metadata_data_selection_log` table. It captures the time of access, the user who executed the query, the working directory, the success status, and any error messages if applicable.
#' - The `raw.metadata_data_selection_log` table is automatically created if it does not already exist.
#' - This function is useful for tracking data access and operations performed on tables for auditing and troubleshooting purposes.
#' - The `error_message` is only recorded when `success` is `FALSE`, and is left empty (NA) if the operation was successful.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' # Log successful data selection from the "public.users" table
#' postgres_log_data_selection(connection, "public.users", success = TRUE)
#' # Log a failed data selection with an error message
#' postgres_log_data_selection(connection, "public.users", success = FALSE, error_message = "Query failed due to timeout.")
#' }
#'
#' @export
postgres_log_data_selection <- function(con, original_input, success = TRUE, error_message = NA_character_) {

  # Infos sammeln
  username <- Sys.info()[["user"]]
  working_dir <- getwd()
  timestamp <- Sys.time()

  # Log-Tabelle anlegen, falls sie nicht existiert
  logging_table_exists <- DBI::dbExistsTable(con, DBI::Id(schema = "raw", table = "metadata_data_selection_log"))

  if (!logging_table_exists) {
    DBI::dbExecute(con, "
      CREATE TABLE raw.metadata_data_selection_log (
        id SERIAL PRIMARY KEY,
        schema_table TEXT,
        timestamp TIMESTAMP,
        working_directory TEXT,
        username TEXT,
        success BOOLEAN,
        error_message TEXT
      );
    ")
  }

  # Logging durchführen
  DBI::dbExecute(con, glue::glue_sql("
    INSERT INTO raw.metadata_data_selection_log
    (schema_table, timestamp, working_directory, username, success, error_message)
    VALUES ({original_input}, {timestamp}, {working_dir}, {username}, {success}, {error_message})
  ", .con = con))
}

#' Collect Data and Log the Table Names
#'
#' Executes a `dbplyr` query and logs the names of all involved database tables.
#'
#' @param tbl Ein `dbplyr`-Lazy Query-Objekt, dessen Ergebnis abgefragt wird.
#' @param con Die Datenbankverbindung für das Logging. Standardmäßig wird `con` aus der globalen Umgebung gezogen.
#'
#' @return Das Ergebnis von `dplyr::collect(tbl)`, typischerweise ein Data Frame.
#'
#' @details
#' Die Funktion ermittelt Tabellen-Namen aus `tbl$lazy_query` über eine rekursive Suche.
#' Diese Namen werden in eine Protokolltabelle geschrieben.
#' Bei Fehlern wird dies ebenfalls protokolliert und der Fehler erneut geworfen.
#'
#' @examples
#' tbl <- dplyr::tbl(con, "some_table")
#' df <- collect_and_log(tbl)
#'
#' @export
collect_and_log <- function(tbl, con = get("con", envir = globalenv())) {
  tryCatch({
    # Run the query
    result <- dplyr::collect(tbl)

    table_names <- suppressWarnings(extract_all_table_names(tbl$lazy_query))

    # Log each table
    for (tbl_name in unique(table_names)) {
      Billomatics::postgres_log_data_selection(
        con = con,
        original_input = tbl_name,
        success = TRUE
      )
    }

    return(result)

  }, error = function(e) {
    # Attempt to extract table names from lazy_query$table_names if it exists
    table_names <- suppressWarnings(extract_all_table_names(tbl$lazy_query))

    for (tbl_name in unique(table_names)) {
      Billomatics::postgres_log_data_selection(
        con = con,
        original_input = tbl_name,
        success = FALSE,
        error_message = conditionMessage(e)
      )
    }

    stop(e)
  })
}

################################################################################-
# ----- Internal Functions -----

#' Extract All Table Names from a Lazy Query Object
#'
#' Rekursiv alle Tabellen-Namen aus einem `lazy_query`-Objekt extrahieren.
#'
#' @param x Das `lazy_query`-Objekt eines `tbl`.
#' @return Ein Character-Vektor mit eindeutigen Tabellennamen.
#'
#' @keywords internal
extract_all_table_names <- function(x) {
  result <- list()

  recursive_search(x)

  if (length(result) == 0) {
    return(as.character(x$x))
  } else {
    return(bind_rows(result) %>%
             unlist(use.names = FALSE) %>%
             unique() %>%
             setdiff("name"))
  }
}



