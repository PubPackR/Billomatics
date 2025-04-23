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
        decrypted <- aes_cbc_decrypt(encrypted_data, key = charToRaw(key), iv = iv)
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
#' This function performs an upsert (insert or update) operation to a PostgreSQL database. It first writes the data to a temporary table,
#' and then performs an `INSERT INTO ... ON CONFLICT ... DO UPDATE` operation, which inserts new rows and updates existing rows based
#' on a conflict with specified columns.
#'
#' @param connection An active database connection object (created with DBI).
#' @param schema The schema in which the table exists in the database.
#' @param table The name of the table in the database. The schema can either be provided separately or as part of the table name
#' (in the format `schema.table`).
#' @param data A data.frame containing the data to be inserted or updated in the database.
#' @param conflict_cols A character vector specifying one or more columns that should be checked for conflicts (defaults to "id").
#' @return A numeric value indicating the number of affected rows (inserted or updated).
#' @details
#' - The function will first attempt to create a temporary table in the database using the data's column names.
#' - Then, it will attempt to perform the upsert operation using an `INSERT INTO ... ON CONFLICT ... DO UPDATE` SQL statement.
#' - In case of failure, an error message will be printed, and the function will return 0.
#' - The function also logs the time taken to perform the operation, providing insight into the performance for large datasets.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' data <- data.frame(id = c(1, 2), name = c("Alice", "Bob"))
#' postgres_upsert_data(connection, "public", "my_table", data)
#' }
#'
#' @export
postgres_upsert_data <- function(connection, schema, table, data, conflict_cols = "id") {

  # Check if the connection is valid
  if (DBI::dbIsValid(connection) == FALSE) {
    stop("Invalid database connection.")
  }

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

  # Temporäre Tabelle erstellen (Name mit "temp_"-Prefix)
  temp_table <- paste0("temp_", table)

  # Sicherstellen, dass die temporäre Tabelle auch bei einem Fehler gelöscht wird
  tryCatch({
    # Bulk-Insert anstelle von dbWriteTable
    # Temporäre Tabelle erstellen
    dbExecute(connection, paste0("CREATE TEMPORARY TABLE ", temp_table, " (",
                                 paste(names(data), collapse = ", "), ")"))

    # Daten in die temporäre Tabelle einfügen
    dbWriteTable(connection, temp_table, data, append = TRUE, row.names = FALSE)

    # Spaltennamen aus dem DataFrame extrahieren
    cols <- colnames(data)
    cols_str <- paste(cols, collapse = ", ")
    update_str <- paste(paste0(cols, " = EXCLUDED.", cols), collapse = ", ")

    # Konflikt-Spalten als Platzhalter einfügen
    conflict_cols_str <- paste(conflict_cols, collapse = ", ")

    # Upsert-Statement (INSERT mit ON CONFLICT)
    query <- sprintf(
      "INSERT INTO %s.%s (%s)\n    SELECT %s FROM %s\n    ON CONFLICT (%s) DO UPDATE \n    SET %s",
      schema, table, cols_str, cols_str, temp_table, conflict_cols_str, update_str
    )

    # Query ausführen und Zeit messen
    start_time <- Sys.time()
    dbExecute(connection, query)
    end_time <- Sys.time()

    # Query ausführen
    DBI::dbExecute(connection, query)

    # Erfolgsnachricht mit Ausführungszeit
    message(paste("Upsert erfolgreich! Gesamtanzahl der Zeilen:", affected_rows,
                  "- Dauer:", round(difftime(end_time, start_time, units = "secs"), 2), "Sekunden"))

    message(paste("Upsert erfolgreich! Gesamtanzahl der Zeilen:", affected_rows))

    # Erfolgreiche Rückgabe
    return(affected_rows = affected_rows)
  }, error = function(e) {
    # Fehlerbehandlung: Fehler ausgeben und temporäre Tabelle löschen
    message("Fehler beim Upsert: ", e$message)
    return(affected_rows = 0)
  }, finally = {
    # Temporäre Tabelle löschen (unabhängig davon, ob ein Fehler aufgetreten ist)
    DBI::dbExecute(connection, paste0("DROP TABLE IF EXISTS ", temp_table))
  })
}

#' postgres_rename_table
#'
#' Renames an existing table in a PostgreSQL database, and optionally moves it to a different schema.
#' If the old and new table names are in different schemas, the table will be moved to the new schema and renamed.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the old table is located. If not provided in `old_name`, this argument is required.
#' @param old_name The current name of the table (can be in the format `schema.table`).
#' @param new_name The new name for the table (can be in the format `schema.table`).
#' @return A logical value indicating success (`TRUE`) or failure (`FALSE`). Additionally, a message will be printed
#' indicating whether the renaming was successful or if there was an error.
#' @details
#' - If the `old_name` or `new_name` includes both schema and table (i.e., `schema.table` format), the schema is extracted automatically.
#' - The function checks if the old table exists and ensures that the new table name is not already in use.
#' - If the old and new table names are in different schemas, the function will rename and move the table across schemas.
#' - If the old and new table names are in the same schema, the function will only rename the table.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_rename_table(connection, "public", "old_table_name", "new_table_name")
#' }
#'
#' @export
postgres_rename_table <- function(con, schema, old_name, new_name) {

  # Extract schema and table if provided in old_name
  if (grepl("\\.", old_name)) {
    parts <- strsplit(old_name, "\\.")[[1]]
    if (length(parts) == 2) {
      schema_old <- parts[1]
      old_name <- parts[2]
    } else {
      stop("Invalid format for `old_name`. Use 'schema.table' or provide `schema` separately.")
    }
  } else if (!is.null(schema)) {
    schema_old <- schema
  } else {
    stop("Schema must be provided either in `old_name` or via the `schema` argument.")
  }

  # Extract schema and table if provided in new_name
  if (grepl("\\.", new_name)) {
    parts <- strsplit(new_name, "\\.")[[1]]
    if (length(parts) == 2) {
      schema_new <- parts[1]
      new_name <- parts[2]
    } else {
      stop("Invalid format for `new_name`. Use 'schema.table' or only table name.")
    }
  } else {
    schema_new <- schema_old
  }

  # Ensure the old table exists
  exists_query <- sprintf("SELECT to_regclass('%s.%s');", schema_old, old_name)
  exists_result <- DBI::dbGetQuery(con, exists_query)

  if (is.null(exists_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' does not exist.", schema_old, old_name))
  }

  # Ensure the new table name doesn't already exist
  exists_new_query <- sprintf("SELECT to_regclass('%s.%s');", schema_new, new_name)
  exists_new_result <- DBI::dbGetQuery(con, exists_new_query)

  if (!is.null(exists_new_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' already exists.", schema_new, new_name))
  }

  # Perform rename or move+rename if schemas differ
  tryCatch({
    if (schema_old == schema_new) {
      # Just rename within same schema
      query <- sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_old, old_name, new_name)
    } else {
      # Rename and move across schemas
      temp_rename <- paste0("temp_", as.integer(Sys.time()))
      queries <- c(
        sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_old, old_name, temp_rename),
        sprintf("ALTER TABLE %s.%s SET SCHEMA %s;", schema_old, temp_rename, schema_new),
        sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_new, temp_rename, new_name)
      )
      query <- paste(queries, collapse = " ")
    }

    DBI::dbExecute(con, query)
    message(sprintf("✅ Table '%s.%s' renamed to '%s.%s'", schema_old, old_name, schema_new, new_name))
    return(TRUE)
  }, error = function(e) {
    message("❌ Error renaming table: ", e$message)
    return(FALSE)
  })
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


#' postgres_add_column
#'
#' Adds a new column to an existing table in PostgreSQL.
#' This function checks if the column already exists, and if not, it adds the specified column to the table with the given type.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table to which the column will be added.
#' @param column_name The name of the new column to add.
#' @param column_type The data type of the new column (e.g., "text", "integer", "boolean").
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the specified column already exists in the given table and schema.
#' - If the column does not exist, it constructs and executes an `ALTER TABLE` query to add the new column.
#' - If the column already exists, the function raises an error and halts the operation.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_add_column(connection, "raw", "my_table", "new_column", "text")
#' }
#'
#' @export
postgres_add_column <- function(con, schema, table, column_name, column_type) {

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

  # Check if the column already exists in the table
  check_query <- sprintf("
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';
  ", schema, table, column_name)

  column_exists <- DBI::dbGetQuery(con, check_query)

  if (nrow(column_exists) > 0) {
    stop(sprintf("Error: Column '%s' already exists in table '%s.%s'.", column_name, schema, table))
  }

  # Construct the query to add the new column
  query <- sprintf("
    ALTER TABLE %s.%s ADD COLUMN %s %s;
  ", schema, table, column_name, column_type)

  # Try to execute the query
  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Column '%s' of type '%s' has been successfully added to '%s.%s'.", column_name, column_type, schema, table))
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    message("Error adding column: ", e$message)
    return(FALSE)  # Return FALSE on failure
  })
}


#' postgres_change_column_type
#'
#' Changes the data type of an existing column in a PostgreSQL table.
#' This function allows altering the data type of a specific column in an existing table.
#' If the column doesn't exist, an error is raised.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table containing the column.
#' @param column_name The name of the column whose data type will be changed.
#' @param new_column_type The new data type for the column (e.g., "text", "integer", "boolean").
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the specified column exists in the table and schema.
#' - If the column exists, it constructs and executes an `ALTER TABLE` query to change the column's data type.
#' - If the column does not exist, the function raises an error and halts the operation.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_change_column_type(connection, "raw", "my_table", "my_column", "text")
#' }
#'
#' @export
postgres_change_column_type <- function(con, schema, table, column_name, new_column_type) {

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

  # Check if the column exists in the table
  check_query <- sprintf("
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';
  ", schema, table, column_name)

  column_exists <- DBI::dbGetQuery(con, check_query)

  if (nrow(column_exists) == 0) {
    stop(sprintf("Error: Column '%s' does not exist in table '%s.%s'.", column_name, schema, table))
  }

  # Construct the query to change the column type
  query <- sprintf("
    ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s;
  ", schema, table, column_name, new_column_type)

  # Try to execute the query
  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Column '%s' in table '%s.%s' has been successfully changed to type '%s'.", column_name, schema, table, new_column_type))
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    message("Error changing column type: ", e$message)
    return(FALSE)  # Return FALSE on failure
  })
}

#' postgres_create_table
#'
#' Creates a new table in a PostgreSQL database.
#' This function allows creating a new table with specified column names and data types in a given schema.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table will be created.
#' @param table The name of the table to be created.
#' @param columns A named vector where names are column names and values are data types (e.g., c("column1" = "integer", "column2" = "text")).
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the schema is provided either via the `schema` argument or embedded in the `table` argument as 'schema.table'.
#' - The table creation SQL query is dynamically constructed from the provided column names and data types.
#' - If the table creation is successful, a success message is shown, and the function returns `TRUE`.
#' - If any error occurs during table creation, an error message is shown, and the function returns `FALSE`.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_create_table(connection, "raw", "new_table", c("id" = "integer", "name" = "text", "created_at" = "timestamp"))
#' }
#'
#' @export
postgres_create_table <- function(con, schema, table, columns) {
  column_definitions <- paste(names(columns), columns, collapse = ", ")

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

  query <- sprintf("CREATE TABLE %s.%s (%s);", schema, table, column_definitions)

  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Table '%s.%s' has been successfully created.", schema, table))
    return(TRUE)
  }, error = function(e) {
    message("Error creating table: ", e$message)
    return(FALSE)
  })
}

#' postgres_drop_table
#'
#' Drops a table from a PostgreSQL database.
#' This function allows dropping an existing table from the specified schema in the database.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table to be dropped.
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the schema is provided either via the `schema` argument or embedded in the `table` argument as 'schema.table'.
#' - The SQL query to drop the table is constructed and executed.
#' - If the table exists, it will be dropped. If it doesn't exist, no error will occur due to the `IF EXISTS` clause in the SQL statement.
#' - If the operation is successful, a success message is shown, and the function returns `TRUE`.
#' - If any error occurs during table deletion, an error message is shown, and the function returns `FALSE`.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_drop_table(connection, "raw", "old_table")
#' }
#'
#' @export
postgres_drop_table <- function(con, schema, table) {

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

  query <- sprintf("DROP TABLE IF EXISTS %s.%s;", schema, table)

  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Table '%s.%s' has been successfully dropped.", schema, table))
    return(TRUE)
  }, error = function(e) {
    message("Error dropping table: ", e$message)
    return(FALSE)
  })
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

#' Recursively Search for Table Names in a Nested List
#'
#' Durchsucht rekursiv ein List-Objekt nach `table_names` und speichert sie global.
#'
#' @param x Eine Liste oder ein verschachteltes Objekt aus `lazy_query`.
#' @return Kein Rückgabewert – füllt globalen `result`-Vektor.
#'
#' @keywords internal
recursive_search <- function(x) {
  if (is.list(x)) {
    if (!is.null(x$table_names)) {
      result[[length(result) + 1]] <<- x$table_names
    }
    lapply(x, recursive_search)
  }
}

#' postgres_connect
#'
#' Stellt eine Verbindung zu einer PostgreSQL-Datenbank her – lokal oder produktiv –, abhängig von der Umgebung (interaktiv oder nicht).
#'
#' @param local_host Hostname der lokalen Datenbank. Standard ist `"localhost"`.
#' @param local_port Port der lokalen Datenbank. Standard ist `5432`.
#' @param local_user Benutzername für die lokale Datenbank. Standard ist `"postgres"`.
#' @param local_dbname Name der lokalen Datenbank. Standard ist `"studyflix_local"`.
#' @param postgres_keys Ein benannter Vektor oder eine Liste mit Produktions-Zugangsdaten in folgender Reihenfolge:
#'   - `postgres_keys[[1]]`: Passwort
#'   - `postgres_keys[[2]]`: Benutzername
#'   - `postgres_keys[[3]]`: Name der Datenbank
#'   - `postgres_keys[[4]]`: Hostname
#'   - `postgres_keys[[5]]`: Port (als Zahl)
#' @param local_pw Optionales Passwort für die lokale Datenbankverbindung. Wird in interaktiver Umgebung abgefragt, falls nicht angegeben.
#' @param ssl_cert_path Pfad zur SSL-Zertifikatsdatei für die Verbindung zur Produktionsdatenbank. Standard ist `"../../metabase-data/postgres/eu-central-1-bundle.pem"`.
#'
#' @return Ein `DBIConnection`-Objekt, falls die Verbindung erfolgreich war. Andernfalls wird ein Fehler ausgelöst.
#'
#' @details
#' - In interaktiven Sessions (`interactive() == TRUE`) wird eine Verbindung zur lokalen PostgreSQL-Datenbank aufgebaut.
#'   - Existiert die angegebene lokale Datenbank nicht, wird sie automatisch erstellt.
#'   - Falls `local_pw` nicht angegeben ist, wird das Passwort via `getPass::getPass()` sicher abgefragt.
#' - In nicht-interaktiven Sessions (z. B. auf Servern) wird die Verbindung zur Produktionsdatenbank aufgebaut – mit SSL-Verschlüsselung und übergebener Zertifikatsdatei.
#' - Verbindungsfehler lösen einen spezifischen Fehler mit erklärender Meldung aus.
#'
#' @examples
#' \dontrun{
#' # Interaktive Verbindung zur lokalen Datenbank
#' con_local <- postgres_connect(local_pw = "dein_passwort")
#'
#' # Verbindung zur Produktionsdatenbank (nicht interaktiv)
#' keys <- list(
#'   "prod_pw",
#'   "prod_user",
#'   "prod_db",
#'   "prod_host",
#'   5432
#' )
#' con_prod <- postgres_connect(postgres_keys = keys)
#' }
#'
#' @export
postgres_connect <- function(local_host = "localhost",
                             local_port = 5432,
                             local_user = "postgres",
                             local_dbname = "studyflix_local",
                             postgres_keys = NULL,
                             local_pw = NULL,
                             ssl_cert_path = "../../metabase-data/postgres/eu-central-1-bundle.pem") {
  tryCatch({
    if (interactive()) {

      if (is.null(local_pw)) {
        message("ℹ️ Interaktiver Modus erkannt – verbinde mit lokaler PostgreSQL-Datenbank")
        local_pw <- getPass::getPass("Gib das Passwort für die lokale Datenbank (Standard: Produktnutzer) ein:")
      }

      # Step 1: Connect to the default 'postgres' database
      admin_con <- tryCatch({
        DBI::dbConnect(
          RPostgres::Postgres(),
          dbname = "postgres",  # default maintenance db
          host = local_host,
          port = local_port,
          user = local_user,
          password = local_pw
        )
      }, error = function(e) {
        message("❌ Verbindung zum lokalen PostgreSQL-Server fehlgeschlagen.")
        return(NULL)
      })

      # Step 2: Check if local_dbname exists
      db_exists <- DBI::dbGetQuery(admin_con, sprintf(
        "SELECT 1 FROM pg_database WHERE datname = '%s';", local_dbname
      ))

      # Step 3: Create the database if it doesn't exist
      if (nrow(db_exists) == 0) {
        message(sprintf("📦 Lokale Datenbank '%s' existiert noch nicht. Wird erstellt...", local_dbname))
        DBI::dbExecute(admin_con, sprintf("CREATE DATABASE \"%s\";", local_dbname))
      }

      # Step 4: Close admin connection
      DBI::dbDisconnect(admin_con)

      local_con <- DBI::dbConnect(
        drv = RPostgres::Postgres(),
        dbname = local_dbname,
        host = local_host,
        port = local_port,
        user = local_user,
        password = local_pw
      )

      rm(local_pw)

      message("✅ Erfolgreich mit der lokalen Datenbank verbunden (studyflix_local)")
      return(local_con)
    } else {
      # 🌐 Produktionsverbindung außerhalb von interactive()
      con <- DBI::dbConnect(
        drv = RPostgres::Postgres(),
        password = postgres_keys[[1]],
        user = postgres_keys[[2]],
        dbname = postgres_keys[[3]],
        host = postgres_keys[[4]],
        port = as.integer(postgres_keys[[5]]),
        sslmode = "verify-full",
        sslrootcert = ssl_cert_path
      )
      message(sprintf("✅ Erfolgreich verbunden mit PostgreSQL-DB '%s' auf Host '%s'",
                      postgres_keys[[3]], postgres_keys[[4]]))
      return(con)
    }
  }, error = function(e) {
    stop(sprintf("❌ Verbindung zur PostgreSQL-Datenbank fehlgeschlagen: %s", e$message))
  })
}

#' Aufbau einer SSH-Verbindung mit Keyfile und Passphrase
#'
#' Diese Funktion stellt eine SSH-Verbindung zu einem Remote-Host her. Dabei wird ein privater SSH-Schlüssel
#' verwendet, dessen Pfad übergeben wird. Die zugehörige Passphrase wird interaktiv abgefragt (via `getPass()`).
#'
#' @param ssh_key_path Pfad zum privaten SSH-Key (z. B. `~/.ssh/id_rsa`).
#' @param remote_user Benutzername für den Remote-Login.
#' @param remote_host Adresse des Remote-Hosts (z. B. `server.example.com`).
#'
#' @return Ein aktives SSH-Verbindungsobjekt vom Typ `ssh::ssh_session`.
#'
#' @details
#' - Die Funktion verwendet das Paket `ssh`, um eine Verbindung zum Remote-Server aufzubauen.
#' - Vor dem Verbindungsaufbau wird geprüft, ob der angegebene SSH-Key existiert.
#' - Tritt ein Fehler beim Verbindungsaufbau auf, wird dieser abgefangen und als Fehlermeldung ausgegeben.
#'
#' @examples
#' \dontrun{
#'   ssh_session <- establish_ssh_connection(
#'     ssh_key_path = "~/.ssh/id_rsa",
#'     remote_user = "mein_nutzer",
#'     remote_host = "remote.studyflix.de"
#'   )
#' }
#'
#' @seealso [ssh::ssh_connect()]
#'
#' @importFrom getPass getPass
#' @export
establish_ssh_connection <- function(ssh_key_path, remote_user, remote_host) {
  if (!file.exists(ssh_key_path)) {
    stop("Die angegebene SSH-Key-Datei existiert nicht: ", ssh_key_path)
  }

  message("🔐 Aufbau SSH-Verbindung...")
  ssh_session <- tryCatch({
    ssh::ssh_connect(
      host = paste0(remote_user, "@", remote_host),
      keyfile = ssh_key_path,
      passwd = getPass("Gib dein Passphrase für den SSH Key ein:")
    )
  }, error = function(e) {
    stop("Fehler beim Aufbau der SSH-Verbindung: ", e$message)
  })

  return(ssh_session)
}

#' Pull production tables from a remote PostgreSQL database via SSH
#'
#' This internal function establishes an SSH connection to the production environment, retrieves the specified tables
#' from a PostgreSQL database, and stores them either in an in-memory SQLite database or in a local PostgreSQL instance.
#'
#' @param tables A character vector of fully qualified table names (e.g., "schema.table") to retrieve from the remote database.
#'               If `NULL`, no tables are loaded and only the database connection is returned.
#' @param load_in_memory Logical. If `TRUE`, the retrieved tables are stored in an in-memory SQLite database and returned as such.
#'                       If `FALSE` (default), the data is written to a local PostgreSQL database.
#' @param ssh_key_path File path to the private SSH key for connecting to the remote server. If `NULL`, a default path is used.
#' @param local_dbname Name of the local PostgreSQL database (used if `load_in_memory = FALSE`, default: "studyflix_local").
#' @param local_host Hostname of the local PostgreSQL database (default: "localhost").
#' @param local_port Port of the local PostgreSQL database (default: 5432).
#' @param local_user Username for the local PostgreSQL database (default: "postgres").
#' @param local_password Password for the local PostgreSQL database. If `NULL`, the production password is reused.
#' @param local_password_is_product Logical. If `TRUE`, assumes `local_password` is already the decrypted production key.
#'
#' @return A database connection object:
#' \itemize{
#'   \item If `load_in_memory = TRUE`, returns an in-memory SQLite connection.
#'   \item If `load_in_memory = FALSE`, returns a connection to the local PostgreSQL database.
#' }
#'
#' @keywords internal
postgres_pull_production_tables <- function(tables = NULL,
                                            ssh_key_path = NULL,
                                            local_dbname = "studyflix_local",
                                            local_host = "localhost",
                                            local_port = 5432,
                                            local_user = "postgres",
                                            local_password = NULL,
                                            load_in_memory = FALSE,
                                            local_password_is_product = FALSE) {

  if (!interactive()) {
    warning("Die Funktion 'pull_production_tables' wird nur in interaktiven Sitzungen ausgeführt.")
    return(NA)
  }

  required_packages <- c("DBI", "RPostgres", "RSQLite", "ssh", "getPass", "shinymanager", "utils")

  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
    library(pkg, character.only = TRUE)
  }

  produkt_key <- NULL
  tables_data <- list()
  failed_tables <- character()

  if (length(tables) > 0) {

    if (is.null(ssh_key_path)) {
      user <- Sys.getenv("USER")
      if (user == "") user <- Sys.getenv("USERNAME")
      ssh_key_path <- file.path("C:/Users", user, ".ssh", "id_rsa")
    }

    if (!file.exists(ssh_key_path)) {
      warning(sprintf("Die angegebene SSH-Key-Datei existiert nicht: %s", ssh_key_path))
      return(NULL)
    }

    # SSH-Verbindungsdaten
    remote_user <- "application-user"
    remote_host <- "shiny.studyflix.info"

    # Verwende die Funktion zur SSH-Verbindung
    ssh_session <- establish_ssh_connection(ssh_key_path, remote_user, remote_host)

    on.exit({
      message("🔒 SSH-Verbindung wird geschlossen...")
      try(ssh::ssh_disconnect(ssh_session), silent = TRUE)
    })

    if(local_password_is_product) {
      produkt_key <- local_password
    } else {
      produkt_key <- getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
    }

    decrypt_key <- shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key)

    # get the postgres_keys
    cmd <- sprintf("
      R_TEMP_SCRIPT=$(mktemp)
      cat > \"${R_TEMP_SCRIPT}\" << 'EORSCRIPT'
      library(safer)
      key <- \"%s\"
      cred <- safer::decrypt_string(readLines(\"keys/PostgreSQL_DB/postgresql_key.txt\"), key)
      srv <- strsplit(safer::decrypt_string(readLines(\"keys/PostgreSQL_DB/postgresql_server.txt\"), key), \", \")[[1]]
      cat(paste(c(cred, srv), collapse=\",\"))
EORSCRIPT
      Rscript --vanilla \"${R_TEMP_SCRIPT}\"
      rm \"${R_TEMP_SCRIPT}\"
    ", gsub("\"", "\\\"", decrypt_key))

    result <- ssh::ssh_exec_internal(ssh_session, cmd)
    postgres_keys <- strsplit(rawToChar(result$stdout), ",")[[1]]

    # Tabellen herunterladen
    for (table in tables) {
      message(paste("📥 Versuche Tabelle zu laden:", table))

      cmd <- sprintf(
        'PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -c \"\\\\copy (SELECT * FROM %s) TO STDOUT WITH CSV HEADER\" 2>&1; echo \"EXIT_STATUS:$?\"',
        postgres_keys[1], postgres_keys[3], postgres_keys[2],
        postgres_keys[4], postgres_keys[5], table
      )

      result <- ssh::ssh_exec_internal(ssh_session, cmd)
      output <- rawToChar(result$stdout)
      parts <- strsplit(output, "EXIT_STATUS:")[[1]]
      data_part <- parts[1]
      status <- as.integer(parts[2])

      if (status != 0 || (grepl("error", data_part, ignore.case = TRUE) & grepl("^ERROR:", output, ignore.case = TRUE))) {
        error_msg <- if (grepl("error", data_part, ignore.case = TRUE)) data_part else "Unbekannter Fehler (keine Fehlermeldung erkannt)"
        if (grepl("does not exist|existiert nicht|relation.*not found", error_msg, ignore.case = TRUE)) {
          message(sprintf("❌ Tabelle %s existiert nicht", table))
        } else if (grepl("permission denied|Zugriff verweigert", error_msg, ignore.case = TRUE)) {
          message(sprintf("❌ Keine Berechtigung für Tabelle %s", table))
        } else {
          message(sprintf("❌ Fehler bei Tabelle %s: %s", table, error_msg))
        }
        failed_tables <- c(failed_tables, table)
        next
      }

      df <- tryCatch({
        tmp_df <- utils::read.csv(text = data_part, stringsAsFactors = FALSE)

        # Convert "t"/"f" columns to logical if appropriate
        tmp_df <- lapply(tmp_df, function(col) {
          if (all(na.omit(col) %in% c("t", "f", ""))) {
            return(col == "t")
          }
          return(col)
        })

        df <- as.data.frame(tmp_df, stringsAsFactors = FALSE)
        df
      }, error = function(e) {
        message(sprintf(
          "❌ Fehler beim Parsen der Daten von Tabelle %s: %s",
          table,
          e$message
        ))
        failed_tables <- c(failed_tables, table)
        return(NULL)
      })

      if (!is.null(df)) {
        tables_data[[table]] <- df
        message(sprintf("✅ Tabelle %s erfolgreich geladen (%d Zeilen)", table, nrow(df)))
      }
    }

    if (length(failed_tables) > 0) {
      warning(sprintf(
        "Konnte %d von %d Tabellen nicht laden: %s",
        length(failed_tables), length(tables),
        paste(failed_tables, collapse = ", ")
      ), call. = FALSE)
    }
  } else {
    message("ℹ️ Keine Tabellen angegeben. Es wird nur die DB-Verbindung zurückgegeben.")
  }

  if (length(tables_data) == 0 && length(tables) > 0) {
    warning("Keine Tabellen konnten geladen werden.", call. = FALSE)
    return(NULL)
  }

  # Ziel: memory
  if (load_in_memory) {
    message("💾 Lade In-Memory-Datenbank...")
    sqlite_con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    for (table in names(tables_data)) {
      table_name <- gsub("\\.", ".", table)
      DBI::dbWriteTable(sqlite_con, table_name, tables_data[[table]], overwrite = TRUE)
    }
    return(sqlite_con)
  } else {
    # Ziel: lokale PostgreSQL
    message("💾 Lade lokale PostgreSQL-Datenbank...")

    if(is.null(local_password)) {
      if (!is.null(produkt_key)) {
        local_password <- produkt_key
      } else {
        local_password <- getPass::getPass("Gib das Passwort für die lokale Datenbank (Standard: Produktnutzer) ein:")
      }
    }

    local_con <- postgres_connect(
      local_host = local_host,
      local_port = local_port,
      local_user = local_user,
      local_dbname = local_dbname,
      local_pw = local_password
    )

    # Step 6: Write tables
    for (table in names(tables_data)) {
      split <- strsplit(table, "\\.")[[1]]
      schema <- split[1]
      table_name <- split[2]

      DBI::dbExecute(local_con, sprintf('CREATE SCHEMA IF NOT EXISTS "%s";', schema))
      message(sprintf("📤 Schreibe Tabelle %s nach PostgreSQL (%s.%s)", table, schema, table_name))

      DBI::dbWriteTable(
        conn = local_con,
        name = DBI::Id(schema = schema, table = table_name),
        value = tables_data[[table]],
        overwrite = TRUE
      )

    }

    return(local_con)
  }

  return(NULL)
}

#' Ermittelt Tabellen, die aus der Produktion gezogen werden müssen
#'
#' Diese Hilfsfunktion prüft basierend auf dem Parameter `update_available_tables`,
#' ob alle oder nur fehlende Tabellen aus der Produktionsdatenbank geholt werden sollen.
#' Dazu wird die aktuelle Verbindung zur lokalen Datenbank genutzt, um vorhandene Tabellen zu erkennen.
#'
#' @param tables Ein Character-Vektor mit vollqualifizierten Tabellennamen im Format `"schema.tabelle"`.
#' @param con Ein PostgreSQL-Verbindungsobjekt zur lokalen Datenbank.
#' @param update_available_tables Logisch. Wenn `TRUE`, werden alle Tabellen zurückgegeben (vollständiger Refresh).
#'
#' @return Character-Vektor mit den Tabellennamen, die noch aus der Produktion gezogen werden müssen.
#'
#' @keywords internal
postgres_get_tables_to_pull <- function(tables, con, update_available_tables) {
  if (update_available_tables) {
    return(tables)
  }

  # Tabellen in der lokalen DB abfragen
  existing_tables <- DBI::dbGetQuery(
    con,
    "
    SELECT table_schema || '.' || table_name AS full_table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
    "
  )$full_table_name

  missing_tables <- tables[!tables %in% existing_tables]
  return(missing_tables)
}

#' Verbindung zur lokalen PostgreSQL-Datenbank mit optionaler Tabellensynchronisation
#'
#' Diese Funktion stellt eine Verbindung zu einer lokalen PostgreSQL-Datenbank her. Optional können angegebene
#' Tabellen aus der Produktionsumgebung geladen werden – entweder vollständig oder nur, wenn sie lokal noch nicht vorhanden sind.
#'
#' Die Funktion kann sowohl interaktiv (z. B. in RStudio) als auch im Server-Kontext verwendet werden.
#'
#' @param tables Ein Character-Vektor mit vollqualifizierten Tabellennamen im Format `"schema.tabelle"`, z. B.
#'        `c("raw.crm_leads", "analytics.dashboard_metrics")`. Wenn `NULL`, findet keine Synchronisation statt.
#' @param con Ein bestehendes PostgreSQL-Verbindungsobjekt. Wenn `NULL`, wird eine neue Verbindung aufgebaut.
#' @param keys_postgres Eine Liste mit Zugangsdaten zur Produktionsdatenbank (benötigt, wenn `con = NULL`).
#'        Erforderliche Felder: `password`, `user`, `dbname`, `host`, `port`.
#' @param update_available_tables Logisch. Wenn `TRUE`, werden alle angegebenen Tabellen aus der Produktion
#'        geladen. Wenn `FALSE` (Standard), werden nur Tabellen geladen, die lokal fehlen.
#' @param ssh_key_path Pfad zum SSH-Schlüssel für den Zugriff auf die Produktionsdatenbank.
#' @param local_dbname, local_host, local_port, local_user, local_pw Parameter zur Konfiguration der lokalen Datenbankverbindung.
#' @param load_in_memory Logisch. Wird aktuell nicht genutzt und ist standardmäßig `FALSE`.
#'
#' @return Gibt ein Verbindungsobjekt zur lokalen Datenbank zurück, sofern eine neue Verbindung aufgebaut wurde.
#'         Gibt `NULL` zurück, wenn keine Tabellen angegeben sind und keine neue Verbindung erforderlich ist.
#'
#' @details
#' - Im interaktiven Modus wird bei fehlender Verbindung das Passwort für den lokalen Datenbanknutzer abgefragt.
#' - Im Server-Modus wird nur eine Verbindung aufgebaut, wenn keine übergeben wurde.
#' - Wenn Tabellen angegeben sind, wird geprüft, ob sie lokal bereits existieren (außer bei `update_available_tables = TRUE`).
#' - Fehlende oder zu aktualisierende Tabellen werden automatisch aus der Produktionsumgebung übernommen.
#'
#' @examples
#' \dontrun{
#'   # Verbindung herstellen und nur fehlende Tabellen laden
#'   con <- postgres_connect_and_update_local(
#'     tables = c("raw.crm_leads", "analytics.dashboard_metrics"),
#'     update_available_tables = FALSE
#'   )
#' }
#
#' @export
postgres_connect_and_update_local <- function(tables = NULL,
                                              con = NULL,
                                              keys_postgres = NULL,
                                              update_available_tables = FALSE,
                                              ssh_key_path = NULL,
                                              local_dbname = "studyflix_local",
                                              local_host = "localhost",
                                              local_port = 5432,
                                              local_user = "postgres",
                                              local_pw = NULL,
                                              load_in_memory = FALSE) {

    is_connection_available <- FALSE

    if (interactive()) {
      message("ℹ️ Interaktiver Modus erkannt – verbinde mit lokaler PostgreSQL-Datenbank")
      local_password_is_product <- FALSE
      if (is.null(con)) {
        if (is.null(local_pw)) {
          local_pw <- getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
          local_password_is_product <- TRUE
        }
        if (is.null(local_pw)) {
          stop("Bitte entweder eine bestehende Connection übergeben oder das Passwort für die lokale DB angeben.")
        }
        is_connection_available <- FALSE
        con <- postgres_connect(postgres_keys = keys_postgres,
                                local_pw = local_pw,
                                local_host = local_host,
                                local_port = local_port,
                                local_user = local_user,
                                local_dbname = local_dbname)
      } else {
        is_connection_available <- TRUE
      }
    } else {
      if (is.null(con)) {
        if (is.null(keys_postgres)) {
          stop("Bitte entweder eine bestehende Connection übergeben oder die Keys für eine neue Postgres-Verbindung.")
        }
        message("ℹ️ Server-Modus erkannt - Gebe nur Connection zurück")
        con <- postgres_connect(postgres_keys = keys_postgres)
        return(con)
      }
      message("ℹ️ Server-Modus erkannt und bestehende Connection übergeben. Gebe nichts zurück.")
      return(NULL)
    }

  if (is.null(tables)) {
    warning("Keine Tabellen angegeben. Es werden keine Daten geladen.")
    return(NULL)
  }

  # Produktionsdaten bei Bedarf synchronisieren
  if (interactive()) {
    tables_to_pull <- postgres_get_tables_to_pull(tables, con, update_available_tables)
    if (length(tables_to_pull) > 0) {
      message("⬇️ Ziehe Tabellen aus Produktion: ", paste(tables_to_pull, collapse = ", "))
      postgres_pull_production_tables(
        tables = tables_to_pull,
        ssh_key_path = ssh_key_path,
        local_dbname = local_dbname,
        local_host = local_host,
        local_port = local_port,
        local_user = local_user,
        local_password = local_pw,
        load_in_memory = FALSE,
        local_password_is_product = local_password_is_product
      )
    } else {
      message("✅ Alle Tabellen bereits lokal vorhanden. Kein Download nötig.")
    }
  }
  return(con)
}
