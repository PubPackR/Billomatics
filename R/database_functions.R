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
custom_decrypt_db <- function(df,
                              columns_to_decrypt,
                              key = NULL) {
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
#' @param connection your database connection
#' @param schema the schema of the table in the database
#' @param table the table in the database
#' @param data the data to be inserted
#' @return Only Feedback Message in Console
#' @export
postgres_upsert_data <- function(connection, schema, table, data, conflict_cols = "id") {
  # ----- Start -----

  # Check if the connection is valid
  if (DBI::dbIsValid(connection) == FALSE) {
    stop("Invalid database connection.")
  }

  # Temporäre Tabelle erstellen (Name mit "temp_"-Prefix)
  temp_table <- paste0("temp_", table)

  # Sicherstellen, dass die temporäre Tabelle auch bei einem Fehler gelöscht wird
  tryCatch({
    # Daten in die temporäre Tabelle schreiben
    DBI::dbWriteTable(connection, temp_table, data, temporary = TRUE, row.names = FALSE)

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

    # Query ausführen
    DBI::dbExecute(connection, query)

    # Anzahl der betroffenen Zeilen abfragen (inserted und updated)
    affected_rows <- DBI::dbGetQuery(connection, paste0("SELECT COUNT(*) FROM ", schema, ".", table))$count

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
#' Renames an existing table in a PostgreSQL database.
#'
#' @param con The database connection object.
#' @param old_name The current name of the table.
#' @param new_name The new name for the table.
#' @param schema The schema where the table is located (default is "raw").
#' @return A message indicating success or failure.
#' @export
postgres_rename_table <- function(con, old_name, new_name, schema = "raw") {
  # ----- Start -----

  # Check if the old table exists
  exists_query <- sprintf("SELECT to_regclass('%s.%s');", schema, old_name)
  exists_result <- DBI::dbGetQuery(con, exists_query)

  # If the table doesn't exist, return an error
  if (is.null(exists_result) || is.null(exists_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' does not exist.", schema, old_name))
  }

  # Check if the new table already exists
  exists_new_query <- sprintf("SELECT to_regclass('%s.%s');", schema, new_name)
  exists_new_result <- DBI::dbGetQuery(con, exists_new_query)

  # If the new table already exists, return an error
  if (!is.null(exists_new_result) && is.null(exists_new_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' already exists.", schema, new_name))
  }

  # Perform the renaming
  query <- sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema, old_name, new_name)

  # Try to execute the rename query
  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Table '%s.%s' renamed to '%s.%s'", schema, old_name, schema, new_name))
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    # Handle any errors during the renaming process
    message("Error renaming table: ", e$message)
    return(FALSE)  # Return FALSE on failure
  })
}

#' postgres_add_metadata
#'
#' Adds or updates metadata in the PostgreSQL database.
#'
#' @param con The database connection object.
#' @param key The metadata key.
#' @param value The metadata value.
#' @param is_new A logical value indicating whether the metadata should be treated as new (default is FALSE).
#'               If TRUE, an error is raised if the metadata with the given key already exists.
#' @return A feedback message in the console or an error message.
#' @export
postgres_add_metadata <- function(con, key, value, is_new = TRUE) {
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
#' Reads metadata from the PostgreSQL database.
#'
#' @param con The database connection object.
#' @param key The metadata key to retrieve.
#' @return A data frame with the metadata key and value, or NULL if the key doesn't exist.
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
#'
#' @param con The database connection object.
#' @param schema The schema where the table is located.
#' @param table The name of the table to which the column will be added.
#' @param column_name The name of the new column.
#' @param column_type The data type of the new column.
#' @return A feedback message in the console indicating success or failure.
#' @export
postgres_add_column <- function(con, schema, table, column_name, column_type) {
  # ----- Start -----

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
#'
#' @param con The database connection object.
#' @param schema The schema where the table is located.
#' @param table The name of the table containing the column.
#' @param column_name The name of the column whose data type will be changed.
#' @param new_column_type The new data type for the column.
#' @return A feedback message in the console indicating success or failure.
#' @export
postgres_change_column_type <- function(con, schema, table, column_name, new_column_type) {
  # ----- Start -----

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
#'
#' @param con The database connection object.
#' @param schema The schema where the table will be created.
#' @param table The name of the table to be created.
#' @param columns A named vector where names are column names and values are data types.
#' @return A feedback message in the console indicating success or failure.
#' @export
postgres_create_table <- function(con, schema, table, columns) {
  column_definitions <- paste(names(columns), columns, collapse = ", ")
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
#'
#' @param con The database connection object.
#' @param schema The schema where the table is located.
#' @param table The name of the table to be dropped.
#' @return A feedback message in the console indicating success or failure.
#' @export
postgres_drop_table <- function(con, schema, table) {
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

#' postgres_list_tables
#'
#' Lists all tables in a specified schema.
#'
#' @param con The database connection object.
#' @param schema The schema from which to list the tables.
#' @return A vector of table names.
#' @export
postgres_list_tables <- function(con, schema) {
  query <- sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s';", schema)
  tables <- DBI::dbGetQuery(con, query)

  if (nrow(tables) == 0) {
    message(sprintf("No tables found in schema '%s'.", schema))
    return(NULL)
  }

  return(tables$table_name)
}

#' postgres_select
#'
#' Selects data from a PostgreSQL table.
#'
#' @param con The database connection object.
#' @param schema The schema of the table.
#' @param table The name of the table.
#' @param columns A character vector of column names to select (default is "*").
#' @param where Optional SQL `WHERE` clause (without the word WHERE).
#' @param limit Optional number of rows to limit.
#' @return A data frame containing the result.
#' @export
postgres_select <- function(con, schema, table, columns = "*", where = NULL, limit = NULL) {
  # Columns part
  cols <- if (length(columns) == 1 && columns == "*") "*" else paste(columns, collapse = ", ")

  # Build query
  query <- sprintf("SELECT %s FROM %s.%s", cols, schema, table)

  if (!is.null(where)) {
    query <- paste(query, "WHERE", where)
  }

  if (!is.null(limit)) {
    query <- paste(query, "LIMIT", limit)
  }

  # Execute query
  tryCatch({
    DBI::dbGetQuery(con, query)
  }, error = function(e) {
    message("Error in postgres_select: ", e$message)
    return(NULL)
  })
}

#' postgres_connect
#'
#' Connects to a PostgreSQL database using provided credentials and gives feedback.
#'
#' @param postgres_keys A named list or object with elements: [[1]] = password, [[2]] = user, [[3]] = dbname, [[4]] = host, [[5]] = port.
#' @param ssl_cert_path Path to the SSL certificate. Default assumes common relative path.
#' @return A `DBI` connection object if successful. Stops with an error otherwise.
#' @export
postgres_connect <- function(postgres_keys, ssl_cert_path = "../../metabase-data/postgres/eu-central-1-bundle.pem") {
  tryCatch({
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
  }, error = function(e) {
    stop(sprintf("❌ Verbindung zur PostgreSQL-Datenbank fehlgeschlagen: %s", e$message))
  })
}



#' Pull production tables from a remote PostgreSQL database via SSH
#'
#' This function establishes an SSH connection to the production environment, retrieves the specified tables
#' from a PostgreSQL database, and stores them either in an in-memory SQLite database or a local PostgreSQL database.
#'
#' @param tables A character vector of fully qualified table names (e.g., "schema.table") to retrieve from the remote PostgreSQL database.If `NULL`, no tables are loaded and the DB connection is returned.
#' @param target Where to store the retrieved data: either `"memory"` for an in-memory SQLite database or `"local_postgres"` for a local PostgreSQL database. If `NULL`, the default memory is used.
#' @param ssh_key_path File path to the private SSH key for connecting to the remote server. If `NULL`, a default path is used.
#' @param local_dbname Name of the local PostgreSQL database (only used if `target = "local_postgres"` and default: studyflix_local).
#' @param local_host Hostname of the local PostgreSQL database (default: "localhost").
#' @param local_port Port of the local PostgreSQL database (default: 5432).
#' @param local_user Username for the local PostgreSQL database (default: "postgres").
#' @param local_password Password for the local PostgreSQL database. If `NULL`, the production password is reused.
#'
#' @return A database connection object:
#' - If `target = "memory"`, returns an SQLite connection object (in-memory).
#' - If `target = "local_postgres"`, returns a PostgreSQL connection object (local).
#'
#' @export
pull_production_tables <- function(tables = NULL,
                                   target = c("memory", "local_postgres"),
                                   ssh_key_path = NULL,
                                   local_dbname = "studyflix_local",
                                   local_host = "localhost",
                                   local_port = 5432,
                                   local_user = "postgres",
                                   local_password = NULL) {

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

  if (length(target) > 1) {
    target <- target[1]
  }

  # Validation of inputs
  valid_targets <- c("memory", "local_postgres")
  if (!(target %in% valid_targets)) {
    warning(sprintf("Ungültiger target-Wert: '%s'. Erlaubt sind nur: %s",
                    target, paste(valid_targets, collapse = ", ")))
    return(NULL)
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

    on.exit({
      message("🔒 SSH-Verbindung wird geschlossen...")
      try(ssh::ssh_disconnect(ssh_session), silent = TRUE)
    })

    produkt_key <- getPass::getPass(msg = "Gib das Passwort für den Produktnutzer ein:")
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
        utils::read.csv(text = data_part, stringsAsFactors = FALSE)
      }, error = function(e) {
        message(sprintf("❌ Fehler beim Parsen der Daten von Tabelle %s: %s", table, e$message))
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
  }

  if (length(tables_data) == 0 && length(tables) > 0) {
    warning("Keine Tabellen konnten geladen werden.", call. = FALSE)
    return(NULL)
  }

  # Ziel: memory
  if (target == "memory") {
    message("💾 Lade In-Memory-Datenbank...")
    sqlite_con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    for (table in names(tables_data)) {
      table_name <- gsub("\\.", ".", table)
      DBI::dbWriteTable(sqlite_con, table_name, tables_data[[table]], overwrite = TRUE)
    }
    return(sqlite_con)
  }

  # Ziel: lokale PostgreSQL
  if (target == "local_postgres") {
    message("💾 Lade lokale PostgreSQL-Datenbank...")

    if (is.null(local_password)) {
      if (!is.null(produkt_key)) {
        local_password <- produkt_key
      } else {
        local_password <- getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
      }
    }

    local_con <- tryCatch({
      DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = local_dbname,
        host = local_host,
        port = local_port,
        user = local_user,
        password = local_password
      )
    }, error = function(e) {
      message("Fehler bei der Verbindung zur lokalen PostgreSQL-Datenbank. Weitere Informationen zur lokalen PostgreSQL stehen in unserem Wiki.")
      return(NULL)
    })

    if (is.null(local_con)) {
      return(NULL)
    }

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
