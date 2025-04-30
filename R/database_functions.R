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
#' Diese Funktion führt ein UPSERT (Insert or Update) in eine PostgreSQL-Datenbank durch.
#' Optional können nicht mehr vorhandene Zeilen gelöscht werden, und automatisch generierte
#' IDs (z.B. serial) werden bei Bedarf zurückgegeben.
#'
#' @param con Eine aktive DB-Verbindung (z.B. mit `DBI::dbConnect()` erstellt).
#' @param schema Schema in der Datenbank (z.B. "raw").
#' @param table Tabellenname (z.B. "my_table" oder "raw.my_table").
#' @param data Ein `data.frame`, das eingefügt oder aktualisiert werden soll.
#' @param match_cols Zeichenvektor mit Spaltennamen, die als Konfliktschlüssel dienen.
#' @param delete_missing Logical. Wenn `TRUE`, werden alle nicht mehr im DataFrame enthaltenen Einträge gelöscht (basierend auf Konfliktspalten).
#' @param returning_cols Optionaler Zeichenvektor mit Spalten, die nach dem Upsert zurückgegeben werden sollen.
#'
#' @return Ein `data.frame` mit den zurückgegebenen Spalten (falls `returning_cols` gesetzt wurde), sonst `invisible(NULL)`.
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

  # Nur Spalten beibehalten, die auch in der Zieltabelle existieren
  colnames_data <- colnames(data)
  table_columns <- DBI::dbListFields(con, DBI::Id(schema = schema, table = table))
  missing_cols <- setdiff(colnames_data, table_columns)
  if (length(missing_cols) > 0) {
    warning("Folgende Spalten sind nicht in der Zieltabelle vorhanden und werden ignoriert: ",
            paste(missing_cols, collapse = ", "))
  }
  data <- data[, intersect(colnames_data, table_columns), drop = FALSE]
  colnames_data <- setdiff(colnames(data), c("updated_at"))  # Aktualisieren, da sich data geändert hat

  # Spalten, die geupdatet werden sollen (ohne created_at und match_cols)
  update_cols <- setdiff(colnames_data, c("created_at", "updated_at", match_cols))

  # Wenn keine 'match_cols' angegeben sind, prüfen, ob es eine typische ID-Spalte gibt
  if (is.null(match_cols)) {
    match_cols <- intersect(colnames_data, c("id"))
    if (length(match_cols) == 0) stop("Keine 'match_cols' angegeben und keine typische ID-Spalte gefunden.")
  }

  # Temporäre Tabelle erstellen, um die Daten zu übertragen
  temp_table <- paste0("temp_upsert_", as.integer(Sys.time()))
  DBI::dbWriteTable(con, temp_table, data, temporary = TRUE)

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
    warning("Keine Spalten zum Aktualisieren vorhanden. Es wird 'DO NOTHING' verwendet.")
    "DO NOTHING"
  }

  # Rückgabeklausel erstellen, falls angegeben
  returning_clause <- if (!is.null(returning_cols)) {
    paste("RETURNING", paste(returning_cols, collapse = ", "))
  } else {
    ""
  }

  # Die endgültige SQL-Abfrage zusammenstellen
  query <- glue::glue("
    INSERT INTO {full_table} ({paste(colnames_data, collapse = ', ')})
    SELECT {paste(colnames_data, collapse = ', ')}
    FROM {DBI::dbQuoteIdentifier(con, temp_table)}
    ON CONFLICT ({paste(match_cols, collapse = ', ')})
    {conflict_action}
    {returning_clause};
  ")

  # Ausführen der Abfrage
  tryCatch({
    if (nzchar(returning_clause)) {
      res <- DBI::dbSendQuery(con, query)
      result <- DBI::dbFetch(res)
      DBI::dbClearResult(res)
    } else {
      DBI::dbExecute(con, query)
    }
  }, error = function(e) {
    if (grepl("no unique or exclusion constraint matching the ON CONFLICT specification",
              e$message)) {
      stop(
        "Fehler beim Upsert: Für die angegebenen 'match_cols' existiert kein UNIQUE- oder PRIMARY KEY-Constraint.\n",
        "Lösung: Lege einen eindeutigen Index auf diese Spalten an, z.B. mit:\n\n",
        glue::glue(
          "  CREATE UNIQUE INDEX ON {schema}.{table}({paste(match_cols, collapse = ', ')});\n"
        )
      )
    } else {
      stop("Fehler beim Upsert: ", e$message)
    }
  })

  # Temporäre Tabelle entfernen
  DBI::dbRemoveTable(con, temp_table)

  # Optional: Löschen von nicht mehr vorhandenen Einträgen
  if (delete_missing) {
    quoted_match_cols <- paste(match_cols, collapse = ", ")
    existing_keys <- unique(data[match_cols])
    quoted_vals <- apply(existing_keys, 1, function(row) {
      paste0("(", paste(DBI::dbQuoteLiteral(con, row), collapse = ", "), ")")
    })
    key_string <- paste(quoted_vals, collapse = ", ")

    delete_query <- glue::glue("
      DELETE FROM {full_table}
      WHERE ({quoted_match_cols}) NOT IN ({key_string});
    ")
    DBI::dbExecute(con, delete_query)
  }

  # Das Ergebnis (die zurückgegebenen Spalten) zurückgeben
  return(result)
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
                                              load_in_memory = FALSE,
                                              local_password_is_product = FALSE) {

    if (interactive()) {
      message("ℹ️ Interaktiver Modus erkannt – verbinde mit lokaler PostgreSQL-Datenbank")
      if (is.null(con)) {
        if (is.null(local_pw)) {
          local_pw <- getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
          local_password_is_product <- TRUE # Wenn kein Passwort initial übergeben wird, dann ist es der Produktnutzer
        }
        if (is.null(local_pw)) {
          stop("Bitte entweder eine bestehende Connection übergeben oder das Passwort für die lokale DB angeben.")
        }
        con <- postgres_connect(postgres_keys = keys_postgres,
                                local_pw = local_pw,
                                local_host = local_host,
                                local_port = local_port,
                                local_user = local_user,
                                local_dbname = local_dbname)
      }

    } else {
      message("ℹ️ Server-Modus erkannt - Gebe nur Connection zurück")
      if (is.null(con)) {
        if (is.null(keys_postgres)) {
          stop("Bitte entweder eine bestehende Connection übergeben oder die Keys für eine neue Postgres-Verbindung.")
        }
        con <- postgres_connect(postgres_keys = keys_postgres)
        return(con)
      }
      message("ℹ️ Server-Modus erkannt und bestehende Connection übergeben. Gebe diese zurück.")
      return(con)
    }

  # Wenn keine Tabellen angegeben sind, gebe nur die Verbindung zurück
  if (is.null(tables)) {
    warning("Keine Tabellen angegeben. Es werden keine Daten geladen.")
    return(con)
  }

  # Produktionsdaten bei Bedarf synchronisieren
  if (interactive()) {
    # Bestimme zu downloadende Tabellen
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
#' @keywords internal
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
    suppressPackageStartupMessages(library(pkg, character.only = TRUE))
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

      metadata <- get_table_metadata(table, ssh_session, postgres_keys)

      df <- tryCatch({
        tmp_df <- utils::read.csv(text = data_part, stringsAsFactors = FALSE)
        tmp_df <- apply_column_types(tmp_df, metadata$data_types)

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
        tables_data[[table]] <- list(
          data = df,
          metadata = metadata
        )
        message(sprintf("✅ Tabelle %s erfolgreich geladen (%d Zeilen)", table, nrow(df)))
      }
    }

    functions <- get_all_functions(ssh_session, postgres_keys)

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
      DBI::dbWriteTable(conn = sqlite_con, name = table_name, value = tables_data[[table]], overwrite = TRUE)
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

    load_functions_to_new_db(
      con = local_con,
      function_string = functions
    )

    # Step 6: Write tables
    for (table in tables) {
      split <- strsplit(table, "\\.")[[1]]
      schema <- split[1]
      table_name <- split[2]

      DBI::dbExecute(local_con, "SET client_min_messages TO WARNING;")
      DBI::dbExecute(local_con, sprintf('CREATE SCHEMA IF NOT EXISTS "%s";', schema))
      DBI::dbExecute(local_con, "SET client_min_messages TO NOTICE;")

      message(sprintf("📤 Schreibe Tabelle %s nach PostgreSQL (%s.%s)", table, schema, table_name))

      tables_names <- tables
      index_available_tables <- which(tables_names == table)[1]
      available_tables <- tables_names[1:(index_available_tables - 1)]

      write_table_with_metadata(con = local_con, schema = schema, table_name = table_name, table_data_with_meta = tables_data[[table]], available_tables = available_tables)

    }
    return(local_con)
  }

  return(NULL)
}

write_table_with_metadata <- function(con, schema, table_name, table_data_with_meta, available_tables) {

  sql <- DBI::SQL  # macht die Funktion sql() verfügbar für glue_sql()

  drop_table_query <- sprintf("
    DROP TABLE IF EXISTS %s.%s CASCADE;
  ", schema, table_name)
  dbExecute(con, drop_table_query)

  # Write data first
  DBI::dbWriteTable(
    conn = con,
    name = DBI::Id(schema = schema, table = table_name),
    value = table_data_with_meta$data,
    overwrite = TRUE
  )

  # Collect metadata
  meta <- table_data_with_meta$metadata

  # Build full table ID
  tbl_id <- DBI::Id(schema = schema, table = table_name)

  schema_quoted <- DBI::dbQuoteIdentifier(con, schema)
  table_name_quoted <- DBI::dbQuoteIdentifier(con, table_name)

  # PRIMARY KEY
  if (!is.null(meta$primary_keys) && nrow(meta$primary_keys) > 0) {
    # Tabelle und Spalten korrekt zitieren
    columns <- meta$primary_keys$column
    columns <- columns[-1]  # Entfernen des ersten Elements, falls nötig
    for (col in columns) {
      # Spaltennamen mit dbQuoteIdentifier zitieren
      pk_cols <- DBI::dbQuoteIdentifier(con, col)

      # Erstellen der SQL-Abfrage mit glue_sql
      query <- glue::glue_sql(
        "ALTER TABLE {schema_quoted}.{table_name_quoted} ADD PRIMARY KEY ({pk_cols})",
        .con = con
      )

      # Ausführen der Abfrage
      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
        cat(sprintf("Fehler beim Erstellen eines Primary Keys: %s\n", e$message))
      })
    }
  }


  if (!is.null(meta$unique_constraints) && nrow(meta$unique_constraints) > 0) {
    uc <- meta$unique_constraints

    # Gruppiere nach constraint_name
    uc_grouped <- split(uc, uc$constraint_name)

    for (constraint_name in names(uc_grouped)) {
      group <- uc_grouped[[constraint_name]]

      # Zitiere Spaltennamen
      quoted_cols <- paste(DBI::dbQuoteIdentifier(con, group$column_name), collapse = ", ")

      # Zitiere Constraint-Name
      quoted_constraint_name <- DBI::dbQuoteIdentifier(con, constraint_name)

      # Zitiere Schema und Tabelle
      schema_quoted <- DBI::dbQuoteIdentifier(con, schema)
      table_name_quoted <- DBI::dbQuoteIdentifier(con, table_name)

      # Baue SQL mit glue_sql
      query <- glue::glue_sql(
        "ALTER TABLE {schema_quoted}.{table_name_quoted}
         ADD CONSTRAINT {quoted_constraint_name}
         UNIQUE ({sql(quoted_cols)})",
        .con = con
      )

      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
        cat(sprintf("Fehler beim Erstellen eines Unique Constraints: %s\n", e$message))
      })
    }
  }

  # FOREIGN KEYS
  if (!is.null(meta$foreign_keys) && nrow(meta$foreign_keys) > 0) {
    for (i in seq_len(nrow(meta$foreign_keys))) {
      fk <- meta$foreign_keys[i, ]
      ref_table <- fk$referenced_table
      ref_schema <- fk$referenced_schema
      # Only apply FK if referenced table is available
      if (paste0(ref_schema, ".", ref_table) %in% available_tables) {
        query <- glue::glue_sql(
          "ALTER TABLE {schema_quoted}.{table_name_quoted}
           ADD CONSTRAINT fk_{sql(table_name)}_{sql(fk$constraint_column)}
           FOREIGN KEY ({`fk$constraint_column`*}) REFERENCES {`ref_schema`}.{`ref_table`} ({`fk$referenced_column`*})",
          .con = con
        )
        DBI::dbExecute(con, query)
      } else {
        warning(glue::glue("Skipping FK on {table_name}.{fk$constraint_column} → {ref_table}.{fk$referenced_column}, because {`ref_schema`}.{`ref_table`} was not downloaded before."))
      }
    }
  }

  # NOT NULL
  if (!is.null(meta$not_null_columns) && nrow(meta$not_null_columns) > 0) {
    columns <- meta$not_null_columns$column
    columns <- columns[-1]  # Entfernen des ersten Elements, falls nötig
    for (col in columns) {
      # Spaltennamen mit dbQuoteIdentifier zitieren
      not_null_cols <- DBI::dbQuoteIdentifier(con, as.character(col))

      # Erstellen der SQL-Abfrage mit glue_sql
      query <- glue::glue_sql(
        "ALTER TABLE {schema_quoted}.{table_name_quoted} ALTER COLUMN {not_null_cols} SET NOT NULL",
        .con = con
      )

      # Ausführen der Abfrage
      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
        cat(sprintf("Fehler beim Erstellen eines Not Null Constrains: %s\n", e$message))
      })
    }
  }

  # DEFAULT VALUES
  if (!is.null(meta$default_values) && nrow(meta$default_values) > 0) {
    for (i in seq_len(nrow(meta$default_values))) {
      col <- meta$default_values$column_name[i]
      default <- meta$default_values$column_default[i]

      # Falls notwendig, Hochkommas um Sequenznamen setzen
      if (grepl("^nextval\\(", default)) {
        sequence_name <- sub("^nextval\\((.*)::regclass\\)$", "\\1", default)
        sequence_name <- gsub("'", "", sequence_name)
        column_name <- gsub(paste0(schema, ".", table_name, "_"), "", sequence_name)
        column_name <- gsub("_seq", "", column_name)

        # Abfrage, um den Maximalwert der Spalte zu finden
        max_value_query <- sprintf("
          SELECT MAX(%s) AS max_value
          FROM %s.%s;
        ", column_name, schema, table_name)
        max_value_result <- dbGetQuery(con, max_value_query)
        max_value <- dplyr::coalesce(max_value_result$max_value, as.integer64(1))

        sequence_creation_query <- sprintf("
          CREATE SEQUENCE IF NOT EXISTS %s
          OWNED BY %s.%s.%s;
        ", sequence_name, schema, table_name, column_name)

        tryCatch({
          DBI::dbExecute(con, sequence_creation_query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen einer Sequenz: %s\n", e$message))
        })

        # Setze den Sequenzwert auf den Maximalwert der Spalte
        set_sequence_query <- sprintf("
          SELECT setval('%s', %s, TRUE);
        ", sequence_name, max_value)

        tryCatch({
          DBI::dbExecute(con, set_sequence_query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Definieren einer Sequenz: %s\n", e$message))
        })

      }
        # Spalten- und Tabellennamen quoten
        col_quoted <- DBI::dbQuoteIdentifier(con, col)

        #sub default string-part starting with first : with ")"
        default <- gsub(":[^)]+", "", default)

        # Default-Wert roh in SQL einfügen (kein quoting!)
        query <- glue::glue_sql(
          "ALTER TABLE {schema_quoted}.{table_name_quoted} ALTER COLUMN {col_quoted} SET DEFAULT {sql(default)}",
          .con = con
        )

        tryCatch({
          DBI::dbExecute(con, query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen eines  DEFAULT Values: %s\n", e$message))
        })
    }
  }

  # INDEXES
  if (!is.null(meta$indexes) && nrow(meta$indexes) > 0) {
    for (i in seq_len(nrow(meta$indexes))) {
      index_sql <- meta$indexes$indexdef[i]

      # Ausführen der CREATE INDEX-Anweisung
      if (!grepl("UNIQUE", index_sql)) {
        tryCatch({
          DBI::dbExecute(con, query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen eines UNIQUE Values: %s\n", e$message))
        })
      }
    }
  }

  # TABLE COMMENT
  if (!is.null(meta$table_comment) && nrow(meta$table_comment) > 1) {
    comment <- meta$table_comment[[1]][2]

    query <- glue::glue_sql(
      "COMMENT ON TABLE {schema_quoted}.{table_name_quoted} IS {comment};",
      .con = con
    )
    tryCatch({
      DBI::dbExecute(con, query)
    }, error = function(e) {
      # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
      cat(sprintf(
        "Fehler beim Erstellen eines Table Comments: %s\n",
        e$message
      ))
    })
  }

  # COLUMN COMMENTS
  if (!is.null(meta$column_comments) && nrow(meta$column_comments) > 0) {
    for (i in seq_len(nrow(meta$column_comments))) {
      column <- DBI::dbQuoteIdentifier(con, meta$column_comments$column_name[i])
      comment <- meta$column_comments$column_comment[i]

      # Kommentar kann NULL sein
      if (!is.na(comment)) {

        query <- glue::glue_sql(
          "COMMENT ON COLUMN {schema_quoted}.{table_name_quoted}.{column} IS {comment};",
          .con = con
        )

        tryCatch({
          DBI::dbExecute(con, query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen eines Column Comments: %s\n", e$message))
        })
      }
    }
  }

  # CHECK CONSTRAINTS
  if (!is.null(meta$check_constraints) && nrow(meta$check_constraints) > 0) {
    for (i in seq_len(nrow(meta$check_constraints))) {
      constraint_name <- DBI::dbQuoteIdentifier(con, meta$check_constraints$constraint_name[i])
      condition <- meta$check_constraints$check_condition[i]

      query <- glue::glue_sql(
        "ALTER TABLE {schema_quoted}.{table_name_quoted} ADD CONSTRAINT {`constraint_name`} CHECK ({sql(condition)});",
        .con = con
      )

      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
        cat(sprintf("Fehler beim Erstellen eines CHECK Constraints: %s\n", e$message))
      })
    }
  }

  # TRIGGERS
  if (!is.null(meta$triggers) && nrow(meta$triggers) > 0) {
    for (i in seq_len(nrow(meta$triggers))) {
      trigger_name <- DBI::dbQuoteIdentifier(con, meta$triggers$trigger_name[i])
      timing <- meta$triggers$timing[i]  # BEFORE, AFTER, etc.
      event <- meta$triggers$event[i]    # INSERT, UPDATE, DELETE
      definition <- meta$triggers$definition[i]  # ganze Prozedur z. B. "EXECUTE FUNCTION ..."

      # Zusammensetzen der SQL-Abfrage
      query <- glue::glue_sql(
        "CREATE TRIGGER {sql(meta$triggers$trigger_name[i])} {sql(timing)} {sql(event)} ON {schema_quoted}.{table_name_quoted}
         FOR EACH ROW {SQL(definition)};",
        .con = con
      )

      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
        cat(sprintf("Fehler beim Erstellen eines Triggers: %s\n", e$message))
      })
    }
  }
  return(NULL)
}

categorize_sql_string <- function(input_string) {
  # Define known SQL functions and keywords
  sql_functions <- c("CURRENT_TIMESTAMP", "NOW()", "COUNT", "SUM", "AVG", "MAX", "MIN")

  # Check if the string is a known SQL function or keyword
  if (input_string %in% sql_functions) {
    return(list(type = "SQL Function/Keyword", needs_quotes = FALSE))
  }

  # Check if the string is a numeric value
  if (grepl("\\(", input_string)) {
    return(list(type = "Function", needs_quotes = FALSE))
  }

  # Check if the string is a numeric value
  if (grepl("^\\d+(\\.\\d+)?$", input_string)) {
    return(list(type = "Numeric Value", needs_quotes = FALSE))
  }

  # Check if the string represents a date/time value
  if (grepl("^\\d{4}-\\d{2}-\\d{2}$", input_string) || grepl("^\\d{2}:\\d{2}:\\d{2}$", input_string)) {
    return(list(type = "Date/Time Value", needs_quotes = TRUE))
  }

  # Default to string literal for general text
  return(list(type = "String Literal", needs_quotes = TRUE))
}

get_all_functions <- function(ssh_session, postgres_keys) {

  # Abfrage, um alle definierten Funktionen abzurufen
  query <- "
  SELECT
      n.nspname AS schema_name,
      p.proname AS function_name,
      pg_catalog.pg_get_function_result(p.oid) AS return_type,
      pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
      pg_catalog.pg_get_functiondef(p.oid) AS function_code
  FROM
      pg_catalog.pg_proc p
  LEFT JOIN
      pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE
      pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
  ORDER BY
      n.nspname, p.proname;
  "

  data_result <- execute_psql_query(query, ssh_session, postgres_keys)

  # Gibt das DataFrame mit den Funktionen zurück
  return(data_result)
}

load_functions_to_new_db <- function(function_string, con) {

  functions_df <- function_string

  for (i in 1:nrow(functions_df)) {
    # Hole die Funktionsdefinition
    function_name <- trimws(functions_df$function_name[i])

    # Wenn die Funktion nicht leer ist, beginne die Definition
    if (nchar(function_name) > 0) {
      function_code <- functions_df$function_code[i]

      # Konstruiere die vollständige Funktionsdefinition
      while (i < nrow(functions_df) && functions_df$function_name[i + 1] == "") {
        function_code <- paste(function_code, functions_df$function_code[i + 1])
        i <- i + 1
      }

      # Entferne das '+' am Ende der Zeilen
      function_code <- gsub("\\s*\\+\\s*", "\n", function_code)

      # Führe die Funktionsdefinition aus
      tryCatch({
        dbExecute(con, function_code)
        cat(sprintf("Function loaded: %s.%s\n", functions_df$schema_name[i], function_name))
      }, error = function(e) {
        cat(sprintf("Error loading function %s: %s\n", function_name, e$message))
      })
    }
  }
}

parse_pg_meta <- function(text_output) {
  lines <- unlist(strsplit(text_output, "\n"))
  lines <- lines[!grepl("^\\(\\d+ rows?\\)", lines)]         # remove row count
  lines <- lines[!grepl("^-+\\+", lines)]                    # remove divider line
  lines <- trimws(lines)
  lines <- lines[nzchar(lines)]                              # remove empty lines

  if (length(lines) < 2) return(data.frame())                # no header or data

  header_line <- lines[1]
  data_lines <- lines[-1]

  header <- trimws(unlist(strsplit(header_line, "\\|")))

  if (length(data_lines) == 0) {
    # return empty data.frame with column names
    return(as.data.frame(setNames(replicate(length(header), character(0), simplify = FALSE), header)))
  }

  # Convert to data frame
  df <- read.table(text = paste(data_lines, collapse = "\n"), quote = "", sep = "|", strip.white = TRUE, stringsAsFactors = FALSE, header = FALSE)
  names(df) <- header
  return(df)
}


# Function to execute a psql query over SSH and retrieve the result
execute_psql_query <- function(query, ssh_session, postgres_keys) {
  # Build the psql command with provided credentials
  psql_command <- sprintf("PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -c \"%s\"",
                          postgres_keys[1], postgres_keys[3], postgres_keys[2], postgres_keys[4], postgres_keys[5], query)

  # Execute the query over SSH
  result <- ssh::ssh_exec_internal(ssh_session, psql_command)

  # Return the result as a character string
  data_result <- rawToChar(result$stdout)

  result <- parse_pg_meta(data_result)
  return(result)
}

# Function to retrieve all essential metadata from a table
get_table_metadata <- function(table, ssh_session, postgres_keys) {

  split <- strsplit(table, "\\.")[[1]]
  schema <- split[1]
  table_name <- split[2]

  # Error Handling
  tryCatch({
    # Data Types
    data_type_query <- sprintf("
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = '%s' AND table_schema = '%s';", table_name, schema)
    data_types <- execute_psql_query(data_type_query, ssh_session, postgres_keys)

    # Abfrage nur für Primary Keys
    pk_query <- sprintf("
      SELECT column_name
      FROM information_schema.key_column_usage
      WHERE table_name = '%s'
        AND table_schema = '%s'
        AND constraint_name LIKE '%%_pkey';", table_name, schema)
    primary_keys <- execute_psql_query(pk_query, ssh_session, postgres_keys)

    # Foreign Keys
    fk_query <- sprintf(
      "
SELECT
    tc.constraint_name,
    tc.table_schema AS constraint_schema,
    tc.table_name AS constraint_table,
    kcu.column_name AS constraint_column,
    ccu.table_schema AS referenced_schema,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM
    information_schema.table_constraints AS tc
JOIN
    information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
        AND tc.table_name = kcu.table_name
JOIN
    information_schema.referential_constraints AS rc
        ON tc.constraint_name = rc.constraint_name
        AND tc.table_schema = rc.constraint_schema
JOIN
    information_schema.key_column_usage AS ccu
        ON rc.unique_constraint_name = ccu.constraint_name
        AND rc.unique_constraint_schema = ccu.constraint_schema
        AND kcu.ordinal_position = ccu.ordinal_position
WHERE
    tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name = '%s'
    AND tc.table_schema = '%s';
      ",
      table_name,
      schema
    )
    foreign_keys <- execute_psql_query(fk_query, ssh_session, postgres_keys)

    # Unique Constraints
    unique_query <- sprintf("
  SELECT
    tc.constraint_name,
    kcu.column_name
  FROM
    information_schema.table_constraints tc
  JOIN
    information_schema.key_column_usage kcu
  ON
    tc.constraint_name = kcu.constraint_name
  WHERE
    tc.table_schema = '%s' AND
    tc.table_name = '%s' AND
    tc.constraint_type = 'UNIQUE'
  ORDER BY
    tc.constraint_name, kcu.ordinal_position;
", schema, table_name)
    unique_constraints <- execute_psql_query(unique_query, ssh_session, postgres_keys)

    # Not Null Constraints
    not_null_query <- sprintf("
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = '%s' AND table_schema = '%s' AND is_nullable = 'NO';", table_name, schema)
    not_null_columns <- execute_psql_query(not_null_query, ssh_session, postgres_keys)

    # Check Constraints
    check_query <- sprintf("
      SELECT
        con.conname AS constraint_name,
        pg_get_expr(con.conbin, con.conrelid) AS check_condition
      FROM
        pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
      WHERE
        con.contype = 'c'
        AND rel.relname = '%s'
        AND nsp.nspname = '%s';
    ", table_name, schema)
    check_constraints <- execute_psql_query(check_query, ssh_session, postgres_keys)

    # Default Values
    default_query <- sprintf("
      SELECT column_name, column_default
      FROM information_schema.columns
      WHERE table_name = '%s' AND table_schema = '%s' AND column_default IS NOT NULL;", table_name, schema)
    default_values <- execute_psql_query(default_query, ssh_session, postgres_keys)

    # Indexes
    index_query <- sprintf("
      SELECT indexname, indexdef
      FROM pg_indexes
      WHERE tablename = '%s' AND schemaname = '%s';", table_name, schema)
    indexes <- execute_psql_query(index_query, ssh_session, postgres_keys)

    # Table Comments
    table_comment_query <- sprintf("
      SELECT obj_description(oid)
      FROM pg_class
      WHERE relname = '%s';", table_name)
    table_comment <- execute_psql_query(table_comment_query, ssh_session, postgres_keys)

    # Get all triggers for a given table
    trigger_query <- sprintf("
      SELECT
        trigger_name,
        event_manipulation AS event,
        action_timing AS timing,
        action_statement AS definition
      FROM information_schema.triggers
      WHERE event_object_table = '%s'
        AND event_object_schema = '%s';", table_name, schema)

    trigger_info <- execute_psql_query(trigger_query, ssh_session, postgres_keys)

    # Column Comments
    column_comment_query <- sprintf("
      SELECT
      a.attname AS column_name,
      d.description AS column_comment
      FROM
      pg_catalog.pg_attribute a
      JOIN
      pg_catalog.pg_class c ON a.attrelid = c.oid
      JOIN
      pg_catalog.pg_namespace n ON c.relnamespace = n.oid
      LEFT JOIN
      pg_catalog.pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
      WHERE
      c.relname = '%s'
      AND n.nspname = '%s'
      AND a.attnum > 0
      AND NOT a.attisdropped;", table_name, schema)
    column_comments <- execute_psql_query(column_comment_query, ssh_session, postgres_keys)

    # Combine all information into a list for easy access
    return(list(
      data_types = data_types,
      primary_keys = primary_keys,
      foreign_keys = foreign_keys,
      unique_constraints = unique_constraints,
      not_null_columns = not_null_columns,
      check_constraints = check_constraints,
      default_values = default_values,
      indexes = indexes,
      table_comment = table_comment,
      triggers = trigger_info,
      column_comments = column_comments
    ))

  }, error = function(e) {
    message("Fehler bei der Abfrage: ", e$message)
    return(NULL)
  })
}

parse_column_data_types <- function(raw_output_string) {
  # Split the string into lines
  lines <- unlist(strsplit(raw_output_string, "\n"))

  # Find lines that look like column definitions (contain a "|")
  data_lines <- grep("\\|", lines, value = TRUE)

  # Remove the header and separator
  data_lines <- data_lines[-c(1)]

  # Remove footer lines like "(7 rows)"
  data_lines <- data_lines[!grepl("^\\(", data_lines)]

  # Split and trim
  parsed <- do.call(rbind, lapply(data_lines, function(line) {
    parts <- strsplit(line, "\\|")[[1]]
    data.frame(
      column_name = trimws(parts[1]),
      data_type = trimws(parts[2]),
      stringsAsFactors = FALSE
    )
  }))

  return(parsed)
}

apply_column_types <- function(df, type_info) {
  for (i in seq_len(nrow(type_info))) {

    col <- type_info$column_name[i]
    type <- type_info$data_type[i]

    if (!col %in% names(df)) next

    raw_values <- df[[col]]
    raw_values[raw_values %in% c("", "NULL", "NA", "NaN")] <- NA

    df[[col]] <- switch(type,
      "text" = as.character(raw_values),
      "integer" = as.integer(raw_values),
      "bigint" = as.integer64(raw_values),
      "boolean" = ifelse(raw_values %in% c("t", "f"), raw_values == "t", NA),
      "timestamp without time zone" = as.POSIXct(raw_values, tz = "UTC", tryFormats = c(
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d"
      )),
      raw_values  # fallback
    )
  }
  return(df)
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

