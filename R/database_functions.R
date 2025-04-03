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
postgres_upsert_data <- function(connection, schema, table, data) {
  # ----- Start -----

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

    # Upsert-Statement (INSERT mit ON CONFLICT)
    query <- sprintf(
      "INSERT INTO %s.%s (%s)\n    SELECT %s FROM %s\n    ON CONFLICT (id) DO UPDATE \n    SET %s",
      schema, table, cols_str, cols_str, temp_table, update_str
    )

    # Query ausführen
    DBI::dbExecute(connection, query)

    # Gesamtanzahl der Zeilen in der Tabelle nach dem Upsert abrufen
    total_rows <- DBI::dbGetQuery(connection, paste0("SELECT COUNT(*) FROM ", schema, ".", table))$count

    # Erfolgreiche Rückgabe
    paste("Upsert erfolgreich! Gesamtanzahl der Zeilen:", total_rows)
  }, error = function(e) {
    # Fehlerbehandlung: Fehler ausgeben und temporäre Tabelle löschen
    message("Fehler beim Upsert: ", e$message)
    return("Upsert fehlgeschlagen.")
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
#' @return Only a feedback message in the console.
#' @export
postgres_rename_table <- function(con, old_name, new_name, schema = "raw") {
  # ----- Start -----
  query <- sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema, old_name, new_name)
  dbExecute(con, query)
  message(sprintf("Table '%s.%s' renamed to '%s.%s'", schema, old_name, schema, new_name))
}

#' postgres_rename_table
#'
#' Renames an existing table in a PostgreSQL database.
#'
#' @param con The database connection object.
#' @param old_name The current name of the table.
#' @param new_name The new name for the table.
#' @param schema The schema where the table is located (default is "raw").
#' @return Only a feedback message in the console.
#' @export
postgres_rename_table <- function(con, old_name, new_name, schema = "raw") {
  # ----- Start -----
  query <- sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema, old_name, new_name)
  dbExecute(con, query)
  message(sprintf("Table '%s.%s' renamed to '%s.%s'", schema, old_name, schema, new_name))
}
