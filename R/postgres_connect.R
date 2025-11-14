#' Connect to Local PostgreSQL Database with Optional Table Synchronization
#'
#' This function establishes a connection to a local PostgreSQL database. Optionally, it can load specified tables
#' from the production environment—either always or only if they are not yet available locally.
#'
#' The function can be used interactively (e.g., in RStudio) or in a server context.
#'
#' @param needed_tables A character vector of fully qualified table names in the format `"schema.table"`, e.g.,
#'        `c("raw.crm_leads", "analytics.dashboard_metrics")`. If `NULL`, no synchronization is performed.
#' @param con An existing PostgreSQL connection object. If `NULL`, a new connection will be established.
#' @param postgres_keys A list of credentials for the production database (required if `con = NULL` in server mode).
#'        Required fields: `password`, `user`, `dbname`, `host`, `port`.
#' @param update_local_tables Logical. If `TRUE`, all specified tables will be pulled from production.
#'        If `FALSE` (default), only missing local tables will be downloaded.
#' @param ssh_key_path Path to the SSH key used to access the production database.
#' @param ssl_cert_path Path to the SSL certificate file for the database connection.
#' @param local_dbname, local_host, local_port, local_user, local_pw Parameters for configuring the local database connection.
#' @param local_password_is_product Logical. Indicates whether the local password is the same as the production password
#'        (only relevant for SSH-based data transfer).
#' @param load_in_memory Logical. Currently not used. Default is `FALSE`.
#' @param chunk_size Integer. Number of rows to process at once when downloading large tables. Default is `10000`.
#'        Smaller values use less memory but may be slower. Increase for faster transfers if memory allows.
#' @param verbose Logical. If `TRUE`, shows detailed output including function loading, foreign keys, ID ranges, etc.
#'        If `FALSE` (default), only shows table name and progress bar.
#' @param path_to_keys_db Character. Optional path to the keys database. If `NULL` (default), uses the standard path.
#' @param path_to_user_db Character. Optional path to the user database. If `NULL` (default), uses the standard path.
#' @param force_interactive Logical. If `TRUE`, forces interactive mode (local database connection) even if `interactive()` returns `FALSE`.
#'        If `FALSE`, forces non-interactive mode (production database connection). If `NULL` (default), uses the result of `interactive()`.
#'
#' @return Returns a connection object to the local database if a new connection is established.
#'         If a connection is passed in via `con`, it is returned unchanged.
#'
#' @details
#' - In interactive mode, if no connection is passed, the user will be prompted to enter the local database password.
#' - In server mode, a connection will only be established if `con` is `NULL`.
#' - If tables are specified, the function checks whether they already exist locally (unless `update_local_tables = TRUE`).
#' - Missing or outdated tables will be automatically synchronized from the production environment.
#'
#' @examples
#' \dontrun{
#'   # Connect and only download missing tables
#'   con <- postgres_connect(
#'     needed_tables = c("raw.crm_leads", "analytics.dashboard_metrics"),
#'     update_local_tables = FALSE
#'   )
#'
#'   # For very large tables, reduce chunk size to use less memory
#'   con <- postgres_connect(
#'     needed_tables = c("raw.large_table"),
#'     chunk_size = 10000  # Process 10k rows at a time
#'   )
#'
#'   # Enable verbose output for detailed progress information
#'   con <- postgres_connect(
#'     needed_tables = c("raw.crm_leads"),
#'     verbose = TRUE  # Shows detailed output (functions loaded, ID ranges, etc.)
#'   )
#'
#'   # Force interactive mode (local DB) even when running from a script
#'   con <- postgres_connect(
#'     force_interactive = TRUE,
#'     local_pw = "your_password"
#'   )
#' }
#'
#' @export
postgres_connect <- function(postgres_keys = NULL,
                             ssl_cert_path = "../../metabase-data/postgres/eu-central-1-bundle.pem",
                             needed_tables = NULL,
                             con = NULL,
                             update_local_tables = FALSE,
                             ssh_key_path = NULL,
                             local_dbname = "studyflix_local",
                             local_host = "localhost",
                             local_port = 5432,
                             local_user = "postgres",
                             local_pw = NULL,
                             load_in_memory = FALSE,
                             local_password_is_product = FALSE,
                             chunk_size = 10000,
                             verbose = FALSE,
                             path_to_keys_db = NULL,
                             path_to_user_db = NULL,
                             force_interactive = NULL) {

  # Determine if we're in interactive mode
  is_interactive <- if (!is.null(force_interactive)) {
    force_interactive
  } else {
    interactive()
  }

  if (!is_interactive)

  { # Server Modus

    message("ℹ️ Server-Modus erkannt - Gebe nur Connection zurück")
    if (is.null(con)) { # Wenn keine Connection übergeben wird, dann erstelle eine neue
      if (is.null(postgres_keys)) { # Wenn keine Keys übergeben werden, dann stoppe die Funktion
        stop("Bitte entweder eine bestehende Connection übergeben oder die Keys für eine neue Postgres-Verbindung.")
      }
      con <- postgres_connect_intern_function(postgres_keys = postgres_keys, force_interactive = force_interactive)
      return(con)
    }
    message("ℹ️ Server-Modus erkannt und bestehende Connection übergeben. Gebe diese zurück.")
    return(con)

  }

  else

  { # Interaktiver Modus

    message("ℹ️ Interaktiver Modus erkannt – verbinde mit lokaler PostgreSQL-Datenbank")
    if (is.null(con)) { # Wenn keine Connection übergeben wird, dann erstelle eine neue
      if (is.null(local_pw)) { # Wenn kein Passwort übergeben wird, dann frage es ab
        local_pw <- getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
        local_password_is_product <- TRUE # Wenn kein Passwort initial übergeben wird, dann ist es der Produktnutzer
        if (is.null(local_pw)) { # Wenn immer noch kein Passwort übergeben wird, dann stoppe die Funktion
          stop("Bitte entweder eine bestehende Connection übergeben oder das Passwort für die lokale DB angeben.")
        }
      }
      result <- connect_with_retry(postgres_keys = postgres_keys, local_pw = local_pw, local_host = local_host, local_port = local_port, local_user = local_user, local_dbname = local_dbname, ssl_cert_path = ssl_cert_path, force_interactive = force_interactive)

      con <- result$connection
      local_pw <- result$local_pw  # optional: updated password
    }

    # Wenn keine Tabellen angegeben sind, gebe nur die Verbindung zurück
    if (is.null(needed_tables)) {
      message("Keine Tabellen angegeben. Es werden keine Daten geladen.")
      return(con)
    }

    # Bestimme zu downloadende Tabellen
    tables_to_pull <- postgres_get_tables_to_pull(needed_tables, con, update_local_tables)

    # Lade Tabellen aus der Produktion nacheinander, so wird der RAM nicht überlastet
    if (length(tables_to_pull) > 0) {
      first_table <- TRUE
      results <- ""  # Initialisiere results für den ersten Durchlauf
      available_tables <- c()  # Liste der bereits heruntergeladenen Tabellen

      required_packages <- c("DBI", "RPostgres", "RSQLite", "ssh", "getPass", "shinymanager", "utils")

      for (pkg in required_packages) {
        if (!requireNamespace(pkg, quietly = TRUE)) {
          install.packages(pkg)
        }
        suppressPackageStartupMessages(library(pkg, character.only = TRUE))
      }

      for (table_name in tables_to_pull) {

        if (verbose) {
          message("⬇️ Ziehe Tabelle aus Produktion: ", table_name)
        }
        results <- postgres_pull_production_tables(
          table = table_name,  # einzeln übergeben
          local_con = con,
          ssh_key_path = ssh_key_path,
          local_password = local_pw,
          load_in_memory = load_in_memory,
          local_password_is_product = local_password_is_product,
          load_functions_to_db = first_table, # Load Functions only in first run
          preset_ssh_key = results,
          available_tables = available_tables,
          chunk_size = chunk_size,
          verbose = verbose,
          path_to_keys_db = path_to_keys_db,
          path_to_user_db = path_to_user_db
        )

        available_tables <- c(available_tables, table_name)  # Füge die Tabelle der Liste hinzu
        first_table <- FALSE  # Nach dem ersten Durchlauf nicht mehr TRUE setzen

      }

      # Nach dem Laden aller Tabellen: Finale Überprüfung aller Identity-Sequenzen
      # (als Sicherheit, falls irgendwo etwas schief ging)
      message("🔄 Überprüfe Identity-Sequenzen für alle Tabellen...")
      postgres_restart_identities(con = con)

    } else {
      message("✅ Alle Tabellen bereits lokal vorhanden. Kein Download nötig.")
    }

  }

  return(con)
}

################################################################################-
# ----- Internal Functions -----

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

#' Connect to PostgreSQL with Retry on Password Failure
#'
#' This internal helper function tries to establish a PostgreSQL connection using the provided credentials.
#' If the first attempt fails (e.g. wrong password), it prompts the user to re-enter the password
#' via a secure prompt (`getPass::getPass`) and retries once.
#'
#' @param postgres_keys List of PostgreSQL connection keys or credentials.
#' @param local_pw Character. The initial password to use for the connection attempt.
#' @param local_host Character. The database host.
#' @param local_port Integer or Character. The database port.
#' @param local_user Character. The database username.
#' @param local_dbname Character. The database name.
#' @param ssl_cert_path Character. Path to the SSL certificate (if required).
#'
#' @return A list with two elements:
#' \describe{
#'   \item{connection}{The established database connection object.}
#'   \item{local_pw}{The password that was successfully used for the connection.}
#' }
#'
#' @keywords internal
#' @seealso \code{\link[getPass]{getPass}}
#' @examples
#' \dontrun{
#' result <- connect_with_retry(postgres_keys, local_pw, local_host, local_port, local_user, local_dbname, ssl_cert_path)
#' con <- result$connection
#' local_pw <- result$local_pw
#' }
connect_with_retry <- function(postgres_keys, local_pw, local_host, local_port, local_user, local_dbname, ssl_cert_path, force_interactive = NULL) {
  tryCatch({
    # Erster Verbindungsversuch
    con <- postgres_connect_intern_function(
      postgres_keys = postgres_keys,
      local_pw = local_pw,
      local_host = local_host,
      local_port = local_port,
      local_user = local_user,
      local_dbname = local_dbname,
      ssl_cert_path = ssl_cert_path,
      force_interactive = force_interactive
    )
    list(connection = con, local_pw = local_pw)
  }, error = function(e) {
    message("First connection attempt failed: ", e$message)
    retry_pw <- getPass::getPass("Please enter your product passwort again: ")

    # Zweiter Versuch
    con <- postgres_connect_intern_function(
      postgres_keys = postgres_keys,
      local_pw = retry_pw,
      local_host = local_host,
      local_port = local_port,
      local_user = local_user,
      local_dbname = local_dbname,
      ssl_cert_path = ssl_cert_path,
      force_interactive = force_interactive
    )
    list(connection = con, local_pw = retry_pw)
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
#' @param passwd Optional. Passphrase für den SSH-Key. Wird interaktiv abgefragt, wenn nicht angegeben.
#' @param timeout Timeout in Sekunden für den SSH-Verbindungsaufbau. Standard: 60 Sekunden.
#'
#' @return Ein aktives SSH-Verbindungsobjekt vom Typ `ssh::ssh_session`.
#'
#' @details
#' - Die Funktion verwendet das Paket `ssh`, um eine Verbindung zum Remote-Server aufzubauen.
#' - Vor dem Verbindungsaufbau wird geprüft, ob der angegebene SSH-Key existiert.
#' - Bei Timeout-Problemen wird automatisch ein Retry-Mechanismus aktiviert. Die Anzahl der Versuche
#'   wird basierend auf dem `timeout`-Parameter berechnet (1 Versuch pro 10 Sekunden).
#' - Zwischen den Versuchen wird eine kurze Pause (2 Sekunden) eingelegt.
#' - Tritt ein Fehler beim Verbindungsaufbau auf, wird eine hilfreiche Fehlermeldung mit Lösungsvorschlägen ausgegeben.
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
establish_ssh_connection <- function(ssh_key_path, remote_user, remote_host, passwd = "", timeout = 60) {
  if (!file.exists(ssh_key_path)) {
    stop("Die angegebene SSH-Key-Datei existiert nicht: ", ssh_key_path)
  }

  if(passwd == "") {
    passwd <- getPass("Gib dein Passphrase für den SSH Key ein:")
  }

  #message("🔐 Aufbau SSH-Verbindung...")

  # Retry-Mechanismus mit mehreren Versuchen
  # libssh hat einen festen Timeout von ~10 Sekunden pro Versuch
  max_retries <- ceiling(timeout / 10)  # Anzahl der Versuche basierend auf gewünschtem Timeout

  for (attempt in 1:max_retries) {
    result <- tryCatch({
      if (attempt > 1) {
        message(sprintf("🔄 Verbindungsversuch %d von %d...", attempt, max_retries))
      }

      ssh_session <- ssh::ssh_connect(
        host = paste0(remote_user, "@", remote_host),
        keyfile = ssh_key_path,
        passwd = passwd,
        verbose = FALSE
      )

      # Erfolgreiche Verbindung
      list(success = TRUE, session = ssh_session, error = NULL)

    }, error = function(e) {
      # Fehler aufgetreten
      list(success = FALSE, session = NULL, error = e)
    })

    # Wenn erfolgreich, gebe Session zurück
    if (result$success) {
      if (attempt > 1) {
        message("✅ SSH-Verbindung erfolgreich hergestellt")
      }
      return(list(result$session, passwd))
    }

    # Warte kurz zwischen Versuchen (außer beim letzten)
    if (attempt < max_retries) {
      Sys.sleep(2)
    } else {
      # Letzter Versuch fehlgeschlagen - Fehler werfen
      error_msg <- sprintf(
        "Fehler beim Aufbau der SSH-Verbindung nach %d Versuchen: %s\n\nMögliche Lösungen:\n- Überprüfe deine Netzwerkverbindung\n- Stelle sicher, dass du mit dem VPN verbunden bist (falls erforderlich)\n- Überprüfe, ob der SSH-Server erreichbar ist\n- Versuche es später erneut",
        max_retries,
        result$error$message
      )
      stop(error_msg)
    }
  }
}

#' Pull production tables from a remote PostgreSQL database via SSH
#'
#' This internal utility function connects to the production environment via SSH, retrieves the specified table
#' from a PostgreSQL database, and stores it either in a local PostgreSQL database or an in-memory SQLite database.
#' It also loads metadata (like view definitions and SQL functions) if configured.
#'
#' @param table A single fully qualified table name (e.g., `"schema.table"`) to retrieve from the remote database.
#'              If `NULL`, no data is loaded and only the decrypted credentials are returned.
#' @param local_con A live DBI connection to the local target database. Must be provided if `load_in_memory = FALSE`.
#' @param ssh_key_path File path to the private SSH key used to connect to the production environment. If `NULL`, a default path is assumed.
#' @param local_password Optional password for the local database. If `local_password_is_product = TRUE`, this must already be decrypted.
#' @param load_in_memory Logical. If `TRUE`, data is kept in memory (e.g. for testing or lightweight workflows). Not yet implemented.
#' @param local_password_is_product Logical. If `TRUE`, `local_password` is assumed to already contain the decrypted key.
#' @param load_functions_to_db Logical. If `TRUE` (default), all PostgreSQL functions from the remote DB are imported into the local DB.
#' @param preset_ssh_key Optional string name to select from preconfigured SSH key paths (e.g. `"ci"`).
#' @param available_tables Character vector of all known tables (optional). Used to assist metadata writing.
#' @param chunk_size Integer. Number of rows to process at once when downloading the table. Default is `10000`.
#' @param path_to_keys_db Character. Optional path to the keys database. If `NULL` (default), uses the standard path.
#' @param path_to_user_db Character. Optional path to the user database. If `NULL` (default), uses the standard path.
#'
#' @return Returns the decrypted production credentials invisibly (as a named list), e.g., for further use.
#'
#' @details
#' This function automatically checks whether the requested object is a table or a view. If it's a view,
#' the corresponding view definition is retrieved and stored in the local DB.
#' If it's a table, the data is copied along with its metadata (like column types and keys).
#'
#' SSH and PostgreSQL sessions are automatically closed after the function completes.
#'
#' @keywords internal
postgres_pull_production_tables <- function(table = NULL,
                                            local_con = NULL,
                                            ssh_key_path = NULL,
                                            local_password = NULL,
                                            load_in_memory = FALSE,
                                            local_password_is_product = FALSE,
                                            load_functions_to_db = TRUE,
                                            preset_ssh_key = "",
                                            available_tables = c(),
                                            chunk_size = 10000,
                                            verbose = FALSE,
                                            path_to_keys_db = NULL,
                                            path_to_user_db = NULL) {

  if (!interactive()) {
    message("Die Funktion 'pull_production_tables' wird nur in interaktiven Sitzungen ausgeführt.")
    return(list(NULL, NULL)) # should never happen
  }

  ssh_session <- connect_to_studyflix_ssh(ssh_key_path = ssh_key_path, preset_ssh_key = preset_ssh_key)

  # Automatisch trennen bei Funktionsende
  on.exit({
    # message("🔒 SSH-Verbindung wird geschlossen...")
    try(ssh::ssh_disconnect(ssh_session[[1]]), silent = TRUE)
  })

  decrypt_key <- load_postgres_decrypt_key(local_password = local_password, local_password_is_product = local_password_is_product, path_to_keys_db = path_to_keys_db, path_to_user_db = path_to_user_db)
  postgres_keys <- get_postgres_keys_via_ssh(ssh_session = ssh_session, decrypt_key = decrypt_key)

  # Lade Funktionen in die lokale DB, wenn es die erste Tabelle ist
  if (load_functions_to_db) { load_functions_to_new_db(con = local_con, function_string = get_all_functions(ssh_session[[1]], postgres_keys), verbose = verbose) }

  table_is_view <- test_view_or_table(ssh_session[[1]], postgres_keys, table)

  if (table_is_view) {

    view_definitions <- get_requested_views(ssh_session[[1]], postgres_keys, table)
    load_view_definitions_to_new_db(con = local_con, view_definitions = view_definitions)

  } else {

    tables_data <- load_postgres_table_via_ssh(table, ssh_session, postgres_keys, ssh_key_path = ssh_key_path, preset_ssh_key = ssh_session[[2]], chunk_size = chunk_size, verbose = verbose)
    ssh_session <- tables_data$ssh_session
    if (is.null(tables_data)) { return(ssh_session[[2]]) }

    # Ziel: lokale PostgreSQL
    # message("💾 Lade lokale PostgreSQL-Datenbank...")

    # Write tables
    split <- strsplit(table, "\\.")[[1]]
    schema <- split[1]
    table_name <- split[2]

    DBI::dbExecute(local_con, "SET client_min_messages TO WARNING;")
    DBI::dbExecute(local_con, sprintf('CREATE SCHEMA IF NOT EXISTS "%s";', schema))
    DBI::dbExecute(local_con, "SET client_min_messages TO NOTICE;")

    if (verbose) {
      message(sprintf("📤 Schreibe Tabelle %s nach PostgreSQL (%s.%s)", table, schema, table_name))
    }

    write_table_with_metadata(con = local_con, schema = schema, table_name = table_name, table_data_with_meta = tables_data, available_tables = available_tables, chunk_size = chunk_size, verbose = verbose)
    # Identity-Sequenz wird bereits in write_table_with_metadata gesetzt
    # postgres_restart_identities(con = local_con) ist nicht mehr nötig

  }

  return(ssh_session[[2]])
}

load_postgres_decrypt_key <- function(local_password, local_password_is_product = FALSE, path_to_keys_db = NULL, path_to_user_db = NULL) {
  # Passwort bestimmen
  produkt_key <- if (local_password_is_product) {
    local_password
  } else {
    getPass::getPass("Gib das Passwort für den Produktnutzer ein:")
  }

  # Versuch, den Decrypt-Key zu laden – mit doppeltem Fallback
  tryCatch({
    if(is.null(path_to_keys_db) && is.null(path_to_user_db)) {
      return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key))
    } else if(!is.null(path_to_keys_db) && is.null(path_to_user_db)) {
      return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_keys_db = path_to_keys_db))
    } else if(is.null(path_to_keys_db) && !is.null(path_to_user_db)) {
      return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_user_db = path_to_user_db))
    } else {
      return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_keys_db = path_to_keys_db, path_to_user_db = path_to_user_db))
    }
  }, error = function(e1) {
    tryCatch({
          if(is.null(path_to_keys_db) && is.null(path_to_user_db)) {
            return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key))
          } else if(!is.null(path_to_keys_db) && is.null(path_to_user_db)) {
            return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_keys_db = path_to_keys_db))
          } else if(is.null(path_to_keys_db) && !is.null(path_to_user_db)) {
            return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_user_db = path_to_user_db))
          } else {
            return(shinymanager::custom_access_keys_2(name_of_secret = "postgresql_public_key", preset_key = produkt_key, path_to_keys_db = path_to_keys_db, path_to_user_db = path_to_user_db))
          }
    }, error = function(e2) {
      stop(
        paste(
          "Fehler beim Auslesen der Keys-DB. Mögliche Fehlerquellen:",
          "- falsches Produktpasswort",
          "- inkompatible `shinymanager`-Version",
          "- veraltete oder beschädigte Keys-Datenbank.",
          "\nUrsprüngliche Fehlermeldung:",
          e2$message
        )
      )
    })
  })
}

connect_to_studyflix_ssh <- function(ssh_key_path = NULL, preset_ssh_key = "") {
  # SSH-Key automatisch bestimmen, falls nicht angegeben
  if (is.null(ssh_key_path)) {
    user <- Sys.getenv("USER")
    if (user == "") user <- Sys.getenv("USERNAME")
    ssh_key_path <- file.path("C:/Users", user, ".ssh", "id_rsa")
  }

  # Existenz des SSH-Keys prüfen
  if (!file.exists(ssh_key_path)) {
    warning(sprintf("❌ Die angegebene SSH-Key-Datei existiert nicht: %s", ssh_key_path))
    return(list(NULL, NULL))
  }

  # Verbindung herstellen
  ssh_session <- establish_ssh_connection(
    ssh_key_path = ssh_key_path,
    remote_user = "application-user",
    remote_host = "shiny.studyflix.info",
    passwd = preset_ssh_key # Wird in establish_ssh_connection abgefragt, falls NULL
  )

  return(ssh_session)
}


get_postgres_keys_via_ssh <- function(ssh_session, decrypt_key) {
  if (length(ssh_session) < 1) {
    stop("SSH-Session ist leer oder ungültig.")
  }

  # Escape Anführungszeichen im Schlüssel
  safe_key <- gsub("\"", "\\\"", decrypt_key)

  # Dynamisches Bash-Skript bauen
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
  ", safe_key)

  # Remote-Befehl ausführen
  result <- ssh::ssh_exec_internal(ssh_session[[1]], cmd)

  # Ergebnis in einzelne Komponenten aufsplitten
  raw_output <- rawToChar(result$stdout)
  split_output <- strsplit(raw_output, ",")[[1]]

  if (length(split_output) < 3) {
    stop("Fehler beim Parsen der PostgreSQL-Schlüssel – unvollständiger oder leerer Output.")
  }

  return(split_output)
}

load_postgres_table_via_ssh <- function(table, ssh_session, postgres_keys, ssh_key_path = ssh_key_path, preset_ssh_key = preset_ssh_key, chunk_size = 10000, verbose = FALSE) {
  if (!verbose) {
    # Im non-verbose Modus: nur Tabellenname anzeigen
    message(sprintf("📥 %s", table))
  } else {
    message(sprintf("📥 Versuche Tabelle zu laden: %s", table))
  }

  # Metadaten abrufen
  metadata <- get_table_metadata(table, ssh_session[[1]], postgres_keys)

  # Zähle zuerst die Zeilen, um zu entscheiden ob Chunking nötig ist
  count_cmd <- sprintf(
    'PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -t -A -c \"SELECT COUNT(*) FROM %s;\" 2>&1; echo \"EXIT_STATUS:$?\"',
    postgres_keys[1], postgres_keys[3], postgres_keys[2], postgres_keys[4], postgres_keys[5], table
  )

  count_result <- ssh::ssh_exec_internal(ssh_session[[1]], count_cmd)
  output <- rawToChar(count_result$stdout)
  parts <- strsplit(output, "EXIT_STATUS:")[[1]]

  count_part <- parts[1]
  status <- as.integer(parts[2])

  # Fehlerbehandlung für COUNT-Abfrage
  if (status != 0 || grepl("ERROR:", count_part, ignore.case = TRUE)) {
    error_msg <- trimws(count_part)

    if (grepl("does not exist|existiert nicht|relation.*not found", error_msg, ignore.case = TRUE)) {
      message(sprintf("❌ Tabelle %s existiert nicht in der Produktionsdatenbank", table))
    } else if (grepl("permission denied|Zugriff verweigert", error_msg, ignore.case = TRUE)) {
      message(sprintf("❌ Keine Berechtigung für Tabelle %s", table))
    } else {
      message(sprintf("❌ Fehler bei Tabelle %s: %s", table, error_msg))
    }
    return(NULL)
  }

  total_rows <- as.integer(trimws(count_part))

  if (is.na(total_rows) || total_rows == 0) {
    # Leere Tabelle - einfach leeren DataFrame zurückgeben
    df <- data.frame(matrix(ncol = nrow(metadata$data_types), nrow = 0))
    names(df) <- metadata$data_types$column_name
    df <- apply_column_types(df, metadata$data_types)
    if (verbose) {
      message(sprintf("✅ Tabelle %s ist leer (0 Zeilen)", table))
    }
    return(list(data = df, metadata = metadata))
  }

  # WICHTIG: Immer Chunking verwenden, auch für kleine Tabellen
  # rawToChar() kann bei großen Strings zu Memory-Errors führen
  # Maximal 10 Chunks verwenden - bei großen Tabellen chunk_size entsprechend anpassen
  max_chunks <- 10
  num_chunks <- min(max_chunks, ceiling(total_rows / chunk_size))
  adjusted_chunk_size <- ceiling(total_rows / num_chunks)
  if (verbose) {
    message(sprintf("📊 Tabelle enthält %d Zeilen - lade in %d Chunks (Chunk-Größe: %d)...", total_rows, num_chunks, adjusted_chunk_size))
  }

  all_chunks <- list()

  for (chunk_num in seq_len(num_chunks)) {
    offset <- (chunk_num - 1) * adjusted_chunk_size

    # Progress anzeigen (überschreibend)
    rows_loaded <- min(offset + adjusted_chunk_size, total_rows)
    progress_pct <- round(100 * rows_loaded / total_rows)
    progress_bar_length <- 20
    filled <- round(progress_bar_length * rows_loaded / total_rows)
    bar <- paste0(paste(rep("█", filled), collapse = ""), paste(rep("░", progress_bar_length - filled), collapse = ""))

    cat(sprintf("\r  ⏳ [%s] %d%% (%d/%d Zeilen)", bar, progress_pct, rows_loaded, total_rows))
    flush.console()

    # Alle Chunks bekommen HEADER - wir filtern später beim Lesen
    # Befehl für SSH-psql mit LIMIT und OFFSET
    # Wichtig: ORDER BY id um sicherzustellen, dass LIMIT/OFFSET konsistent ist
    # NULL wird als \N exportiert (PostgreSQL Standard für CSV NULL)
    cmd <- sprintf(
      'PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -c \"\\\\copy (SELECT * FROM %s ORDER BY id LIMIT %d OFFSET %d) TO STDOUT WITH (FORMAT CSV, HEADER true)\" 2>&1; echo \"EXIT_STATUS:$?\"',
      postgres_keys[1], postgres_keys[3], postgres_keys[2], postgres_keys[4], postgres_keys[5],
      table, adjusted_chunk_size, offset
    )

    result <- ssh::ssh_exec_internal(ssh_session[[1]], cmd)
    output <- rawToChar(result$stdout)
    parts <- strsplit(output, "EXIT_STATUS:")[[1]]

    data_part <- parts[1]
    status <- as.integer(parts[2])

    # Fehlerbehandlung
    if (status != 0 || (grepl("error", data_part, ignore.case = TRUE) & grepl("^ERROR:", output, ignore.case = TRUE))) {
      error_msg <- if (grepl("error", data_part, ignore.case = TRUE)) data_part else "Unbekannter Fehler"
      message(sprintf("❌ Fehler bei Chunk %d: %s", chunk_num, error_msg))
      return(NULL)
    }

    # Chunk einlesen - alle haben Header
    # WICHTIG: PostgreSQL verwendet \N für NULL
    # "NA" ist ein String und sollte NICHT als NULL interpretiert werden!
    # WICHTIG: colClasses = "character" verhindert automatische Typ-Konvertierung
    # (z.B. führende Nullen bei Tel-Nummern würden sonst verloren gehen)
    chunk_df <- tryCatch({
      raw_df <- utils::read.csv(text = data_part, stringsAsFactors = FALSE, header = TRUE,
                     na.strings = c("\\N"), colClasses = "character")

      # WICHTIG: read.csv konvertiert \N in character-Spalten zu "" statt NA
      # Wir müssen leere Strings nachträglich zu NA konvertieren (aber nur für \N, nicht für echte "")
      # Problem: Wir können nicht unterscheiden zwischen echtem "" und \N-konvertiertem ""
      # Lösung: Alle "" in character-Spalten zu NA konvertieren (PostgreSQL speichert "" nicht als NULL)
      for (col in names(raw_df)) {
        if (is.character(raw_df[[col]])) {
          raw_df[[col]][raw_df[[col]] == ""] <- NA_character_
        }
      }
      raw_df
    }, error = function(e) {
      message(sprintf("❌ Fehler beim Parsen von Chunk %d: %s", chunk_num, e$message))
      return(NULL)
    })

    if (is.null(chunk_df)) return(NULL)

    all_chunks[[chunk_num]] <- chunk_df

    # Speicher freigeben nach jedem Chunk
    gc(verbose = FALSE)

    # SSH-Session erneuern nach jedem 5. Chunk um Buffer-Probleme zu vermeiden
    if (chunk_num %% 5 == 0 && chunk_num < num_chunks) {
      ssh::ssh_disconnect(ssh_session[[1]])
      Sys.sleep(1)  # Kurze Pause
      ssh_session <- connect_to_studyflix_ssh(ssh_key_path = ssh_key_path, preset_ssh_key = preset_ssh_key)
    }
  }

  # Neue Zeile nach Progress-Bar
  cat("\n")

  # Alle Chunks zusammenfügen und Typen anwenden
  if (verbose) {
    message("🔗 Füge Chunks zusammen...")
  }
  df <- tryCatch({
    combined_df <- do.call(rbind, all_chunks)
    # Typen auf kombinierten DataFrame anwenden (mit robuster Fehlerbehandlung)
    combined_df <- apply_column_types(combined_df, metadata$data_types)
    as.data.frame(combined_df, stringsAsFactors = FALSE)
  }, error = function(e) {
    message(sprintf("❌ Fehler beim Zusammenfügen der Chunks: %s", e$message))
    return(NULL)
  })

  if (!is.null(df)) {
    if (verbose) {
      message(sprintf("✅ Tabelle %s erfolgreich geladen (%d Zeilen)", table, nrow(df)))
    }
    return(list(data = df, metadata = metadata, ssh_session = ssh_session))
  } else {
    return(NULL)
  }
}

# Hilfsfunktion für kleine Tabellen (keine Chunks nötig)
load_table_full <- function(table, ssh_session, postgres_keys, metadata) {
  # NULL wird als \N exportiert (PostgreSQL Standard für CSV NULL)
  cmd <- sprintf(
    'PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -c \"\\\\copy (SELECT * FROM %s) TO STDOUT WITH (FORMAT CSV, HEADER true)\" 2>&1; echo \"EXIT_STATUS:$?\"',
    postgres_keys[1], # Passwort
    postgres_keys[3], # DB-Name
    postgres_keys[2], # User
    postgres_keys[4], # Host
    postgres_keys[5], # Port
    table
  )

  result <- ssh::ssh_exec_internal(ssh_session[[1]], cmd)
  output <- rawToChar(result$stdout)
  parts <- strsplit(output, "EXIT_STATUS:")[[1]]

  data_part <- parts[1]
  status <- as.integer(parts[2])

  # Fehlerbehandlung
  if (status != 0 || (grepl("error", data_part, ignore.case = TRUE) & grepl("^ERROR:", output, ignore.case = TRUE))) {
    error_msg <- if (grepl("error", data_part, ignore.case = TRUE)) data_part else "Unbekannter Fehler"

    if (grepl("does not exist|existiert nicht|relation.*not found", error_msg, ignore.case = TRUE)) {
      message(sprintf("❌ Tabelle %s existiert nicht", table))
    } else if (grepl("permission denied|Zugriff verweigert", error_msg, ignore.case = TRUE)) {
      message(sprintf("❌ Keine Berechtigung für Tabelle %s", table))
    } else {
      message(sprintf("❌ Fehler bei Tabelle %s: %s", table, error_msg))
    }
    return(NULL)
  }

  # Daten einlesen & typisieren
  # WICHTIG: PostgreSQL verwendet \N für NULL in CSV-Export
  # WICHTIG: colClasses = "character" verhindert automatische Typ-Konvertierung
  # (z.B. führende Nullen bei Tel-Nummern würden sonst verloren gehen)
  df <- tryCatch({
    tmp_df <- utils::read.csv(text = data_part, stringsAsFactors = FALSE,
                             na.strings = c("\\N"), colClasses = "character")

    # WICHTIG: read.csv konvertiert \N in character-Spalten zu "" statt NA
    # Alle "" in character-Spalten zu NA konvertieren
    for (col in names(tmp_df)) {
      if (is.character(tmp_df[[col]])) {
        tmp_df[[col]][tmp_df[[col]] == ""] <- NA_character_
      }
    }

    tmp_df <- apply_column_types(tmp_df, metadata$data_types)
    as.data.frame(tmp_df, stringsAsFactors = FALSE)
  }, error = function(e) {
    message(sprintf("❌ Fehler beim Parsen der Daten von Tabelle %s: %s", table, e$message))
    return(NULL)
  })

  return(df)
}

test_view_or_table <- function(ssh_session, postgres_keys, table) {

  split <- strsplit(table, "\\.")[[1]]
  schema <- split[1]
  table_name <- split[2]

  cmd <- sprintf(
    'PGPASSWORD="%s" psql -d "%s" -U "%s" -h "%s" -p "%s" -t -A -c "SELECT table_type FROM information_schema.tables WHERE table_schema = \'%s\' AND table_name = \'%s\';"',
    postgres_keys[1],
    postgres_keys[3],
    postgres_keys[2],
    postgres_keys[4],
    postgres_keys[5],
    schema,
    table_name
  )
  result <- ssh::ssh_exec_internal(ssh_session, cmd)
  output <- trimws(rawToChar(result$stdout))

  is_view <- tolower(output) == "view"

  return(is_view)
}

postgres_restart_identities <- function(con) {

  # Alle Tabellen mit Identity-Spalte 'id' aus allen Schemas holen
  qry <- "
    SELECT table_schema, table_name
    FROM information_schema.columns
    WHERE column_name = 'id'
      AND is_identity = 'YES'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
  "

  tables <- DBI::dbGetQuery(con, qry)
  if (nrow(tables) == 0) {
    message("Keine Identity-Spalten 'id' gefunden.")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(tables))) {
    schema <- tables$table_schema[i]
    table <- tables$table_name[i]

    max_id_qry <- sprintf("SELECT MAX(id) FROM %s.%s", DBI::dbQuoteIdentifier(con, schema), DBI::dbQuoteIdentifier(con, table))
    max_id <- DBI::dbGetQuery(con, max_id_qry)[[1]]

    if (is.na(max_id)) max_id <- 0

    alter_qry <- sprintf("ALTER TABLE %s.%s ALTER COLUMN id RESTART WITH %s",
                        DBI::dbQuoteIdentifier(con, schema),
                        DBI::dbQuoteIdentifier(con, table),
                        as.character(max_id + 1))

    DBI::dbExecute(con, alter_qry)
    #message(sprintf("Identity 'id' in %s.%s auf %s gesetzt", schema, table, as.character(max_id + 1)))
  }

  invisible(NULL)
}

write_table_with_metadata <- function(con, schema, table_name, table_data_with_meta, available_tables, chunk_size = 10000, verbose = FALSE) {

  DBI::dbExecute(con, "SET client_min_messages TO WARNING;")
  drop_table_query <- sprintf("DROP TABLE IF EXISTS %s.%s CASCADE;", schema, table_name)
  res <- DBI::dbExecute(con, drop_table_query)
  DBI::dbExecute(con, "SET client_min_messages TO NOTICE;")

  meta <- table_data_with_meta$metadata

  # Beispiel: identity_columns ist ein data.frame mit Spalten, die Identity bekommen sollen
  if (!is.null(meta$identity_keys) && nrow(meta$identity_keys) > 0) {
      col <- meta$identity_keys[1, ]
      col_name <- col$column_name
  } else {
      col_name <- NULL
  }

  sql <- generate_pg_create_table_simple(con, table_data_with_meta$data[0,], schema = schema, table_name = table_name, id_col = col_name, generated_cols = meta$generated_columns, data_types = meta$data_types)
  DBI::dbExecute(con, sql)

  # Collect metadata
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
      quoted_cols_vec <- DBI::dbQuoteIdentifier(con, group$column_name)
      quoted_cols <- DBI::SQL(paste(quoted_cols_vec, collapse = ", "))

      # Zitiere Constraint-Name
      quoted_constraint_name <- DBI::dbQuoteIdentifier(con, constraint_name)

      # Zitiere Schema und Tabelle
      schema_quoted <- DBI::dbQuoteIdentifier(con, schema)
      table_name_quoted <- DBI::dbQuoteIdentifier(con, table_name)

      # Baue SQL mit glue_sql
      query <- glue::glue_sql(
        "ALTER TABLE {schema_quoted}.{table_name_quoted}
         ADD CONSTRAINT {quoted_constraint_name}
         UNIQUE ({quoted_cols})",
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
    constraint_name <- glue::glue("fk_{table_name}_{fk$constraint_column}")

    # Nur anwenden, wenn die referenzierte Tabelle verfügbar ist
    if (paste0(ref_schema, ".", ref_table) %in% available_tables) {
      # Prüfe, ob Constraint bereits existiert
      constraint_exists_query <- glue::glue("
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = '{schema}'
          AND table_name = '{table_name}'
          AND constraint_name = '{constraint_name}'
          AND constraint_type = 'FOREIGN KEY';
      ")

      exists <- tryCatch({
        DBI::dbGetQuery(con, constraint_exists_query)
      }, error = function(e) NULL)

      if (is.null(exists) || nrow(exists) == 0) {
        # Constraint anlegen
        query <- glue::glue("
          ALTER TABLE {DBI::dbQuoteIdentifier(con, schema)}.{DBI::dbQuoteIdentifier(con, table_name)}
            ADD CONSTRAINT {DBI::dbQuoteIdentifier(con, constraint_name)}
            FOREIGN KEY ({DBI::dbQuoteIdentifier(con, fk$constraint_column)})
            REFERENCES {DBI::dbQuoteIdentifier(con, fk$referenced_schema)}.{DBI::dbQuoteIdentifier(con, fk$referenced_table)} ({DBI::dbQuoteIdentifier(con, fk$referenced_column)})
            {if (fk$update_rule != 'NO ACTION') glue::glue('ON UPDATE {fk$update_rule}') else ''}
            {if (fk$delete_rule != 'NO ACTION') glue::glue('ON DELETE {fk$delete_rule}') else ''};
        ")

        tryCatch({
          DBI::dbExecute(con, query)
          if (verbose) {
            message(glue::glue("✔ FOREIGN KEY added: {table_name} → {ref_table}"))
          }
        }, error = function(e) {
          if (verbose) {
            message(glue::glue("✖ Failed to add FK {constraint_name}: {e$message}"))
          }
        })
      } else {
        if (verbose) {
          message(glue::glue("⚠ Constraint {constraint_name} already exists, skipping."))
        }
      }
    } else {
      if (verbose) {
        message(glue::glue("⚠ Foreign key skipped: {table_name} → {ref_table}. Load {ref_table} before {table_name}."))
      }
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

  # GENERATED COLUMNS werden bereits in CREATE TABLE definiert (siehe generate_pg_create_table_simple)
  # Daher keine separate Logik hier nötig

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
        max_value_result <- DBI::dbGetQuery(con, max_value_query)
        max_value <- dplyr::coalesce(max_value_result$max_value, bit64::as.integer64(1))

        DBI::dbExecute(con, "SET client_min_messages TO WARNING;")
        sequence_creation_query <- sprintf("
          CREATE SEQUENCE IF NOT EXISTS %s
          OWNED BY %s.%s.%s;
        ", sequence_name, schema, table_name, column_name)
        DBI::dbExecute(con, "SET client_min_messages TO NOTICE;")

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

        # Remove type casts (e.g., ::timestamp, ::regclass) from default values
        # This regex removes everything from :: to the end of the word
        default <- gsub("::[a-zA-Z_ ]+", "", default)

        # Default-Wert roh in SQL einfügen (kein quoting!)
        # Use DBI::SQL() to prevent glue_sql from escaping the default value
        default_sql <- DBI::SQL(default)
        query <- glue::glue_sql(
          "ALTER TABLE {schema_quoted}.{table_name_quoted} ALTER COLUMN {col_quoted} SET DEFAULT {default_sql}",
          .con = con
        )

        tryCatch({
          DBI::dbExecute(con, query)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen eines DEFAULT Values für Spalte '%s': %s\n", col, e$message))
        })
    }
  }

  # INDEXES
  if (!is.null(meta$indexes) && nrow(meta$indexes) > 0) {
    for (i in seq_len(nrow(meta$indexes))) {
      index_sql <- meta$indexes$indexdef[i]

      # Ausführen der CREATE INDEX-Anweisung
      # UNIQUE Indexes überspringen, da diese bereits über UNIQUE CONSTRAINTS erstellt wurden
      if (!grepl("UNIQUE", index_sql)) {
        tryCatch({
          DBI::dbExecute(con, index_sql)
        }, error = function(e) {
          # Fehlerbehandlung: Gebe eine Nachricht aus, anstatt die Funktion abzubrechen
          cat(sprintf("Fehler beim Erstellen eines Index: %s\n", e$message))
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
        "ALTER TABLE {schema_quoted}.{table_name_quoted} ADD CONSTRAINT {`constraint_name`} CHECK ({condition});",
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
      query <- meta$triggers$trigger_definition[i]

      tryCatch({
        DBI::dbExecute(con, query)
      }, error = function(e) {
        cat(sprintf(
          "Fehler beim Erstellen des Triggers '%s': %s\n",
          meta$triggers$trigger_name[i], e$message
        ))
      })
    }
  }


  # Write data in chunks to avoid memory issues
  total_rows <- nrow(table_data_with_meta$data)

  if (total_rows == 0) {
    if (verbose) {
      message("  ⚠️ Keine Daten zum Schreiben vorhanden")
    }
    return(NULL)
  }

  # WICHTIG: GENERATED COLUMNS müssen aus den Daten entfernt werden
  # PostgreSQL berechnet diese automatisch und erlaubt keine manuellen Werte
  if (!is.null(meta$generated_columns) && nrow(meta$generated_columns) > 0) {
    gen_col_names <- meta$generated_columns$column_name
    # Entferne alle GENERATED COLUMNS aus dem DataFrame
    table_data_with_meta$data <- table_data_with_meta$data[, !names(table_data_with_meta$data) %in% gen_col_names, drop = FALSE]
    if (verbose) {
      message(sprintf("  🔧 %d GENERATED COLUMNS entfernt (werden automatisch berechnet)", length(gen_col_names)))
    }
  }

  # Die Tabelle wurde bereits mit GENERATED BY DEFAULT AS IDENTITY erstellt,
  # daher können wir existierende IDs direkt einfügen
  # Aber: wir müssen die Identity-Sequenz vorab setzen, damit sie nicht mit den Daten kollidiert
  if (!is.null(col_name) && col_name %in% names(table_data_with_meta$data)) {
    # Prüfe auf Duplikate in den Daten
    id_values <- table_data_with_meta$data[[col_name]]
    duplicated_ids <- id_values[duplicated(id_values)]
    if (length(duplicated_ids) > 0 && verbose) {
      message(sprintf("  ⚠️ WARNUNG: %d doppelte IDs in den Daten gefunden!", length(duplicated_ids)))
      message(sprintf("  Beispiele: %s", paste(head(duplicated_ids, 5), collapse = ", ")))
      message("  → Die Daten aus der Produktion enthalten bereits Duplikate!")
    }

    max_id <- max(id_values, na.rm = TRUE)
    min_id <- min(id_values, na.rm = TRUE)
    if (verbose) {
      message(sprintf("  📊 ID-Range: %s bis %s (%d Zeilen)", min_id, max_id, total_rows))
    }

    # WICHTIG: Sortiere die Daten nach ID, um sicherzustellen, dass jede ID nur in einem Chunk ist
    # (Verhindert Duplikate über Chunk-Grenzen hinweg)
    if (!is.unsorted(id_values)) {
      if (verbose) {
        message("  ✓ Daten sind bereits nach ID sortiert")
      }
    } else {
      if (verbose) {
        message("  🔄 Sortiere Daten nach ID für sichere Chunk-Verarbeitung...")
      }
      table_data_with_meta$data <- table_data_with_meta$data[order(table_data_with_meta$data[[col_name]]), ]
    }

    if (!is.na(max_id) && max_id > 0) {
      # Nutze die gleiche Logik wie postgres_restart_identities()
      # Setze die Identity-Sequenz auf MAX(id) + 1
      tryCatch({
        alter_qry <- sprintf("ALTER TABLE %s.%s ALTER COLUMN %s RESTART WITH %s",
                            DBI::dbQuoteIdentifier(con, schema),
                            DBI::dbQuoteIdentifier(con, table_name),
                            DBI::dbQuoteIdentifier(con, col_name),
                            as.character(max_id + 1))
        DBI::dbExecute(con, alter_qry)
        if (verbose) {
          message(sprintf("  🔢 Identity-Sequenz auf %s gesetzt (höchste ID + 1)", max_id + 1))
        }
      }, error = function(e) {
        if (verbose) {
          message("  ⚠️ Konnte Identity-Sequenz nicht vorab setzen: ", e$message)
        }
      })
    }
  }

  if (total_rows <= chunk_size) {
    # Small table - write in one go
    DBI::dbAppendTable(
      conn = con,
      name = DBI::Id(schema = schema, table = table_name),
      value = table_data_with_meta$data
    )
  } else {
    # Large table - write in chunks
    num_chunks <- ceiling(total_rows / chunk_size)
    if (verbose) {
      message(sprintf("  📝 Schreibe %d Zeilen in %d Batches...", total_rows, num_chunks))
    }

    # Verwende dbWriteTable mit append=TRUE statt dbAppendTable
    # dbWriteTable respektiert GENERATED BY DEFAULT besser
    for (chunk_num in seq_len(num_chunks)) {
      start_idx <- (chunk_num - 1) * chunk_size + 1
      end_idx <- min(chunk_num * chunk_size, total_rows)

      chunk_data <- table_data_with_meta$data[start_idx:end_idx, , drop = FALSE]

      tryCatch({
        DBI::dbWriteTable(
          conn = con,
          name = DBI::Id(schema = schema, table = table_name),
          value = chunk_data,
          append = TRUE,
          row.names = FALSE
        )
        if (verbose && (chunk_num %% 5 == 0 || chunk_num == num_chunks)) {
          message(sprintf("    ✓ Batch %d/%d geschrieben (%d Zeilen)", chunk_num, num_chunks, end_idx))
        }
      }, error = function(e) {
        stop(sprintf("Fehler beim Schreiben von Batch %d: %s", chunk_num, e$message))
      })

      # Speicher freigeben nach jedem Chunk
      gc(verbose = FALSE)
    }
  }

  return(NULL)
}

generate_pg_create_table_simple <- function(con, df, schema = NULL, table_name, id_col = NULL, generated_cols = NULL, data_types = NULL) {

  full_table_name <- if (!is.null(schema)) {
    paste0(DBI::dbQuoteIdentifier(con, schema), ".", DBI::dbQuoteIdentifier(con, table_name))
  } else {
    DBI::dbQuoteIdentifier(con, table_name)
  }

  # Hilfsfunktion: R-Typ → Postgres-Typ (umgekehrtes Mapping)
  r_to_pg <- function(x, col_name = NULL) {
    # If we have metadata with udt_name, use it for exact type mapping
    if (!is.null(data_types) && !is.null(col_name)) {
      type_info <- data_types[data_types$column_name == col_name, ]
      if (nrow(type_info) > 0 && "udt_name" %in% names(type_info)) {
        udt <- type_info$udt_name[1]
        # Map PostgreSQL udt_name to SQL type
        if (udt == "timestamptz") {
          return("TIMESTAMP WITH TIME ZONE")
        } else if (udt == "timestamp") {
          return("TIMESTAMP WITHOUT TIME ZONE")
        } else if (udt == "timetz") {
          return("TIME WITH TIME ZONE")
        } else if (udt == "time") {
          return("TIME WITHOUT TIME ZONE")
        }
        # For other types, continue with R type inference below
      }
    }

    # Fallback: Prüfe Klassen
    if (inherits(x, "integer")) {
      "INTEGER"
    } else if (inherits(x, "integer64")) {
      "BIGINT"
    } else if (inherits(x, "numeric")) {
      "DOUBLE PRECISION"
    } else if (inherits(x, "logical")) {
      "BOOLEAN"
    } else if (inherits(x, "Date")) {
      "DATE"
    } else if (inherits(x, "POSIXct") || inherits(x, "POSIXlt")) {
      "TIMESTAMP WITHOUT TIME ZONE"  # Fallback if no metadata available
    } else if (inherits(x, "hms")) {
      "TIME WITHOUT TIME ZONE"
    } else {
      "TEXT"
    }
  }

  cols <- names(df)

  col_defs <- sapply(cols, function(col) {
    # Prüfe ob Spalte eine GENERATED COLUMN ist
    is_generated <- FALSE
    gen_expr <- NULL
    if (!is.null(generated_cols) && nrow(generated_cols) > 0) {
      gen_row <- generated_cols[generated_cols$column_name == col, ]
      if (nrow(gen_row) > 0) {
        is_generated <- TRUE
        gen_expr <- gen_row$generation_expression[1]
      }
    }

    if (is_generated) {
      # GENERATED COLUMN
      pg_type <- r_to_pg(df[[col]], col_name = col)
      paste0("\"", col, "\" ", pg_type, " GENERATED ALWAYS AS (", gen_expr, ") STORED")
    } else if (!is.null(id_col) && col == id_col) {
      # BY DEFAULT erlaubt das Einfügen von existierenden IDs aus der Produktion
      paste0("\"", col, "\" BIGINT GENERATED BY DEFAULT AS IDENTITY")
    } else {
      pg_type <- r_to_pg(df[[col]], col_name = col)
      paste0("\"", col, "\" ", pg_type)
    }
  }, USE.NAMES = FALSE)

  sql <- paste0(
    "CREATE TABLE ", full_table_name, " (\n  ",
    paste(col_defs, collapse = ",\n  "),
    "\n);"
  )

  return(sql)
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

get_requested_views <- function(ssh_session, postgres_keys, views) {

  view_definitions <- list()

  for (view in views) {
    split <- strsplit(view, "\\.")[[1]]
    schema <- split[1]
    view_name <- split[2]

    message(paste("📥 Versuche View zu laden:", view))

    view_full_name <- sprintf('"%s"."%s"', schema, view_name)
    cmd <- sprintf(
      'PGPASSWORD="%s" psql -d "%s" -U "%s" -h "%s" -p "%s" -t -A -c "SELECT pg_get_viewdef(\'%s\'::regclass, true);"',
      postgres_keys[1], postgres_keys[3], postgres_keys[2],
      postgres_keys[4], postgres_keys[5],
      view_full_name
    )

    result <- ssh::ssh_exec_internal(ssh_session, cmd)
    definition <- trimws(rawToChar(result$stdout))

    if (nchar(definition) > 0) {
      sql <- sprintf('CREATE OR REPLACE VIEW "%s"."%s" AS %s;', schema, view_name, definition)
      view_definitions <- c(view_definitions, sql)
    }

  }

  return(view_definitions)

}

get_all_functions <- function(ssh_session, postgres_keys) {

  # Abfrage, um alle definierten Funktionen abzurufen
  query <- "
COPY (
  SELECT
    n.nspname AS schema_name,
    p.proname AS function_name,
    pg_get_function_result(p.oid) AS return_type,
    pg_get_function_arguments(p.oid) AS arguments,
    pg_get_functiondef(p.oid) AS function_code
  FROM pg_proc p
  LEFT JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  ORDER BY n.nspname, p.proname
) TO STDOUT WITH CSV DELIMITER ',' QUOTE '\"' ESCAPE '\"';
"

  data_result <- execute_functions_query(query, ssh_session, postgres_keys)

  # Gibt das DataFrame mit den Funktionen zurück
  return(data_result)
}

load_view_definitions_to_new_db <- function(view_definitions, con) {
  for (view_sql in view_definitions) {

    # Bessere Regex: Greife nur den View-Namen zwischen CREATE ... VIEW "schema"."view"
    view_name <- sub('.*?CREATE OR REPLACE VIEW\\s+"([^"]+)"\\."([^"]+)"\\s+AS.*',
                     '\\1.\\2', view_sql, ignore.case = TRUE)

    tryCatch({
      DBI::dbExecute(con, view_sql)
      message(sprintf("✅ View '%s' erfolgreich erstellt.", view_name))
    }, error = function(e) {
      # Prüfen auf Fehlermeldung "relation ... does not exist"
      if (grepl("relation \"([^\"]+)\" does not exist", e$message, ignore.case = TRUE)) {
        fehlende_tabelle <- sub(".*relation \"([^\"]+)\" does not exist.*", "\\1", e$message, ignore.case = TRUE)
        message(sprintf("❌ Fehler beim Erstellen der View '%s':", view_name))
        message(sprintf("   Die Tabelle '%s' fehlt in der lokalen DB. Bitte stelle sicher, dass diese Tabelle zuerst mit heruntergeladen und geladen wird.", fehlende_tabelle))
      } else {
        message(sprintf("❌ Fehler beim Erstellen der View '%s':", view_name))
        message(e$message)
      }
    })
  }
}

load_functions_to_new_db <- function(function_string, con, verbose = FALSE) {

  functions_df <- function_string
  loaded_count <- 0
  error_count <- 0

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
        DBI::dbExecute(con, function_code)
        loaded_count <- loaded_count + 1
      }, error = function(e) {
        error_count <- error_count + 1
        if (verbose) {
          cat(sprintf("⚠️ Error loading function %s: %s\n", function_name, e$message))
        }
      })
    }
  }

  # Zusammenfassung statt einzelne Messages
  if (verbose) {
    if (loaded_count > 0) {
      message(sprintf("✅ %d PostgreSQL functions loaded", loaded_count))
    }
    if (error_count > 0) {
      message(sprintf("⚠️ %d functions failed to load", error_count))
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
execute_functions_query <- function(query, ssh_session, postgres_keys) {
  # Build the psql command with provided credentials
  psql_command <- sprintf("PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" <<'EOF'\n%s\nEOF",
                          postgres_keys[1], postgres_keys[3], postgres_keys[2], postgres_keys[4], postgres_keys[5], query)

  # Execute the query over SSH
  result <- ssh::ssh_exec_internal(ssh_session, psql_command)

  output_text <- rawToChar(result$stdout)

  col_names <- c("schema_name", "function_name", "return_type", "arguments", "function_code")

  df <- readr::read_csv(output_text, col_names = col_names, show_col_types = FALSE)

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
    # Data Types - use udt_name to distinguish timestamp vs timestamptz
    data_type_query <- sprintf("
      SELECT column_name, data_type, udt_name
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

    # Abfrage nur für Identities
    identity_query <- sprintf("
    SELECT
      column_name,
      identity_generation
    FROM information_schema.columns
    WHERE table_schema = '%s'
      AND table_name = '%s'
      AND identity_generation IS NOT NULL;", schema, table_name)
    identity_keys <- execute_psql_query(identity_query, ssh_session, postgres_keys)

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
    ccu.column_name AS referenced_column,
    rc.update_rule,
    rc.delete_rule
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
    information_schema.constraint_column_usage AS ccu
        ON rc.unique_constraint_name = ccu.constraint_name
        AND rc.unique_constraint_schema = ccu.constraint_schema
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

    # Default Values (normale defaults, OHNE GENERATED COLUMNS)
    # GENERATED COLUMNS werden separat behandelt weil generation_expression mehrzeilig ist
    default_query <- sprintf("
      SELECT column_name, column_default
      FROM information_schema.columns
      WHERE table_name = '%s' AND table_schema = '%s'
        AND column_default IS NOT NULL
        AND (is_generated IS NULL OR is_generated = 'NEVER');", table_name, schema)
    default_values <- execute_psql_query(default_query, ssh_session, postgres_keys)

    # Generated Columns separat holen (mit CSV wegen mehrzeiligen Expressions)
    gen_col_cmd <- sprintf(
      'PGPASSWORD=\"%s\" psql -d \"%s\" -U \"%s\" -h \"%s\" -p \"%s\" -c \"\\\\copy (SELECT column_name, generation_expression FROM information_schema.columns WHERE table_name = \'%s\' AND table_schema = \'%s\' AND is_generated = \'ALWAYS\') TO STDOUT WITH CSV HEADER\"',
      postgres_keys[1], postgres_keys[3], postgres_keys[2], postgres_keys[4], postgres_keys[5],
      table_name, schema
    )
    result <- ssh::ssh_exec_internal(ssh_session, gen_col_cmd)
    gen_csv <- rawToChar(result$stdout)
    generated_columns <- if (nchar(trimws(gen_csv)) > 0 && grepl("column_name", gen_csv)) {
      read.csv(text = gen_csv, stringsAsFactors = FALSE)
    } else {
      data.frame(column_name = character(), generation_expression = character(), stringsAsFactors = FALSE)
    }

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
        tg.tgname AS trigger_name,
        pg_get_triggerdef(tg.oid, true) AS trigger_definition,
        c.relname AS table_name,
        s.nspname AS schema_name
      FROM pg_trigger tg
      JOIN pg_class c ON tg.tgrelid = c.oid
      JOIN pg_namespace s ON c.relnamespace = s.oid
      WHERE s.nspname = '%s'
        AND c.relname = '%s'
        AND NOT tg.tgisinternal;", schema, table_name)

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
      identity_keys = identity_keys,
      foreign_keys = foreign_keys,
      unique_constraints = unique_constraints,
      not_null_columns = not_null_columns,
      check_constraints = check_constraints,
      default_values = default_values,
      generated_columns = generated_columns,
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
    # Use udt_name if available (more precise), fallback to data_type
    type <- if ("udt_name" %in% names(type_info) && !is.na(type_info$udt_name[i])) {
      type_info$udt_name[i]
    } else {
      type_info$data_type[i]
    }

    if (!col %in% names(df)) next

    raw_values <- df[[col]]
    # WICHTIG: NULL-Werte werden bereits beim CSV-Import erkannt (na.strings = c("\\N"))
    # Hier findet KEINE String-zu-NULL-Konvertierung mehr statt!
    # Alle Strings (auch "", "NULL", "NA", "NaN") bleiben erhalten.

    # Konvertierung mit Fehlerbehandlung - bei Fehler bleiben Werte als character
    df[[col]] <- tryCatch({
      switch(type,
        "text" = as.character(raw_values),
        "varchar" = as.character(raw_values),
        "char" = as.character(raw_values),
        "integer" = as.integer(raw_values),
        "int4" = as.integer(raw_values),
        "bigint" = bit64::as.integer64(raw_values),
        "int8" = bit64::as.integer64(raw_values),
        "smallint" = as.integer(raw_values),
        "int2" = as.integer(raw_values),
        "numeric" = as.numeric(raw_values),
        "decimal" = as.numeric(raw_values),
        "real" = as.numeric(raw_values),
        "float4" = as.numeric(raw_values),
        "double precision" = as.numeric(raw_values),
        "float8" = as.numeric(raw_values),
        "bool" = ifelse(raw_values %in% c("t", "f", "true", "false", "TRUE", "FALSE"),
                           tolower(raw_values) %in% c("t", "true"), NA),
        "boolean" = ifelse(raw_values %in% c("t", "f", "true", "false", "TRUE", "FALSE"),
                           tolower(raw_values) %in% c("t", "true"), NA),
        "timestamp" = as.POSIXct(raw_values, tz = "UTC", tryFormats = c(
          "%Y-%m-%d %H:%M:%S",
          "%Y-%m-%dT%H:%M:%S",
          "%Y-%m-%d"
        )),
        "timestamp without time zone" = as.POSIXct(raw_values, tz = "UTC", tryFormats = c(
          "%Y-%m-%d %H:%M:%S",
          "%Y-%m-%dT%H:%M:%S",
          "%Y-%m-%d"
        )),
        "timestamptz" = as.POSIXct(raw_values, tz = "UTC", tryFormats = c(
          "%Y-%m-%d %H:%M:%S%z",
          "%Y-%m-%dT%H:%M:%S%z",
          "%Y-%m-%d %H:%M:%S",
          "%Y-%m-%dT%H:%M:%S"
        )),
        "timestamp with time zone" = as.POSIXct(raw_values, tz = "UTC", tryFormats = c(
          "%Y-%m-%d %H:%M:%S%z",
          "%Y-%m-%dT%H:%M:%S%z",
          "%Y-%m-%d %H:%M:%S",
          "%Y-%m-%dT%H:%M:%S"
        )),
        "date" = as.Date(raw_values),
        "time" = hms::as_hms(raw_values),
        "time without time zone" = hms::as_hms(raw_values),
        "timetz" = hms::as_hms(raw_values),
        "json" = raw_values,  # optional: jsonlite::fromJSON(raw_values)
        "jsonb" = raw_values,
        "uuid" = as.character(raw_values),
        "bytea" = raw_values,  # falls gewünscht: base64decode oder raw
        {
          message(glue::glue("Unbekannter Datentyp: {type}, wird als character behandelt."))
          as.character(raw_values)
        }
      )
    }, error = function(e) {
      # Bei Konvertierungsfehler: Werte als character belassen
      message(sprintf("⚠️ Fehler bei Konvertierung von Spalte '%s' zu Typ '%s': %s - Spalte bleibt als character",
                      col, type, e$message))
      as.character(raw_values)
    })
  }
  return(df)
}

#' Ermittelt Tabellen, die aus der Produktion gezogen werden müssen
#'
#' Diese Hilfsfunktion prüft basierend auf dem Parameter `update_local_tables`,
#' ob alle oder nur fehlende Tabellen aus der Produktionsdatenbank geholt werden sollen.
#' Dazu wird die aktuelle Verbindung zur lokalen Datenbank genutzt, um vorhandene Tabellen zu erkennen.
#'
#' @param tables Ein Character-Vektor mit vollqualifizierten Tabellennamen im Format `"schema.tabelle"`.
#' @param con Ein PostgreSQL-Verbindungsobjekt zur lokalen Datenbank.
#' @param update_local_tables Logisch. Wenn `TRUE`, werden alle Tabellen zurückgegeben (vollständiger Refresh).
#'
#' @return Character-Vektor mit den Tabellennamen, die noch aus der Produktion gezogen werden müssen.
#'
#' @keywords internal
postgres_get_tables_to_pull <- function(tables, con, update_local_tables) {
  if (update_local_tables) {
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

#' postgres_connect_intern_function
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
#' @return Ein `pool::dbPool`-Objekt, falls die Verbindung erfolgreich war. Andernfalls wird ein Fehler ausgelöst.
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
#' con_local <- postgres_connect_intern_function(local_pw = "dein_passwort")
#'
#' # Verbindung zur Produktionsdatenbank (nicht interaktiv)
#' keys <- list(
#'   "prod_pw",
#'   "prod_user",
#'   "prod_db",
#'   "prod_host",
#'   5432
#' )
#' con_prod <- postgres_connect_intern_function(postgres_keys = keys)
#' }
#'
#' @keywords internal
postgres_connect_intern_function <- function(local_host = "localhost",
                             local_port = 5432,
                             local_user = "postgres",
                             local_dbname = "studyflix_local",
                             postgres_keys = NULL,
                             local_pw = NULL,
                             ssl_cert_path = "../../metabase-data/postgres/eu-central-1-bundle.pem",
                             force_interactive = NULL) {

  # Determine if we're in interactive mode
  is_interactive <- if (!is.null(force_interactive)) {
    force_interactive
  } else {
    interactive()
  }

  tryCatch({
    if (is_interactive) {

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

      local_con <- pool::dbPool(
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
      con <- pool::dbPool(
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
