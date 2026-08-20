# -------------------------- Start script --------------------------------


#' Authenticate Multiple Services
#'
#' Runs authentication for multiple external services such as Billomat, CRM,
#' Google Sheet, Asana, MS Graph, and others, based on the provided service names.
#'
#' The function maps each requested service to its corresponding authentication function
#' and passes the respective argument from \code{args}.
#'
#' **Usage Context:**
#' \itemize{
#'   \item In \strong{FlowForce jobs}, \code{args} is usually filled automatically
#'   by reading from \code{commandArgs(trailingOnly = TRUE)}.
#'   \item In \strong{Shiny apps}, \code{args} typically contains one or more preset keys from
#'   your internal keys database, e.g., via \code{shinymanager::custom_access_keys_2("postgresql_public_key")}.
#' }
#'
#' @param needed_services Character vector of service names to authenticate.
#' Default includes common services like \code{"billomat"}, \code{"crm"}, \code{"google sheet"}, etc.
#' @param args A list of arguments (e.g., API keys, tokens, credentials) for each service,
#' usually populated from FlowForce job parameters or Shiny keys.
#'
#' @return A named list containing authentication results or \code{NA} for unsupported services.
#'
#' @examples
#' \dontrun{
#' # Example in FlowForce context
#' args <- commandArgs(trailingOnly = TRUE)
#' authentication_process(needed_services = c("billomat", "crm"), args = args)
#'
#' # Example in Shiny app
#' args <- list(shinymanager::custom_access_keys_2("postgresql_public_key"))
#' authentication_process(needed_services = c("postgresql"), args = args)
#' }
#' @export
authentication_process <- function(needed_services = c("billomat", "crm", "crm_lm", "google sheet","asana", "msgraph", "msgraph_scoped_app", "msgraph_delegated", "msgraph_sharepoint", "brevo", "google analytics", "bonusDB", "BigQuery", "BigQuery GA4", "cleverreach", "postgresql", "gemini", "openrouter", "openai_admin", "personio", "github", "metabase"), args) {

  auth_functions <- list(
    billomat = authentication_billomat,
    crm = authentication_crm,
    crm_lm = authentication_crm_lm,
    `google sheet` = authentication_GSheet,
    asana = authentication_asana,
    msgraph = authentication_msgraph,
    msgraph_scoped_app = authentication_msgraph_scoped_app,
    msgraph_delegated = authentication_msgraph_delegated,
    msgraph_sharepoint = authentication_msgraph_sharepoint,
    brevo = authentication_brevo,
    `google analytics` = authentication_Google_Analytics,
    bonusDB = authentication_bonus_db,
    BigQuery = authentication_Google_BigQuery,
    `BigQuery GA4` = authentication_Google_BigQuery_GA4,
    cleverreach = authentication_cleverreach,
    postgresql = authentication_postgresql,
    gemini = authentication_gemini,
    openrouter = authentication_openrouter,
    openai_admin = authentication_openai_admin,
    personio = authentication_personio,
    github = authentication_github,
    metabase = authentication_metabase
  )

  keys <- list()

  for (service in needed_services) {
    pos <- match(service, needed_services)

    if (service %in% names(auth_functions)) {
      keys[[service]] <- auth_functions[[service]](args[pos])
    } else {
      keys[[service]] <- NA
    }
  }

  return(keys)
}
#' authentication_billomat
#'
#' Returns `c(legacy_data_key, billomat_api_key)`.
#'
#' Element `[1]` is NOT a copy of the API key. It is the data-encryption key for
#' `base-data/` RDS files and the shinymanager SQLite stores, and roughly 49
#' call sites across the organisation depend on it. Under the `file` backend it
#' is the password the caller supplied, which is what those call sites receive
#' today; under `gsm` it is the stored secret
#' `studyflix-legacy-data-key-billomat`.
#'
#' The password is resolved ONCE and threaded into both calls, so the two
#' elements always derive from the same string. Resolving twice would let an
#' operator who types differently on a second prompt receive a data key that
#' silently does not match the credential.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return `character[2]`: the legacy data key, then the Billomat API key.
authentication_billomat <- function(args) {
  # ---- start ---- #
  prompt <- "Enter the password for Billomat-DB: "
  key <- if (secretsR::secret_backend() == "gsm") NULL else
    billomatics_resolve_key(args, prompt)
  c(billomatics_legacy_data_key("billomat", args, key = key, prompt = prompt),
    billomatics_secret("studyflix-billomat-api-key", args, prompt, key = key))
}
#' authentication_crm
#'
#' Resolves the CRM API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_crm <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-crm-api-key", args,
                     "Bitte Decryption_Key fuer CRM eingeben: ")
}
#' authentication_crm_lm
#'
#' Resolves the CRM Lead-Management API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_crm_lm <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-crm-lm-api-key", args,
                     "Bitte Decryption_Key fuer CRM LM eingeben: ")
}

#' authentication_GSheet
#'
#' This function executes the Google Sheet authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return no return values
authentication_GSheet <-  function(args) {
    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_google_sheets_key <-
        getPass::getPass("Enter the password for Google Sheets: ")

    } else {
      decrypt_google_sheets_key <- args
    }

    encrypted_file <-
      "../../keys/GoogleSheets/encrypted_google_sheets.bin"
    decrypted_file <-
      "../../keys/GoogleSheets/google_sheets_auth.json"

    tryCatch({
      decrypted_data <-
        safer::decrypt_file(infile = encrypted_file,
                            key = decrypt_google_sheets_key,
                            outfile = decrypted_file)
      print("Decryption successful. Data saved to google_sheets_auth.json")

      # Authentifizieren bei Google Sheets
      creds <- googlesheets4::gs4_auth(path = decrypted_file)

    },
    error = function(e) {
      # Error handling
      cat("An error occurred: ", e$message, "\n")
      print("Please check also if you have ../../keys/GoogleSheets/encrypted_google_sheets.bin")
    },
    finally = {
      # Cleanup of private key afterwards
      unlink(decrypted_file)
      print("google_sheets_auth.json deleted.")

      return("No Key")
    })
}
#' authentication_asana
#'
#' Returns `c(legacy_data_key, asana_access_token)`.
#'
#' Element `[1]` is the data key `base-02-asana_auswertung` encrypts its pipeline
#' output with and `base-18_export_billomat2sap` reads it back with. It is a
#' DIFFERENT string from billomat's - each service has its own password - so it
#' resolves to `studyflix-legacy-data-key-asana` under `gsm` and cannot be
#' collapsed into the billomat key.
#'
#' Same one-prompt threading as `authentication_billomat()`; see its note.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return `character[2]`: the legacy data key, then the Asana access token.
authentication_asana <- function(args) {
  # ---- start ---- #
  prompt <- "Enter the password for Asana: "
  key <- if (secretsR::secret_backend() == "gsm") NULL else
    billomatics_resolve_key(args, prompt)
  c(billomatics_legacy_data_key("asana", args, key = key, prompt = prompt),
    billomatics_secret("studyflix-asana-token", args, prompt, key = key))
}
#' authentication_msgraph
#'
#' Resolves the MSGraph app secret.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_msgraph <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-msgraph-secret", args,
                     "Bitte Decryption_Key fuer MSGraph eingeben: ")
}
#' authentication_msgraph_scoped_app
#'
#' Resolves the client secret of the scoped app-only MSGraph app.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_msgraph_scoped_app <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-msgraph-scoped-app-secret", args,
                     "Bitte Decryption_Key fuer MSGraph Scoped App eingeben: ")
}

#' authentication_msgraph_delegated
#'
#' Decryptet Delegated-App-Secret und Store-Key des Service-Account-Wegs.
#' @param args FlowForce-Decryption-Key.
#' @return Named list(client_secret, store_key).
authentication_msgraph_delegated <- function(args) {
  # ---- start ---- #
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key fuer MSGraph Delegated eingeben: ")
  } else {
    decrypt_key <- args
  }
  list(
    client_secret = safer::decrypt_string(readLines("../../keys/Microsoft365R/msgraph_delegated_secret.txt"), key = decrypt_key),
    store_key     = safer::decrypt_string(readLines("../../keys/Microsoft365R/msgraph_delegated_storekey.txt"), key = decrypt_key)
  )
}

#' authentication_msgraph_sharepoint
#'
#' Decryptet die Konfiguration des delegierten SharePoint-Zugriffs (n8n-App):
#' ein JSON mit tenant_id, client_id, client_secret, store_key, store_path,
#' site_url. Siehe Spec docs/superpowers/specs/2026-08-18-msgraph-sharepoint-
#' delegated-design.md.
#' @param args FlowForce-Decryption-Key.
#' @return Named list(tenant_id, client_id, client_secret, store_key,
#'   store_path, site_url).
authentication_msgraph_sharepoint <- function(args) {
  # ---- start ---- #
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key fuer MSGraph SharePoint eingeben: ")
  } else {
    decrypt_key <- args
  }
  json <- safer::decrypt_string(
    readLines("../../keys/Microsoft365R/msgraph_sharepoint.txt"), key = decrypt_key)
  auth <- jsonlite::fromJSON(json, simplifyVector = TRUE)
  required <- c("tenant_id", "client_id", "client_secret",
                "store_key", "store_path", "site_url")
  missing <- setdiff(required, names(auth))
  if (length(missing)) {
    stop("msgraph_sharepoint.txt unvollstaendig, fehlt: ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  auth
}
#' authentication_brevo
#'
#' Resolves the Brevo SMTP key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_brevo <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-brevo-smtp-key", args,
                     "Bitte Decryption_Key fuer Brevo eingeben: ")
}


#' authentication_Google_Analytics
#'
#' This function executes the Google Analytics authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return no return values
authentication_Google_Analytics <-  function(args) {
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_google_analytics_key <-
      getPass::getPass("Enter the password for Google Analytics: ")

  } else {
    decrypt_google_analytics_key <- args
  }

  encrypted_file <-
    "../../keys/GoogleAnalytics/encrypted_google_analytics.bin"
  decrypted_file <-
    "../../keys/GoogleAnalytics/google_analytics_auth.json"

  tryCatch({
    decrypted_data <-
      safer::decrypt_file(infile = encrypted_file,
                          key = decrypt_google_analytics_key,
                          outfile = decrypted_file)
    print("Decryption successful. Data saved to google_analytics_auth.json")

    # Authentifizieren bei Google Analytics
    google_analytics_auth <- googleAuthR::gar_auth_service(
      json_file = decrypted_file
    )

  },
  error = function(e) {
    # Error handling
    cat("An error occurred: ", e$message, "\n")
    print("Please check also if you have ../../keys/GoogleAnalytics/encrypted_google_analytics.bin")
  },
  finally = {
    # Cleanup of private key afterwards
    unlink(decrypted_file)
    print("google_analytics_auth.json deleted.")

    return("No Key")
  })
}
#' authentication_bonus_db
#'
#' Resolves the Bonus-DB key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_bonus_db <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-bonusdb-key", args,
                     "Bitte Decryption_Key fuer Bonus DB eingeben: ")
}

#' authentication_Google_BigQuery
#'
#' This function executes the Google_BigQuery authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return no return values
authentication_Google_BigQuery <-  function(args) {
  if (interactive()  & (length(args) == 0 | is.na(args[1]))) {
    decrypt_google_BigQuery_key <-
      getPass::getPass("Enter the password for BigQuery: ")

  } else {
    decrypt_google_BigQuery_key <- args
  }

  encrypted_file <-
    "../../keys/gsc_bigQuery/encrypted_key_service_account_bigQuery.bin"
  decrypted_file <-
    "../../keys/gsc_bigQuery/search-console-api-399013-5cb724656590.json"

  tryCatch({
    decrypted_data <-
      safer::decrypt_file(infile = encrypted_file,
                          key = decrypt_google_BigQuery_key,
                          outfile = decrypted_file)
    print("Decryption successful. Data saved to search-console-api-399013-5cb724656590.json")

    # Authentifizieren bei Google BigQuery
    google_gsc_BigQuery_auth <- bigrquery::bq_auth(path = decrypted_file)


  },
  error = function(e) {
    # Error handling
    cat("An error occurred: ", e$message, "\n")
    print("Please check also if you have ../../keys/gsc_bigQuery/encrypted_key_service_account_bigQuery.bin")
  },
  finally = {
    # Cleanup of private key afterwards
    unlink(decrypted_file)
    print(paste0(decrypted_file, " deleted."))
  })
}

#' authentication_Google_BigQuery_GA4
#'
#' This function executes the GA4 BigQuery authentication process for the
#' bigquery@ga4studyflix.iam.gserviceaccount.com service account.
#' It decrypts the encrypted key file, authenticates via googleAuthR and bigrquery,
#' verifies the connection, and deletes the decrypted file afterwards.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return no return values
authentication_Google_BigQuery_GA4 <- function(args) {
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <-
      getPass::getPass("Enter the password for BigQuery GA4: ")
  } else {
    decrypt_key <- args
  }

  encrypted_file <-
    "../../keys/ga4_bigQuery/encrypted_ga4_bigquery.bin"
  decrypted_file <-
    "../../keys/ga4_bigQuery/ga4studyflix-c43a79c8c2cb.json"

  project_id <- "ga4studyflix"

  tryCatch({
    safer::decrypt_file(
      infile  = encrypted_file,
      key     = decrypt_key,
      outfile = decrypted_file
    )
    print("Decryption successful. Data saved to ga4studyflix-c43a79c8c2cb.json")

    # Authenticate with GA4 BigQuery service account
    googleAuthR::gar_auth_service(
      json_file = decrypted_file,
      scope     = "https://www.googleapis.com/auth/bigquery"
    )
    bigrquery::bq_auth(token = googleAuthR::gar_token())

    # Verify authentication
    bigrquery::bq_project_datasets(project_id)
    message("Authentication successful — connected to '", project_id, "'.")
  },
  error = function(e) {
    cat("An error occurred: ", e$message, "\n")
    print("Please check also if you have ../../keys/ga4_bigQuery/encrypted_ga4_bigquery.bin")
  },
  finally = {
    # Cleanup of decrypted key file
    unlink(decrypted_file)
    print(paste0(decrypted_file, " deleted."))
  })
}
#' authentication_cleverreach
#'
#' Resolves the CleverReach REST API token.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_cleverreach <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-cleverreach-token", args,
                     "Bitte Decryption_Key fuer CleverReach eingeben: ")
}
#' authentication_postgresql
#'
#' Returns the connection vector `postgres_connect()` consumes:
#' `c(password, user, dbname, host, port)` - the order documented at
#' `postgres_connect.R:882-886`.
#'
#' The backends store this asymmetrically. GSM holds ONE JSON secret; the legacy
#' folder holds the password in `postgresql_key.txt` and
#' `"user, dbname, host, port"` in `postgresql_server.txt`. secretsR
#' deliberately does not compose them - doing so would mean inventing the GSM
#' payload format inside the credential package - so the composition lives here.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return `character[5]`: password, user, dbname, host, port.
authentication_postgresql <- function(args) {
  # ---- start ---- #
  gsm <- secretsR::secret_backend() == "gsm"

  if (!gsm && billomatics_interactive() && is.null(billomatics_file_key(args))) {
    # Unchanged: production credentials are not needed for local development.
    print("Postgres-Key wird lokal nicht benötigt")
    return(c("Postgres-Credentials werden lokal nicht benötigt",
             "Postgres-Server-Info wird lokal nicht benötigt"))
  }

  if (gsm) {
    conn <- billomatics_parse_json(
      secretsR::secret_get("studyflix-postgresql-connection"),
      "studyflix-postgresql-connection")
    required <- c("password", "user", "dbname", "host", "port")
    missing <- setdiff(required, names(conn))
    if (length(missing)) {
      stop("studyflix-postgresql-connection is missing: ",
           paste(missing, collapse = ", "), call. = FALSE)
    }
    return(as.character(c(conn$password, conn$user, conn$dbname,
                          conn$host, conn$port)))
  }

  prompt <- "Bitte Decryption_Key fuer PostgreSQL eingeben: "
  key <- billomatics_resolve_key(args, prompt)
  credentials <- billomatics_secret("file:postgresql-credentials", args, prompt, key = key)
  server_info <- strsplit(
    billomatics_secret("file:postgresql-server", args, prompt, key = key),
    ", ", fixed = TRUE)[[1]]
  if (length(server_info) != 4L) {
    stop(sprintf("postgresql_server.txt decrypted to %d fields, expected 4 (user, dbname, host, port)",
                 length(server_info)), call. = FALSE)
  }
  c(credentials, server_info)
}
#' authentication_gemini
#'
#' Resolves the Gemini API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_gemini <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-gemini-api-key", args,
                     "Bitte Decryption_Key fuer Gemini eingeben: ")
}
#' authentication_openrouter
#'
#' Resolves the OpenRouter API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_openrouter <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-openrouter-api-key", args,
                     "Bitte Decryption_Key fuer OpenRouter eingeben: ")
}
#' authentication_openai_admin
#'
#' Resolves the OpenAI Admin API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_openai_admin <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-openai-admin-api-key", args,
                     "Bitte Decryption_Key fuer OpenAI Admin eingeben: ")
}

#' authentication_personio
#'
#' This function handles the authentication for the Personio API.
#' It decrypts the client_id and client_secret, then requests an access token from the Personio API.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return Named list containing decrypted client_id, client_secret, and access_token
authentication_personio <- function(args) {

  encrypted_client_id <- readLines("../../keys/Personio/personio_client_id.txt")
  encrypted_client_secret <- readLines("../../keys/Personio/personio_client_secret.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für Personio eingeben: ")
  } else {
    decrypt_key <- args
  }

  # Entschlüssele client_id und client_secret
  client_id <- safer::decrypt_string(encrypted_client_id, key = decrypt_key)
  client_secret <- safer::decrypt_string(encrypted_client_secret, key = decrypt_key)

  # Erstelle den Request Body als JSON
  request_body <- jsonlite::toJSON(list(
    client_id = client_id,
    client_secret = client_secret
  ), auto_unbox = TRUE)

  # Führe den API-Call aus
  tryCatch({
    response <- httr::POST(
      url = "https://api.personio.de/v1/auth",
      httr::add_headers("Content-Type" = "application/json"),
      body = request_body,
      encode = "raw"
    )

    # Prüfe auf erfolgreichen Response
    if (httr::status_code(response) == 200) {
      response_content <- httr::content(response, as = "parsed")
      access_token <- response_content$data$token

      print("Personio authentication successful.")

      return(list(
        client_id = client_id,
        client_secret = client_secret,
        access_token = access_token
      ))
    } else {
      stop(paste("Personio API request failed with status code:", httr::status_code(response)))
    }
  },
  error = function(e) {
    cat("An error occurred during Personio authentication: ", e$message, "\n")
    stop(e)
  })
}
#' authentication_github
#'
#' Resolves the GitHub personal access token.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_github <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-github-token", args,
                     "Bitte Decryption_Key fuer GitHub eingeben: ")
}
#' authentication_metabase
#'
#' Resolves the Metabase API key.
#'
#' Under the `file` backend the password arrives through `args` exactly as
#' before; under `gsm` it is ignored and Application Default Credentials are
#' used.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The credential as a character scalar.
authentication_metabase <- function(args) {
  # ---- start ---- #
  billomatics_secret("studyflix-metabase-api-key", args,
                     "Bitte Decryption_Key fuer Metabase eingeben: ")
}
