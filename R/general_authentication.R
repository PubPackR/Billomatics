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
#' Authenticates googlesheets4 with the Google Sheets service account.
#'
#' Behaviour change, deliberate and the only one in this migration. This
#' function used to report success to the caller whatever happened - including
#' a failed decrypt - so a job continued unauthenticated and the fault surfaced
#' later as an unrelated API error, or not at all if a cached token was still
#' valid. Errors now propagate, so an unattended job crashes loudly.
#'
#' It returns the sentinel "OK" rather than NULL because
#' `authentication_process()` does `keys[[service]] <- <result>`, and assigning
#' NULL REMOVES the element - a 22-service call would come back with 18 entries
#' and every caller using length() or names() would change behaviour silently.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The string "OK". Signals an error if authentication fails.
authentication_GSheet <- function(args) {
  # ---- start ---- #
  json <- billomatics_sa_json("studyflix-gsheets-service-account", args,
                              "Enter the password for Google Sheets: ")
  googlesheets4::gs4_auth(path = json)
  "OK"
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
#' Decryptet Delegated-App-Secret und Store-Key des Service-Account-Wegs. Beide
#' teilen sich unter dem `file`-Backend ein Passwort, das einmal aufgeloest und
#' an beide Aufrufe durchgereicht wird.
#'
#' @param args FlowForce-Decryption-Key.
#' @return Named list(client_secret, store_key).
authentication_msgraph_delegated <- function(args) {
  # ---- start ---- #
  prompt <- "Bitte Decryption_Key fuer MSGraph Delegated eingeben: "
  key <- if (secretsR::secret_backend() == "gsm") NULL else
    billomatics_resolve_key(args, prompt)
  list(
    client_secret = billomatics_secret("studyflix-msgraph-delegated-secret", args, prompt, key = key),
    store_key     = billomatics_secret("studyflix-msgraph-delegated-storekey", args, prompt, key = key)
  )
}
#' authentication_msgraph_sharepoint
#'
#' Decryptet die Konfiguration des delegierten SharePoint-Zugriffs (n8n-App):
#' ein JSON mit tenant_id, client_id, client_secret, store_key, store_path,
#' site_url. Unter dem `file`-Backend liegt das JSON als verschluesselter
#' STRING, nicht als verschluesselte Datei, also ist es ein gewoehnliches
#' `secret_get()`-Secret trotz strukturierter Inhalte.
#'
#' @param args FlowForce-Decryption-Key.
#' @return Named list(tenant_id, client_id, client_secret, store_key,
#'   store_path, site_url).
authentication_msgraph_sharepoint <- function(args) {
  # ---- start ---- #
  json <- billomatics_secret("studyflix-msgraph-sharepoint-config", args,
                             "Bitte Decryption_Key fuer MSGraph SharePoint eingeben: ")
  auth <- billomatics_parse_json(json, "studyflix-msgraph-sharepoint-config")
  required <- c("tenant_id", "client_id", "client_secret",
                "store_key", "store_path", "site_url")
  missing <- setdiff(required, names(auth))
  if (length(missing)) {
    # Wording kept close to the original so a caller matching on
    # "unvollstaendig" keeps working.
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
#' Authenticates googleAuthR with the Google Analytics service account.
#'
#' Behaviour change, deliberate and the only one in this migration. This
#' function used to report success to the caller whatever happened - including
#' a failed decrypt - so a job continued unauthenticated and the fault surfaced
#' later as an unrelated API error, or not at all if a cached token was still
#' valid. Errors now propagate, so an unattended job crashes loudly.
#'
#' It returns the sentinel "OK" rather than NULL because
#' `authentication_process()` does `keys[[service]] <- <result>`, and assigning
#' NULL REMOVES the element - a 22-service call would come back with 18 entries
#' and every caller using length() or names() would change behaviour silently.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The string "OK". Signals an error if authentication fails.
authentication_Google_Analytics <- function(args) {
  # ---- start ---- #
  json <- billomatics_sa_json("studyflix-google-analytics-service-account", args,
                              "Enter the password for Google Analytics: ")
  googleAuthR::gar_auth_service(json_file = json)
  "OK"
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
#' Authenticates bigrquery with the search-console service account.
#' Previously returned its own error message - a 96-character string - where
#' a credential belongs.
#'
#' Behaviour change, deliberate and the only one in this migration. This
#' function used to report success to the caller whatever happened - including
#' a failed decrypt - so a job continued unauthenticated and the fault surfaced
#' later as an unrelated API error, or not at all if a cached token was still
#' valid. Errors now propagate, so an unattended job crashes loudly.
#'
#' It returns the sentinel "OK" rather than NULL because
#' `authentication_process()` does `keys[[service]] <- <result>`, and assigning
#' NULL REMOVES the element - a 22-service call would come back with 18 entries
#' and every caller using length() or names() would change behaviour silently.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The string "OK". Signals an error if authentication fails.
authentication_Google_BigQuery <- function(args) {
  # ---- start ---- #
  # Checked before the decrypt, so a credential is not decrypted just to be
  # discarded when the package turns out to be absent.
  billomatics_require("bigrquery")
  json <- billomatics_sa_json("studyflix-bigquery-gsc-service-account", args,
                              "Enter the password for BigQuery: ")
  bigrquery::bq_auth(path = json)
  "OK"
}
#' authentication_Google_BigQuery_GA4
#'
#' Authenticates bigrquery for the ga4studyflix project, then verifies the
#' connection. Previously returned NULL on success and a non-NULL string on
#' failure - inverted, so the natural `is.null()` guard aborted on success
#' and continued on failure.
#'
#' Behaviour change, deliberate and the only one in this migration. This
#' function used to report success to the caller whatever happened - including
#' a failed decrypt - so a job continued unauthenticated and the fault surfaced
#' later as an unrelated API error, or not at all if a cached token was still
#' valid. Errors now propagate, so an unattended job crashes loudly.
#'
#' It returns the sentinel "OK" rather than NULL because
#' `authentication_process()` does `keys[[service]] <- <result>`, and assigning
#' NULL REMOVES the element - a 22-service call would come back with 18 entries
#' and every caller using length() or names() would change behaviour silently.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return The string "OK". Signals an error if authentication fails.
authentication_Google_BigQuery_GA4 <- function(args) {
  # ---- start ---- #
  billomatics_require("bigrquery")
  json <- billomatics_sa_json("studyflix-bigquery-ga4-service-account", args,
                              "Enter the password for BigQuery GA4: ")
  googleAuthR::gar_auth_service(
    json_file = json,
    scope     = "https://www.googleapis.com/auth/bigquery"
  )
  bigrquery::bq_auth(token = googleAuthR::gar_token())
  # Retained from the original: the only one of the four that proves the
  # credential works rather than merely parses.
  bigrquery::bq_project_datasets("ga4studyflix")
  "OK"
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
  # ---- start ---- #
  prompt <- "Bitte Decryption_Key fuer Personio eingeben: "
  key <- if (secretsR::secret_backend() == "gsm") NULL else
    billomatics_resolve_key(args, prompt)
  client_id     <- billomatics_secret("studyflix-personio-client-id", args, prompt, key = key)
  client_secret <- billomatics_secret("studyflix-personio-client-secret", args, prompt, key = key)

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
