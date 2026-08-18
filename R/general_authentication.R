# -------------------------- Start script --------------------------------

library(safer)
library(tidyverse)
library(googlesheets4)
library(googleAuthR)

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
#' This function executes the billomat authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_billomat <-  function(args) {

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {

      encryption_db <-
        getPass::getPass("Enter the password for Billomat-DB: ")
      billomatApiKey <-
        safer::decrypt_string(readLines("../../keys/billomat.txt"), key = encryption_db)

    } else {

      encryption_db <- args
      billomatApiKey <-
        safer::decrypt_string(readLines("../../keys/billomat.txt"), key = encryption_db)
    }

    c(encryption_db, billomatApiKey)
}

#' authentication_crm
#'
#' This function executes the CRM authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_crm <-  function(args) {

    encrypted_api_key <- readLines("../../keys/CRM.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_key <-
        getPass::getPass("Bitte Decryption_Key für CRM eingeben: ")
    } else{
      decrypt_key <- args
    }

    safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}

#' authentication_crm_lm
#'
#' This function executes the CRM LM authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_crm_lm <-  function(args) {

    encrypted_api_key <- readLines("../../keys/CRM_LM.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_key <-
        getPass::getPass("Bitte Decryption_Key für CRM LM eingeben: ")
    } else{
      decrypt_key <- args
    }

    safer::decrypt_string(encrypted_api_key, key = decrypt_key)
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
#' This function executes the Asana authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.
#'
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_asana <-  function(args) {

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {

    asana_key <-
      getPass::getPass("Enter the password for Asana: ")
    asana_access_token <-
      safer::decrypt_string(readLines("../../keys/asana.txt"), key = asana_key)

  } else {

    asana_key <- args
    asana_access_token <-
      safer::decrypt_string(readLines("../../keys/asana.txt"), key = asana_key)
  }

  c(asana_key, asana_access_token)
}


#' authentication_msgraph
#'
#' This function executes the MSGraph authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_msgraph <-  function(args) {

  encrypted_api_key <- readLines("../../keys/Microsoft365R/microsoft365r.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <-
      getPass::getPass("Bitte Decryption_Key für MSGraph eingeben: ")
  } else{
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}

#' authentication_msgraph_scoped_app
#'
#' Decryptet das Client-Secret der gescopten app-only-MSGraph-App (neuer Weg).
#' @param args FlowForce-Decryption-Key.
#' @return App-Secret als String.
authentication_msgraph_scoped_app <- function(args) {
  # ---- start ---- #
  encrypted_api_key <- readLines("../../keys/Microsoft365R/msgraph_scoped_app.txt")
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key fuer MSGraph Scoped App eingeben: ")
  } else {
    decrypt_key <- args
  }
  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
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
#' This function executes the Brevo authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_brevo <-  function(args) {

    encrypted_api_key <- readLines("../../keys/Brevo/smpt-key.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_key <-
        getPass::getPass("Bitte Decryption_Key für Brevo eingeben: ")
    } else{
      decrypt_key <- args
    }

    safer::decrypt_string(encrypted_api_key, key = decrypt_key)
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
#' This function executes the Bonus DB authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector
authentication_bonus_db <-  function(args) {

    encrypted_api_key <- readLines("../../keys/BonusDB/bonusDBKey.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_key <-
        getPass::getPass("Bitte Decryption_Key für Bonus DB eingeben: ")
    } else{
      decrypt_key <- args
    }

    safer::decrypt_string(encrypted_api_key, key = decrypt_key)
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

#' authentication_cleverReach
#'
#' Diese Funktion führt den Authentifizierungsprozess für CleverReach-RESTAPI durch.
#' Sie kann sowohl manuelle Passwort-Eingaben als auch FlowForce-Argumente verarbeiten.

#' @param args Zusätzlicher Eingabeparameter, nur erforderlich bei FlowForce-Jobs
#' @return Authentifizierungs-Token als Zeichenkette
authentication_cleverreach <- function(args) {
    encrypted_api_key <- readLines("../../keys/cleverReach_key.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      decrypt_key <- getPass::getPass("Bitte Decryption_Key für CleverReach eingeben: ")
    } else {
      decrypt_key <- args
    }

    safer::decrypt_string(encrypted_api_key, key = decrypt_key)

}


#' authentication_postgresql
#'
#' This function handles the key encryption for a PostgreSQL database authentication.
#' It supports manual password input as well as FlowForce arguments.

#' @param args Additional input parameter, only needed through FlowForce Job
#' @return PostgreSQL DB Key as String
authentication_postgresql <- function(args) {
    encrypted_credentials <- readLines("../../keys/PostgreSQL_DB/postgresql_key.txt")
    encrypted_server_info <- readLines("../../keys/PostgreSQL_DB/postgresql_server.txt")

    if (interactive() & (length(args) == 0 | is.na(args[1]))) {
      #decrypt_key <- getPass::getPass("Bitte Decryption_Key für PostgreSQL eingeben: ")
      print("Postgres-Key wird lokal nicht benötigt")
      return(c("Postgres-Credentials werden lokal nicht benötigt", "Postgres-Server-Info wird lokal nicht benötigt"))
    } else {
      decrypt_key <- args

      credentials <- safer::decrypt_string(encrypted_credentials, key = decrypt_key)
      server_info <- (safer::decrypt_string(encrypted_server_info, key = decrypt_key) %>% strsplit(", "))[[1]]

      return(c(credentials, server_info))
    }

}

#' authentication_gemini
#'
#' This function handles the key decryption for the Gemini API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return Gemini API Key as String
authentication_gemini <-  function(args) {

  encrypted_api_key <- readLines("../../keys/gemini_key.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für Gemini eingeben: ")
  } else{
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}

#' authentication_openrouter
#'
#' This function handles the key decryption for the OpenRouter API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return Openrouter API Key as String
authentication_openrouter <-  function(args) {

  encrypted_api_key <- readLines("../../keys/openrouter.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für OpenRouter eingeben: ")
  } else{
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}

#' authentication_openai_admin
#'
#' This function handles the key decryption for the OpenAI Admin API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return OpenAI Admin API Key as String
authentication_openai_admin <-  function(args) {

  encrypted_api_key <- readLines("../../keys/openai_admin.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für OpenAI Admin eingeben: ")
  } else{
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
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
#' This function handles the key decryption for the GitHub API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return GitHub Personal Access Token as String
authentication_github <- function(args) {

  encrypted_api_key <- readLines("../../keys/Github/github_token.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für GitHub eingeben: ")
  } else {
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}

#' authentication_metabase
#'
#' This function handles the key decryption for the Metabase API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return Metabase API Key as String
authentication_metabase <- function(args) {

  encrypted_api_key <- readLines("../../keys/metabase.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für Metabase eingeben: ")
  } else {
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}
