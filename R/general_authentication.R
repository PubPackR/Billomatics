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
authentication_process <- function(needed_services = c("billomat", "crm", "google sheet","asana", "msgraph", "brevo", "google analytics", "bonusDB", "BigQuery", "cleverreach", "postgresql", "gemini", "openrouter"), args) {

  auth_functions <- list(
    billomat = authentication_billomat,
    crm = authentication_crm,
    `google sheet` = authentication_GSheet,
    asana = authentication_asana,
    msgraph = authentication_msgraph,
    brevo = authentication_brevo,
    `google analytics` = authentication_Google_Analytics,
    bonusDB = authentication_bonus_db,
    BigQuery = authentication_Google_BigQuery,
    cleverreach = authentication_cleverreach,
    postgresql = authentication_postgresql,
    gemini = authentication_gemini,
    openrouter = authentication_openrouter
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

#' @export
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

#' @export
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

#' authentication_GSheet
#'
#' This function executes the Google Sheet authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return no return values

#' @export
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


#' authentication_billomat
#'
#' This function executes the billomat authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector

#' @export
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

#' @export
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

#' authentication_brevo
#'
#' This function executes the Brevo authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector

#' @export
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

#' @export
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

#' @export
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

#' @export
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

    return("No Key")
  })
}

#' authentication_cleverReach
#'
#' Diese Funktion führt den Authentifizierungsprozess für CleverReach-RESTAPI durch.
#' Sie kann sowohl manuelle Passwort-Eingaben als auch FlowForce-Argumente verarbeiten.

#' @param args Zusätzlicher Eingabeparameter, nur erforderlich bei FlowForce-Jobs
#' @return Authentifizierungs-Token als Zeichenkette

#' @export
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

#' @export
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
#'
#' @export
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
#' This function handles the key decryption for the Gemini API authentication.
#' It supports manual decryption key input as well as FlowForce arguments.
#'
#' @param args Additional input parameter, only needed through FlowForce Job
#' @return Openrouter API Key as String
#'
#' @export
authentication_gemini <-  function(args) {

  encrypted_api_key <- readLines("../../keys/openrouter.txt")

  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key für OpenRouter eingeben: ")
  } else{
    decrypt_key <- args
  }

  safer::decrypt_string(encrypted_api_key, key = decrypt_key)
}
