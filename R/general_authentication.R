# -------------------------- Start script --------------------------------

library(safer)
library(tidyverse)
library(googlesheets4)

#' authentication_process
#'
#' This function executes the requested authentication processes.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param needed_services the authentication services you need as vector. Currently available: "Billomat", "CRM", "Google Sheet" & "Asana"
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @return authentication keys as vector

#' @export
authentication_process <- function(needed_services = c("billomat", "crm", "google sheet","asana"), args) {

  # 1 Authentication Billomat ----

  pos_Billomat <- match(1, stringr::str_detect("billomat", needed_services))

  if(!is.na(pos_Billomat)) {
    billomat_key <- authentication_billomat(args[pos_Billomat])
  } else {
    billomat_key <- NA
  }

  # 2 Authentication CRM ----

  pos_CRM <- match(1, stringr::str_detect("crm", needed_services))

  if(!is.na(pos_CRM)) {
    crm_key <- authentication_crm(args[pos_CRM])
  } else {
    crm_key <- NA
  }

  # 3 Authentication Google Sheet ----

  pos_GSheet <- match(1, stringr::str_detect("google sheet", needed_services))

  if(!is.na(pos_GSheet)) {
    authentication_GSheet(args[pos_GSheet])
  }

  # 4 Authentication Asana ---

  pos_Asana <- match(1, stringr::str_detect("asana", needed_services))

  if(!is.na(pos_Asana)) {
    asana_key <- authentication_asana(args[pos_Asana])
  } else {
    asana_key <- NA
  }

  keys  <- list("billomat" = billomat_key, "crm" = crm_key, "asana" = asana_key)

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

    if (interactive()) {

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

    if (interactive()) {
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
    if (interactive()) {
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
    },
    finally = {
      # Cleanup of private key afterwards
      unlink(decrypted_file)
      print("google_sheets_auth.json deleted.")
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

  if (interactive()) {

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
