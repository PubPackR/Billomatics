# -------------------------- Start script --------------------------------

library(safer)
library(tidyverse)
library(googlesheets4)

#' authentication_process
#'
#' This function executes the requested authentication processes.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param needed_services the authentication services you need as vector. Currently available: "Billomat", "CRM" & "Google Sheet"
#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param path_api_key path to api_key decription key
#' @return authentication keys as vector

#' @export
authentication_process <- function(needed_services = c("Billomat", "CRM", "Google Sheet"), args, path_api_key = "") {

  return_keys <- c()

  # 1 Authentication Billomat ----

  pos_Billomat <- match(1, str_detect("Billomat", needed_services))

  if(!is.na(pos_Billomat)) {
    return_keys <- authentication_billomat(args[pos_Billomat], return_keys)
  }

  # 2 Authentication CRM ----

  pos_CRM <- match(1, str_detect("CRM", needed_services))

  if(!is.na(pos_CRM)) {
    return_keys <- authentication_crm(args[pos_CRM], path_api_key, return_keys)
  }

  # 3 Authentication Google Sheet ----

  pos_GSheet <- match(1, str_detect("Google Sheet", needed_services))

  if(!is.na(pos_GSheet)) {
    authentication_GSheet(args[pos_GSheet])
  }

  return(return_keys)

}

#' authentication_billomat
#'
#' This function executes the billomat authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector

#' @export
authentication_billomat <-  function(args, return_keys = c()) {

    if (interactive()) {
      encryption_db <-
        getPass::getPass("Enter the password for Billomat-DB: ")

    } else {
      encryption_db <- args
    }

    return_keys <- c(return_keys, encryption_db)
}

#' authentication_crm
#'
#' This function executes the CRM authentication process.
#' It can handle manual password inputs as well as Flow Force args Inputs.

#' @param args Additional Input Parameter, only needed through FlowForce Job
#' @param path_api_key path to api_key decription key
#' @param return_keys optional, vector with already acquired keys
#' @return authentication key in vector

#' @export
authentication_crm <-  function(args, path_api_key, return_keys = c()) {

    encrypted_api_key <- readLines(path_api_key)

    if (interactive()) {
      decrypt_key <-
        getPass::getPass("Bitte Decryption_Key für CRM eingeben: ")
    } else{
      decrypt_key <- args
    }

    return_keys <- c(return_keys, decrypt_string(encrypted_api_key, key = decrypt_key))
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
