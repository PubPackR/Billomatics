
#' get access to Billomat
#'
#' this function authenticates at billomat. You will need to enter your created password
#'
#' @param location_key this is the location where the key is found as txt file,
#' just put it above in the main folder and call it with location_key <- "../key"
#' @return the call returns the decrypted key
#' @export
get_billomatApiKey <- function(location_key) {
  key = readline(prompt = "decryption_key: \n")
  message("key is set to: ", key)
  encrypted_ApiKey <- readLines(paste0(location_key,"/billomat.txt"))
  billomatApiKey <- safer::decrypt_string(encrypted_ApiKey, key=key)
}



#' get access to Billomat on server
#'
#' this function authenticates at billomat. You will need to enter your created password
#'
#' @param location_key this is the location where the key is found as txt file,
#' just put it above in the main folder and call it with location_key <- "../key"
#' @return the call returns the decrypted key
#' @export
get_billomatApiKey_server <- function() {
  encrypted_ApiKey <- Sys.getenv("ENCRYPTION_PAYLOAD")
  key <- Sys.getenv("ENCRYPTION_SECRET")
  billomatApiKey <- safer::decrypt_string(encrypted_ApiKey, key=key)
}
