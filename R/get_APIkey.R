
#' get access to Billomat
#'
#' Resolves the Billomat API key.
#'
#' Two declared behaviour changes.
#'
#' The previous implementation ran `message("key is set to: ", key)`, printing
#' the supplied password to stderr on every call - from an exported function in
#' a public repository, into any log that captured stderr. That is removed.
#'
#' `location_key` is retained for backwards compatibility and is now **ignored**:
#' the path comes from secretsR's legacy map, so a caller that passed a
#' non-default directory now reads `../../keys/billomat.txt`. Resolution also
#' goes through the seam, so this errors in a non-interactive session rather
#' than blocking on a prompt nobody can answer.
#'
#' @param location_key Deprecated and ignored; kept so existing call sites work.
#' @return The decrypted API key, invisibly.
#' @export
get_billomatApiKey <- function(location_key = NULL) {
  # ---- start ---- #
  invisible(billomatics_secret("studyflix-billomat-api-key", NULL,
                               "Enter the password for the api key: "))
}

#' get access to Billomat on server
#'
#' Resolves the Billomat API key in an unattended context.
#'
#' Off `gsm` this keeps reading `ENCRYPTION_PAYLOAD` and `ENCRYPTION_SECRET` from
#' the environment. Those are live **GitHub Actions** secrets and a CI runner has
#' no `keys/` checkout, so routing this through the file backend would turn a
#' working function into one that always errors. The environment path retires
#' when CI gains a `gsm` identity through Workload Identity Federation - the two
#' changes belong together, and separating them breaks deployment.
#'
#' @return The decrypted API key, invisibly.
#' @export
get_billomatApiKey_server <- function() {
  # ---- start ---- #
  if (secretsR::secret_backend() == "gsm") {
    return(invisible(secretsR::secret_get("studyflix-billomat-api-key")))
  }
  payload <- Sys.getenv("ENCRYPTION_PAYLOAD")
  key <- Sys.getenv("ENCRYPTION_SECRET")
  if (!nzchar(payload) || !nzchar(key)) {
    stop("get_billomatApiKey_server(): ENCRYPTION_PAYLOAD/ENCRYPTION_SECRET are unset and the backend is not 'gsm'",
         call. = FALSE)
  }
  invisible(safer::decrypt_string(payload, key = key))
}
