#' Seam around interactive(), so tests can mock it
#'
#' local_mocked_bindings(.package = "base") does NOT work on interactive():
#' R's byte-compiler emits GETBUILTIN.OP, fetching the builtin directly and
#' bypassing the namespace rebinding a mock installs. Package code is always
#' byte-compiled, so the mock is silently a no-op - the test then runs with the
#' real interactive() == FALSE, falls through to getPass(), and BLOCKS ON STDIN
#' FOREVER: green in RStudio, hanging in CI. Confirmed by disassembly; this seam
#' compiles to GETFUN.OP, which is mockable.
#'
#' @return TRUE in an interactive session.
#' @noRd
billomatics_interactive <- function() {
  # ---- start ---- #
  interactive()
}

#' Fail with an actionable message when an optional Google package is absent
#'
#' bigrquery is a Suggests: it is not installed on every host, and a hard
#' Imports would make installing Billomatics fail there entirely.
#'
#' @param pkg Package name.
#' @return NULL, invisibly. Errors if the package is unavailable.
#' @noRd
billomatics_require <- function(pkg) {
  # ---- start ---- #
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("'%s' is required for this authentication service but is not installed", pkg),
         call. = FALSE)
  }
  invisible(NULL)
}

#' Normalise the positional password argument to a character scalar
#'
#' authentication_process() calls auth_functions[[service]](args[pos]), and args
#' arrives as a character vector from commandArgs() in FlowForce and as a list
#' from shinymanager::custom_access_keys_2() in Shiny. args[pos] is therefore
#' character[1] in one case and a length-1 LIST in the other - and list(NULL)
#' when needed_services is longer than the supplied keys. secret_get() validates
#' file_key as a character scalar, so the shape is flattened once here rather
#' than at 22 call sites.
#'
#' @param args Whatever authentication_process() passed through.
#' @return A character scalar, or NULL when no usable key was supplied.
#' @noRd
billomatics_file_key <- function(args) {
  # ---- start ---- #
  if (is.null(args)) return(NULL)
  if (is.list(args)) args <- unlist(args, use.names = FALSE)
  if (length(args) == 0L) return(NULL)
  key <- as.character(args)[1]
  if (is.na(key) || !nzchar(key)) return(NULL)
  key
}

#' Resolve the file-backend password once
#'
#' Separate from billomatics_secret() so a function needing several secrets
#' under one password prompts once and threads the result. For billomat and
#' asana that is a correctness requirement rather than a convenience: element
#' [1] of their return value and the credential itself must derive from the same
#' password, and an operator typing differently on a second prompt would
#' otherwise produce a data key that silently does not match.
#'
#' @param args The positional argument from authentication_process().
#' @param prompt Text shown when a password is needed interactively.
#' @return The password as a character scalar.
#' @noRd
billomatics_resolve_key <- function(args, prompt = "Enter the decryption password: ") {
  # ---- start ---- #
  key <- billomatics_file_key(args)
  if (!is.null(key)) return(key)
  if (!billomatics_interactive()) {
    stop("no decryption password supplied and the session is not interactive",
         call. = FALSE)
  }
  getPass::getPass(prompt)
}

#' Resolve one secret
#'
#' Under gsm there is no password to enter - authentication is Application
#' Default Credentials - so the interactive prompt is skipped entirely. `key`
#' lets a caller hoist the prompt when it needs more than one secret.
#'
#' @param name Canonical secret name (see the GSM IAM matrix).
#' @param args The positional argument from authentication_process().
#' @param prompt Text shown when a password is needed interactively.
#' @param key An already-resolved password, or NULL to resolve one.
#' @return The secret as a character scalar.
#' @noRd
billomatics_secret <- function(name, args,
                               prompt = "Enter the decryption password: ",
                               key = NULL) {
  # ---- start ---- #
  if (secretsR::secret_backend() == "gsm") {
    return(secretsR::secret_get(name))
  }
  if (is.null(key)) key <- billomatics_resolve_key(args, prompt)
  secretsR::secret_get(name, file_key = key)
}

#' Parse a JSON secret without putting it in the error message
#'
#' jsonlite's lexer error embeds its input verbatim:
#'   fromJSON("SECRET-abc") -> 'lexical error: invalid char ... SECRET-abc'
#' and on these calls the input IS a credential, so an unattended job would log
#' it in full. Every fromJSON() applied to a secret goes through here.
#'
#' @param json The raw secret.
#' @param name Secret name, for the message.
#' @return The parsed object.
#' @noRd
billomatics_parse_json <- function(json, name) {
  # ---- start ---- #
  tryCatch(
    jsonlite::fromJSON(json, simplifyVector = TRUE),
    error = function(e) {
      stop(sprintf("secret '%s' is not valid JSON", name), call. = FALSE)
    }
  )
}

#' The legacy data-encryption key
#'
#' Under file this is the password the caller supplied - there is no keys/ file
#' holding it, because it IS the key those files are encrypted with. Under gsm
#' it is a stored secret, so call sites reading keys$billomat[1] to decrypt
#' base-data/ keep working after cutover.
#'
#' There are TWO. A sweep of every encrypt_object()/decrypt_object() call site
#' across the organisation (2026-08-20) found ~49 passing keys$billomat[1] and 9
#' passing keys$asana[1]: base-02-asana_auswertung writes its pipeline output
#' under the Asana password and base-18 reads it back. Those are different
#' strings, so a single key would leave base-02's data unreadable.
#'
#' @param which "billomat" or "asana".
#' @param args The positional argument from authentication_process().
#' @param key An already-resolved password, or NULL to resolve one.
#' @param prompt Text shown when a password is needed interactively.
#' @return The data key as a character scalar.
#' @noRd
billomatics_legacy_data_key <- function(which, args, key = NULL,
                                        prompt = "Enter the password: ") {
  # ---- start ---- #
  if (!which %in% c("billomat", "asana")) {
    stop(sprintf("unknown legacy data key: '%s'", which), call. = FALSE)
  }
  if (secretsR::secret_backend() == "gsm") {
    # Constructed rather than literal; the two full names are
    # studyflix-legacy-data-key-billomat and studyflix-legacy-data-key-asana.
    return(secretsR::secret_get(paste0("studyflix-legacy-data-key-", which)))
  }
  if (is.null(key)) key <- billomatics_resolve_key(args, prompt)
  key
}

#' A private directory for a decrypted service-account key
#'
#' Separate so tests can mock it. mode = "0700" is ignored on Windows, so the
#' guarantee holds only on the server - which is where it matters, and where the
#' umask is 0002 and would otherwise leave the plaintext world-readable.
#'
#' @return Path to a newly created 0700 directory.
#' @noRd
billomatics_sa_dir <- function() {
  # ---- start ---- #
  d <- file.path(tempdir(), paste0("sa", Sys.getpid(), basename(tempfile())))
  if (!dir.create(d, mode = "0700")) {
    stop("could not create a private directory for the service-account key",
         call. = FALSE)
  }
  d
}

#' Resolve a service-account JSON document as a string
#'
#' The four Google services store a safer::encrypt_file blob, not an encrypted
#' string, so secret_get() cannot serve them on the file backend: binary
#' payloads are out of scope there and secretsR's legacy map omits them
#' deliberately. Under gsm the JSON is an ordinary UTF-8 string secret.
#'
#' The file branch must NOT pre-create the output file: safer::decrypt_file()
#' asserts !file.exists(outfile) and fails outright. It also opens the file
#' itself, so the mode is 0666 & ~umask - 0664 on the server, i.e. a
#' world-readable plaintext private key. The containing directory is the only
#' place the mode can be set. Deleted with the file backend in Plan F.
#'
#' @param name Canonical secret name.
#' @param args The positional argument from authentication_process().
#' @param prompt Text shown when a password is needed interactively.
#' @return The service-account JSON as a single character scalar.
#' @noRd
billomatics_sa_json <- function(name, args,
                                prompt = "Enter the decryption password: ") {
  # ---- start ---- #
  if (secretsR::secret_backend() == "gsm") {
    return(secretsR::secret_get(name))
  }
  key <- billomatics_resolve_key(args, prompt)
  infile <- billomatics_sa_encrypted_path(name)
  if (!file.exists(infile)) {
    stop(sprintf("service-account file not found: %s (cwd: %s)", infile, getwd()),
         call. = FALSE)
  }
  dir <- billomatics_sa_dir()
  on.exit(unlink(dir, recursive = TRUE, force = TRUE), add = TRUE)
  outfile <- file.path(dir, "sa.json")
  safer::decrypt_file(infile = infile, key = key, outfile = outfile)
  paste(readLines(outfile, warn = FALSE), collapse = "\n")
}

#' Encrypted-file paths for the four service-account services
#'
#' Not in secretsR's legacy map: that map serves secret_get(), which reads
#' decrypt_string payloads only. These are encrypt_file blobs.
#'
#' @param name Canonical secret name.
#' @return Relative path to the encrypted .bin file.
#' @noRd
billomatics_sa_encrypted_path <- function(name) {
  # ---- start ---- #
  paths <- list(
    "studyflix-gsheets-service-account"          = "../../keys/GoogleSheets/encrypted_google_sheets.bin",
    "studyflix-google-analytics-service-account" = "../../keys/GoogleAnalytics/encrypted_google_analytics.bin",
    "studyflix-bigquery-gsc-service-account"     = "../../keys/gsc_bigQuery/encrypted_key_service_account_bigQuery.bin",
    "studyflix-bigquery-ga4-service-account"     = "../../keys/ga4_bigQuery/encrypted_ga4_bigquery.bin"
  )
  if (!name %in% names(paths)) {
    stop(sprintf("unknown service-account secret: '%s'", name), call. = FALSE)
  }
  paths[[name]]
}
