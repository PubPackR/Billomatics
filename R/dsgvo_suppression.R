#' Normalisiert eine E-Mail-Adresse (lowercase + trim)
#' @param email Character (Skalar/Vektor).
#' @return Character; NA bleibt NA.
#' @export
dsgvo_normalize_email <- function(email) {
  # ---- start ---- #
  ifelse(is.na(email), NA_character_, tolower(trimws(email)))
}

#' Normalisiert eine Telefonnummer auf kanonische Ziffernform
#' @param phone Character (Skalar/Vektor).
#' @return Character; NA bleibt NA.
#' @export
dsgvo_normalize_phone <- function(phone) {
  # ---- start ---- #
  s <- gsub("[^0-9+]", "", phone)
  s <- sub("^\\+", "", s)
  s <- sub("^00", "", s)
  s <- sub("^0", "49", s)
  s <- sub("^490", "49", s)
  ifelse(is.na(phone), NA_character_, s)
}

#' Gepfefferter, deterministischer SHA-256 eines normalisierten Strings
#' @param x Bereits normalisierter Character.
#' @param pepper Pepper-Geheimnis.
#' @return 64-Hex je Element; NA bleibt NA.
#' @keywords internal
dsgvo_hash_peppered <- function(x, pepper) {
  # ---- start ---- #
  vapply(x, function(v) {
    if (is.na(v)) return(NA_character_)
    digest::digest(paste0(v, pepper), algo = "sha256", serialize = FALSE)
  }, character(1), USE.NAMES = FALSE)
}

#' Gepfefferter SHA-256-Hash einer E-Mail (normalisiert die Eingabe)
#' @param email Roh-E-Mail (wird via dsgvo_normalize_email normalisiert).
#' @param pepper Pepper-Geheimnis.
#' @return 64-Hex je Element; NA bleibt NA.
#' @export
dsgvo_hash_email <- function(email, pepper) {
  # ---- start ---- #
  dsgvo_hash_peppered(dsgvo_normalize_email(email), pepper)
}

#' Gepfefferter SHA-256-Hash einer Telefonnummer (normalisiert die Eingabe)
#' @param phone Roh-Telefonnummer (wird via dsgvo_normalize_phone normalisiert).
#' @param pepper Pepper-Geheimnis.
#' @return 64-Hex je Element; NA bleibt NA.
#' @export
dsgvo_hash_phone <- function(phone, pepper) {
  # ---- start ---- #
  dsgvo_hash_peppered(dsgvo_normalize_phone(phone), pepper)
}

#' Personen-stabiler Tombstone aus einem E-Mail-Hash
#' @param email_hash Gepfefferter SHA-256 der E-Mail.
#' @return Tombstone-String `[geloescht]-<hash>`.
#' @export
dsgvo_email_tombstone <- function(email_hash) {
  # ---- start ---- #
  paste0("[geloescht]-", email_hash)
}

#' Liest das Pepper-Geheimnis (ENV -> Key-Datei-Fallback)
#' @param env_var Name der ENV-Variable.
#' @param key_file Optionaler Pfad (Fallback). Erste Zeile, getrimmt.
#' @return Character mit dem Pepper. Fehler, wenn weder ENV noch Datei etwas liefert.
#' @export
get_deletion_pepper <- function(env_var = "DELETION_LOG_PEPPER", key_file = NULL) {
  # ---- start ---- #
  pepper <- Sys.getenv(env_var, unset = "")
  if (nzchar(pepper)) return(pepper)
  if (!is.null(key_file)) {
    path <- path.expand(key_file)
    if (file.exists(path)) {
      pepper <- trimws(readLines(path, n = 1, warn = FALSE))
      if (nzchar(pepper)) return(pepper)
    }
  }
  stop(sprintf("Pepper-Geheimnis fehlt: weder ENV '%s' gesetzt noch nicht-leere Datei unter key_file (%s).",
               env_var, if (is.null(key_file)) "nicht angegeben" else key_file))
}
