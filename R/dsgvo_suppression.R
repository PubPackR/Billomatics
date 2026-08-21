#' Normalisiert eine E-Mail-Adresse (lowercase + trim; MS-Graph #EXT#-UPN -> echte Mail)
#'
#' Reduziert die MS-Graph-UPN-Foederationsform (z.B.
#' john.doe_gmail.com#EXT#@tenant.onmicrosoft.com) auf die echte Adresse
#' (john.doe@gmail.com) — identisch zur Normalisierung im Kontakt-Write
#' (update_contacts_from_calls), damit Writer (Loeschung) und Reader (Ingest-Suppression)
#' denselben Hash erzeugen.
#'
#' @param email Character (Skalar/Vektor).
#' @return Character; NA bleibt NA.
#' @export
dsgvo_normalize_email <- function(email) {
  # ---- start ---- #
  e <- tolower(trimws(email))
  e <- ifelse(grepl("#ext#", e, ignore.case = TRUE),
              sub("_", "@", sub("(?i)#ext#.*", "", e), fixed = TRUE), e)
  ifelse(is.na(email), NA_character_, e)
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

#' Liest das Pepper-Geheimnis (Secret Manager -> ENV -> Key-Datei-Fallback)
#'
#' Unter dem `gsm`-Backend kommt der Pepper aus Secret Manager. Die bisherigen
#' Fallbacks bleiben fuer `file` und die lokale Entwicklung erhalten, damit
#' dieser Wechsel nichts bricht.
#'
#' Ein falscher Pepper scheitert **lautlos**: die Hashes stimmen nicht mehr,
#' `dsgvo_load_suppression()` findet keine Treffer, Datensaetze von
#' Loesch-Betroffenen werden ohne Fehler wieder eingelesen. Der Wert muss bei
#' der Migration daher verifiziert werden, nicht nur kopiert.
#'
#' @param env_var Name der ENV-Variable.
#' @param key_file Optionaler Pfad (Fallback). Erste Zeile, getrimmt.
#' @return Character mit dem Pepper. Fehler, wenn keine Quelle etwas liefert.
#' @export
get_deletion_pepper <- function(env_var = "DELETION_LOG_PEPPER", key_file = NULL) {
  # ---- start ---- #
  if (secretsR::secret_backend() == "gsm") {
    return(secretsR::secret_get("studyflix-deletion-log-pepper"))
  }
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

#' Lädt die Sperrlisten-Hashes aus config.privacy_deletion_log
#' @param con DB-Verbindung.
#' @param table Voll qualifizierter Tabellenname.
#' @return list(email_hashes, phone_hashes) als Character-Vektoren (dedupliziert).
#' @export
dsgvo_load_suppression <- function(con, table = "config.privacy_deletion_log") {
  # ---- start ---- #
  raw <- dplyr::collect(dplyr::select(dplyr::tbl(con, I(table)), "email_hash", "phone_hashes"))
  emails <- unique(raw$email_hash[!is.na(raw$email_hash) & nzchar(raw$email_hash)])
  phones <- unique(unlist(lapply(raw$phone_hashes, dsgvo_parse_pg_array)))
  list(email_hashes = as.character(emails), phone_hashes = as.character(phones))
}

#' Parst ein PG-Array-Literal ('{a,b}') zu einem Character-Vektor (leere/NA -> character(0))
#' @param s Ein '{...}'-String ODER bereits ein Vektor.
#' @return Character-Vektor.
#' @keywords internal
dsgvo_parse_pg_array <- function(s) {
  # ---- start ---- #
  if (length(s) != 1) return(as.character(s[!is.na(s)]))
  if (is.na(s) || !nzchar(s)) return(character(0))
  inner <- sub("\\}$", "", sub("^\\{", "", s))
  if (!nzchar(inner)) return(character(0))
  trimws(strsplit(inner, ",", fixed = TRUE)[[1]])
}

#' Ersetzt PII gesperrter Personen in einem MS-Graph-Record (calls ODER events/booking)
#'
#' Setzt die Mail-Spalte gesperrter E-Mails auf den stabilen Tombstone und die Name-Spalte
#' auf NA. Reine Transformation (kein DB-Zugriff). Keine Zeile wird entfernt ->
#' Participant-Join bleibt auflösbar. Spaltennamen sind parametrisierbar:
#' calls = call_identity_mail/_name (Default), events/booking = attendees_emailAddress_address/_name.
#'
#' @param cmr data.frame mit der Mail- und Name-Spalte.
#' @param email_hashes Character-Vektor gesperrter E-Mail-Hashes.
#' @param pepper Pepper-Geheimnis.
#' @param mail_col Name der E-Mail-Spalte (Default "call_identity_mail").
#' @param name_col Name der Namens-Spalte (Default "call_identity_name").
#' @return data.frame gleicher Zeilenzahl mit transformierten Spalten.
#' @export
dsgvo_suppress_msgraph_record <- function(cmr, email_hashes, pepper,
                                          mail_col = "call_identity_mail",
                                          name_col = "call_identity_name") {
  # ---- start ---- #
  if (nrow(cmr) == 0 || length(email_hashes) == 0) return(cmr)
  h <- dsgvo_hash_email(cmr[[mail_col]], pepper)
  hit <- !is.na(h) & h %in% email_hashes
  cmr[[name_col]][hit] <- NA_character_
  cmr[[mail_col]][hit] <- dsgvo_email_tombstone(h[hit])
  cmr
}

#' Ersetzt gesperrte Telefonnummern in einem Sipgate-Calls-DataFrame
#'
#' Tombstoned je Anruf die source- und/oder target-Seite, deren Nummer gesperrt ist
#' (Nummer -> `[geloescht]`, zugehöriger contact_name -> NA). Reine Transformation;
#' keine Zeile wird entfernt (Match läuft auf sipgate_call_id, nicht Telefon).
#'
#' @param calls_df data.frame mit source_number/target_number (+ *_contact_name).
#' @param phone_hashes Character-Vektor gesperrter Telefon-Hashes.
#' @param pepper Pepper-Geheimnis.
#' @return data.frame gleicher Zeilenzahl mit transformierten Spalten.
#' @export
dsgvo_suppress_sipgate_calls <- function(calls_df, phone_hashes, pepper) {
  # ---- start ---- #
  if (nrow(calls_df) == 0 || length(phone_hashes) == 0) return(calls_df)
  src_hit <- !is.na(calls_df$source_number) &
    dsgvo_hash_phone(calls_df$source_number, pepper) %in% phone_hashes
  tgt_hit <- !is.na(calls_df$target_number) &
    dsgvo_hash_phone(calls_df$target_number, pepper) %in% phone_hashes
  calls_df$source_number[src_hit] <- "[geloescht]"
  calls_df$source_contact_name[src_hit] <- NA_character_
  calls_df$target_number[tgt_hit] <- "[geloescht]"
  calls_df$target_contact_name[tgt_hit] <- NA_character_
  calls_df
}
