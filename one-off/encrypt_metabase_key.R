# -------------------------------------------------------------------------- #
# Description:
#   Einmal-Skript: verschlüsselt den bestehenden Metabase-API-Key (Klartext)
#   in das Format, das authentication_metabase() erwartet, und schreibt das
#   Ergebnis nach keys/metabase.txt.
#
#   Der hier verwendete Decryption-Key MUSS derselbe sein, den FlowForce
#   später als arg an authentication_metabase() übergibt (bzw. den du bei
#   getPass eingibst) — sonst schlägt safer::decrypt_string() zur Laufzeit fehl.
#
#   Nach dem Lauf: den Klartext-Key aus diesem Skript wieder entfernen
#   (nicht committen) und keys/metabase.txt an den keys-Ordner ausliefern.
# -------------------------------------------------------------------------- #

## ----- libraries ----- #
library(safer)

## ----- constants ----- #
# Platzhalter: hier den bestehenden Klartext-API-Key einsetzen.
plaintext_api_key <- "PLATZHALTER_KEY"
# Platzhalter: hier den Namen des Systems einsetzen.
system <- "Metabase"

# Zielpfad der verschlüsselten Datei. Anpassen an den echten keys-Ordner
# (zur Laufzeit liest authentication_metabase() aus "../../keys/metabase.txt").
output_file <- paste0("../../keys/", tolower(system), ".txt")

# ----- Start ----- #

# Decryption-Key interaktiv eingeben (nicht im Skript hardcoden).
decrypt_key <- getPass::getPass(paste0("Decryption-Key für ", system, " (gleich wie andere Services): "))

# Klartext-Key verschlüsseln.
encrypted_api_key <- safer::encrypt_string(plaintext_api_key, key = decrypt_key)

# In Datei schreiben.
writeLines(encrypted_api_key, output_file)
message(paste0("Verschlüsselter ", system, "-Key geschrieben nach: "), normalizePath(output_file))

# Gegenprobe: wieder entschlüsseln und mit Original vergleichen.
check <- safer::decrypt_string(readLines(output_file), key = decrypt_key)
if (identical(check, plaintext_api_key)) {
  message("Round-trip OK — Ent-/Verschlüsselung passt.")
} else {
  stop("Round-trip FEHLGESCHLAGEN — verschlüsselter Key entschlüsselt nicht zum Original.")
}

keys <- Billomatics::authentication_process(c(tolower(system), "postgresql"), args = commandArgs(trailingOnly = TRUE))
