################################################################################-
# ----- Description -------------------------------------------------------------
#
# Delegierte MSGraph-Auth fuer SharePoint-Dateizugriff (Service msgraph_sharepoint).
# Authorization-Code-Flow mit Confidential Client: einmaliger Bootstrap auf dem
# Server, danach unbeaufsichtigte Refreshs aus einem verschluesselten Token-Store
# mit Rotation (n8n-Muster). Ein gemeinsamer Store fuer alle Konsumenten-Repos.
# Spec: docs/superpowers/specs/2026-08-18-msgraph-sharepoint-delegated-design.md
#
# Interne Helfer (msgraph_sp_*) sind bewusst NICHT exportiert - Namenskonflikte
# mit dbconnectorR (base-62) vermeiden.
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/08
#

.msgraph_sp_default_scopes <- c(
  "https://graph.microsoft.com/Files.ReadWrite.All",
  "https://graph.microsoft.com/User.Read",
  "offline_access")

#' Verschluesselten SharePoint-Token-Store lesen (intern)
#'
#' @param path Pfad zur verschluesselten Store-Datei.
#' @param key Entschluesselungsschluessel.
#' @return Liste mit refresh_token, obtained_at, last_refreshed_at, tenant_id,
#'   client_id, scopes.
#' @noRd
msgraph_sp_store_read <- function(path, key) {
  # ---- start ---- #
  if (!file.exists(path)) {
    stop("Token-Store nicht gefunden: ", path,
         "\nBootstrap erforderlich - msgraph_sharepoint_bootstrap() ausfuehren.",
         call. = FALSE)
  }
  cipher <- paste(readLines(path, warn = FALSE), collapse = "")
  json <- safer::decrypt_string(cipher, key = key)
  jsonlite::fromJSON(json, simplifyVector = TRUE)
}

#' Verschluesselten SharePoint-Token-Store schreiben (intern)
#'
#' Schreibt ueber eine Temp-Datei und legt den Vorgaenger als .bak ab.
#'
#' @param path Zielpfad.
#' @param key Verschluesselungsschluessel.
#' @param store Liste wie von msgraph_sp_store_read() geliefert.
#' @return invisible(path)
#' @noRd
msgraph_sp_store_write <- function(path, key, store) {
  # ---- start ---- #
  json <- as.character(jsonlite::toJSON(store, auto_unbox = TRUE))
  cipher <- safer::encrypt_string(json, key = key)
  # Zeilenumbrueche im Chiffrat wuerden das einzeilige Wiedereinlesen zerstoeren
  cipher <- gsub("[\r\n]", "", cipher)

  tmp <- paste0(path, ".tmp")
  writeLines(cipher, tmp)

  if (file.exists(path)) {
    file.copy(path, paste0(path, ".bak"), overwrite = TRUE)
    # file.rename() scheitert unter Windows, wenn das Ziel existiert
    unlink(path)
  }
  if (!file.rename(tmp, path)) {
    unlink(tmp)
    stop("Token-Store konnte nicht ersetzt werden: ", path, call. = FALSE)
  }
  invisible(path)
}

#' Access-Token ueber ein Refresh-Token erneuern (intern)
#'
#' Confidential Client: client_secret geht mit. Wirft bei HTTP != 200 mit dem
#' AADSTS-Code, damit im Log sofort die Ursache steht.
#'
#' @param tenant_id Tenant-ID.
#' @param client_id Client-ID der App-Registrierung.
#' @param client_secret Client-Secret.
#' @param refresh_token Aktuelles Refresh-Token.
#' @param scopes Character-Vektor der Scopes.
#' @return Geparste Entra-Antwort (access_token, expires_in, meist refresh_token).
#' @noRd
msgraph_sp_refresh <- function(tenant_id, client_id, client_secret,
                               refresh_token, scopes) {
  # ---- start ---- #
  uri <- paste0("https://login.microsoftonline.com/", tenant_id, "/oauth2/v2.0/token")
  response <- httr::POST(uri, encode = "form", body = list(
    grant_type    = "refresh_token",
    client_id     = client_id,
    client_secret = client_secret,
    refresh_token = refresh_token,
    scope         = paste(scopes, collapse = " ")
  ))
  parsed <- httr::content(response, as = "parsed", type = "application/json")
  status <- response$status_code
  if (status != 200) {
    error_code <- if (is.null(parsed$error)) "unbekannt" else parsed$error
    error_desc <- if (is.null(parsed$error_description)) "keine Beschreibung" else parsed$error_description
    stop(sprintf(paste0(
      "MSGraph-SharePoint-Token-Refresh fehlgeschlagen (HTTP %s): %s\n%s\n",
      "msgraph_sharepoint_bootstrap() erneut ausfuehren, um ein neues Refresh-Token zu holen."),
      status, error_code, error_desc), call. = FALSE)
  }
  parsed
}
