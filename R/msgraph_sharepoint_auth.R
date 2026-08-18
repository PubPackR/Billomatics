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

# --- Token-Provider mit Session-Cache und Rotation --------------------------

.msgraph_sp_provider_cache <- new.env(parent = emptyenv())

#' Provider-Cache leeren (intern, fuer Tests)
#' @noRd
msgraph_sp_provider_cache_clear <- function() {
  # ---- start ---- #
  rm(list = ls(.msgraph_sp_provider_cache), envir = .msgraph_sp_provider_cache)
  invisible(NULL)
}

#' Provider-Closure bauen (intern)
#' @noRd
msgraph_sp_provider_build <- function(auth, scopes, refresh_buffer_seconds,
                                      warn_inactive_days) {
  # ---- start ---- #
  cache <- new.env(parent = emptyenv())
  cache$token <- NULL
  cache$exp <- as.POSIXct(NA)

  function(force_refresh = FALSE) {
    now <- Sys.time()
    needs_refresh <- force_refresh ||
      is.null(cache$token) ||
      is.na(cache$exp) ||
      as.numeric(difftime(cache$exp, now, units = "secs")) < refresh_buffer_seconds
    if (!needs_refresh) {
      return(cache$token)
    }

    store <- msgraph_sp_store_read(auth$store_path, auth$store_key)

    last <- as.POSIXct(store$last_refreshed_at,
                       format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    inactive_days <- as.numeric(difftime(now, last, units = "days"))
    if (is.finite(inactive_days) && inactive_days > warn_inactive_days) {
      warning(sprintf(paste0(
        "MSGraph-SharePoint: letzter erfolgreicher Refresh ist %.0f Tage her ",
        "(Warnschwelle %s Tage). Refresh-Token verfaellt nach ~90 Tagen Inaktivitaet."),
        inactive_days, warn_inactive_days), call. = FALSE)
    }

    credentials <- msgraph_sp_refresh(auth$tenant_id, auth$client_id,
                                      auth$client_secret, store$refresh_token, scopes)

    rotated <- !is.null(credentials$refresh_token) &&
      !identical(credentials$refresh_token, store$refresh_token)
    if (rotated) {
      store$refresh_token <- credentials$refresh_token
      store$last_refreshed_at <- format(now, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      msgraph_sp_store_write(auth$store_path, auth$store_key, store)
    }

    cache$token <- credentials$access_token
    cache$exp <- now + as.numeric(credentials$expires_in)
    cache$token
  }
}

#' Token-Provider fuer den delegierten SharePoint-Zugriff
#'
#' Liefert eine Closure `function(force_refresh = FALSE)` mit dem Access-Token
#' als String — gleiche Signatur wie die uebrigen Token-Provider im Stack.
#' Der Provider wird pro (client_id, store_path) fuer die Prozesslaufzeit
#' gecacht: mehrere SharePoint-Calls in einem Skript teilen sich ein
#' Access-Token statt jedes Mal zu refreshen. Bei Rotation wird das neue
#' Refresh-Token sofort in den verschluesselten Store geschrieben.
#'
#' @param auth Liste aus authentication_process()$msgraph_sharepoint
#'   (tenant_id, client_id, client_secret, store_key, store_path, site_url).
#' @param scopes Character-Vektor der Scopes.
#' @param refresh_buffer_seconds Vorlauf, ab dem vorsorglich erneuert wird.
#' @param warn_inactive_days Tage seit letztem Refresh, ab denen gewarnt wird.
#' @return function(force_refresh = FALSE), liefert das Access-Token als String.
#' @export
msgraph_sharepoint_token_provider <- function(auth,
                                              scopes = .msgraph_sp_default_scopes,
                                              refresh_buffer_seconds = 300,
                                              warn_inactive_days = 60) {
  # ---- start ---- #
  cache_key <- paste(auth$client_id, auth$store_path, sep = "|")
  existing <- .msgraph_sp_provider_cache[[cache_key]]
  if (!is.null(existing)) {
    return(existing)
  }
  provider <- msgraph_sp_provider_build(auth, scopes, refresh_buffer_seconds,
                                        warn_inactive_days)
  .msgraph_sp_provider_cache[[cache_key]] <- provider
  provider
}
