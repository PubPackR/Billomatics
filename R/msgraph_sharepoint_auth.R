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

#' Authorization-Code aus Roh-Eingabe extrahieren (intern)
#'
#' Akzeptiert den reinen Code, 'code=...' oder die komplette Redirect-URL.
#' @noRd
msgraph_sp_extract_code <- function(raw_in) {
  # ---- start ---- #
  code <- trimws(raw_in)
  code <- sub("^.*?code=", "", code)
  code <- sub("[&#].*$", "", code)
  code <- trimws(code)
  if (!nzchar(code)) stop("Authorization-Code ist leer.", call. = FALSE)
  code
}

#' Login-URL fuer den einmaligen SharePoint-Bootstrap bauen
#'
#' Die URL im LOKALEN Browser oeffnen und als Service-Account anmelden. Der
#' Browser landet danach auf redirect_uri?code=... (Seite laedt nicht - es
#' lauscht nichts); den Code aus der Adresszeile kopieren und an
#' msgraph_sharepoint_bootstrap() uebergeben.
#'
#' Voraussetzung App-Registrierung: redirect_uri als *Web*-Redirect,
#' delegierte Scopes inkl. offline_access mit Admin-Consent.
#'
#' @param auth Liste mit tenant_id und client_id (z. B. aus
#'   authentication_process()$msgraph_sharepoint).
#' @param scopes Character-Vektor der Scopes.
#' @param redirect_uri Redirect-URI (Typ Web an der App-Registrierung).
#' @return Login-URL als String.
#' @export
msgraph_sharepoint_bootstrap_url <- function(auth,
                                             scopes = .msgraph_sp_default_scopes,
                                             redirect_uri = "http://localhost:1410/") {
  # ---- start ---- #
  paste0(
    "https://login.microsoftonline.com/", auth$tenant_id, "/oauth2/v2.0/authorize?",
    "client_id=", auth$client_id,
    "&response_type=code&response_mode=query",
    "&redirect_uri=", utils::URLencode(redirect_uri, reserved = TRUE),
    "&scope=", utils::URLencode(paste(scopes, collapse = " "), reserved = TRUE),
    "&prompt=login")
}

#' Einmaliger Bootstrap des SharePoint-Token-Stores (auf dem Server ausfuehren)
#'
#' Tauscht den Authorization-Code gegen Access- + Refresh-Token (der Tausch
#' passiert auf der Maschine, auf der diese Funktion laeuft - Bertelsmann-
#' Vorgabe: Token entsteht AUF dem Server), schreibt den verschluesselten
#' Store und macht eine /me-Probe. Der Code ist einmalig und laeuft nach
#' ~10 Minuten ab - zuegig einfuegen.
#'
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @param auth_code Purer Code ODER komplette kopierte Redirect-URL.
#' @param scopes Character-Vektor der Scopes (muessen zu bootstrap_url passen).
#' @param redirect_uri Muss identisch zu msgraph_sharepoint_bootstrap_url() sein.
#' @return invisible(store_path)
#' @export
msgraph_sharepoint_bootstrap <- function(auth, auth_code,
                                         scopes = .msgraph_sp_default_scopes,
                                         redirect_uri = "http://localhost:1410/") {
  # ---- start ---- #
  code <- msgraph_sp_extract_code(auth_code)

  response <- httr::POST(
    paste0("https://login.microsoftonline.com/", auth$tenant_id, "/oauth2/v2.0/token"),
    encode = "form", body = list(
      grant_type    = "authorization_code",
      client_id     = auth$client_id,
      client_secret = auth$client_secret,
      code          = code,
      redirect_uri  = redirect_uri,
      scope         = paste(scopes, collapse = " ")
    ))
  parsed <- httr::content(response, as = "parsed", type = "application/json")
  if (response$status_code != 200) {
    error_code <- if (is.null(parsed$error)) "unbekannt" else parsed$error
    error_desc <- if (is.null(parsed$error_description)) "keine Beschreibung" else parsed$error_description
    stop(sprintf("Bootstrap-Token-Tausch fehlgeschlagen (HTTP %s): %s\n%s",
                 response$status_code, error_code, error_desc), call. = FALSE)
  }
  if (is.null(parsed$refresh_token) || !nzchar(parsed$refresh_token)) {
    stop("Kein Refresh-Token erhalten. Steht 'offline_access' in den Scopes ",
         "und ist es consented?", call. = FALSE)
  }

  now_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  msgraph_sp_store_write(auth$store_path, auth$store_key, list(
    refresh_token = parsed$refresh_token,
    obtained_at = now_utc, last_refreshed_at = now_utc,
    tenant_id = auth$tenant_id, client_id = auth$client_id, scopes = scopes))

  probe <- httr::GET("https://graph.microsoft.com/v1.0/me",
                     httr::add_headers(Authorization = paste("Bearer", parsed$access_token)))
  message("Store geschrieben: ", auth$store_path,
          " | Probe /me HTTP ", probe$status_code)
  invisible(auth$store_path)
}
