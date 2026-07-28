# Metabase-API: gemeinsame Zugriffsfunktionen fuer alle Vorhaben, die Metabase
# lesen oder schreiben (SQL-Chatbot-Kontext, Question-Versionierung/CI).
# Auth laeuft ueber authentication_process("metabase", ...) in
# R/general_authentication.R. Diese Datei ist die EINZIGE Stelle mit
# Metabase-HTTP-Logik.

#' Nicht-deterministische Card-Felder
#'
#' Felder, die sich bei jeder Ausfuehrung aendern und deshalb aus exportierten
#' Card-Definitionen entfernt werden, damit Git-Diffs nur echte Aenderungen an
#' Query und Visualisierung zeigen.
#'
#' @export
METABASE_DYNAMIC_FIELDS <- c(
  "updated_at", "created_at", "last_query_start",
  "query_average_duration", "creator_id", "creator",
  "actor_id", "moderation_reviews", "can_write"
)

#' Card-Definition von nicht-deterministischen Feldern bereinigen
#'
#' @param card Liste mit der Card-Definition (Antwort von \code{metabase_get_card}).
#' @param dynamic_fields Character-Vektor der zu entfernenden Feldnamen.
#' @return Liste ohne die dynamischen Felder.
#' @export
metabase_sanitize_card <- function(card, dynamic_fields = METABASE_DYNAMIC_FIELDS) {
  if (!is.list(card)) {
    stop("card muss eine Liste sein.", call. = FALSE)
  }
  card[!names(card) %in% dynamic_fields]
}

#' Metabase-API-Request ausfuehren
#'
#' Dünner Wrapper um httr2 mit Retry bei transienten Fehlern (429/5xx) und
#' klaren Fehlermeldungen. Der API-Key wird NIE in eine Fehlermeldung
#' uebernommen.
#'
#' @param method HTTP-Methode, z.B. "GET" oder "PUT".
#' @param path Character-Vektor der Pfadsegmente, z.B. c("api", "card", "42").
#' @param api_key Metabase-API-Key (Header X-API-Key).
#' @param base_url Basis-URL der Metabase-Instanz.
#' @param query Optionale Named List mit Query-Parametern.
#' @param body Optionaler Request-Body (wird als JSON gesendet).
#' @param max_retries Maximale Wiederholungen bei transienten Fehlern.
#' @return Geparste JSON-Antwort als Liste.
#' @keywords internal
metabase_request <- function(method, path, api_key, base_url,
                             query = NULL, body = NULL, max_retries = 3) {

  if (is.null(api_key) || !nzchar(api_key[1])) {
    stop("Metabase-API-Key fehlt oder ist leer.", call. = FALSE)
  }
  if (is.null(base_url) || !nzchar(base_url[1])) {
    stop("Metabase-Base-URL fehlt oder ist leer.", call. = FALSE)
  }

  req <- httr2::request(base_url)
  req <- do.call(httr2::req_url_path_append, c(list(req), as.list(path)))
  req <- httr2::req_headers(req, "X-API-Key" = api_key[1])
  req <- httr2::req_method(req, method)

  if (!is.null(query)) {
    req <- do.call(httr2::req_url_query, c(list(req), query))
  }
  if (!is.null(body)) {
    req <- httr2::req_body_json(req, body)
  }

  req <- httr2::req_error(req, is_error = function(resp) FALSE)
  req <- httr2::req_retry(
    req,
    max_tries    = max_retries + 1,
    is_transient = function(resp) httr2::resp_status(resp) %in% c(429L, 500L, 502L, 503L, 504L)
  )

  resp   <- httr2::req_perform(req)
  status <- httr2::resp_status(resp)
  route  <- paste0("/", paste(path, collapse = "/"))

  if (status %in% c(401L, 403L)) {
    stop("Metabase-API: Authentifizierung fehlgeschlagen (HTTP ", status,
         ") fuer ", route, ". Bitte den Metabase-API-Key und dessen ",
         "Berechtigungen pruefen.", call. = FALSE)
  }
  if (status >= 400L) {
    stop("Metabase-API: Anfrage fehlgeschlagen (HTTP ", status, ") fuer ",
         route, ".", call. = FALSE)
  }

  httr2::resp_body_json(resp)
}
