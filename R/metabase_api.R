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
