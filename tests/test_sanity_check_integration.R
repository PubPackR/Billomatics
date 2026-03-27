################################################################################-
# Integration Test: report_sanity_check
#
# Ausfuehren via:
#   Rscript tests/test_sanity_check_integration.R <master_key>
#
# Prueft alle 4 Szenarien gegen echte DB + Asana.
# Nach Test 2: Asana-Ticket manuell schliessen, dann Test 4 ausfuehren.
################################################################################-

devtools::load_all()

args <- commandArgs(trailingOnly = TRUE)
keys <- Billomatics::authentication_process(c("postgresql", "asana"), args)

con <- postgres_connect(
  needed_tables    = c(),
  postgres_keys    = keys$postgresql,
  update_local_tables = FALSE
)

logger           <- log4r::logger("DEBUG", appenders = log4r::console_appender())
asana_token      <- keys$asana[[2]]
asana_project    <- "1211291490559148"  # Interne Prozesse
test_check_name  <- "integration_test_revenue_rows_not_empty"

cat("\n===== Test 1: check_passed = TRUE -> keine DB-Row, kein Ticket =====\n")
report_sanity_check(
  check_passed      = TRUE,
  check_name        = test_check_name,
  check_message     = "Sollte nie erscheinen",
  con               = con,
  asana_api_token   = asana_token,
  asana_project_gid = asana_project,
  logger            = logger
)
cat("Erwartet: keine Ausgabe. Pruefe DB: SELECT * FROM raw.metadata_sanity_check_log WHERE check_name = '", test_check_name, "';\n", sep = "")

cat("\n===== Test 2: Erster Fehler -> DB-Row + Asana-Ticket =====\n")
report_sanity_check(
  check_passed      = FALSE,
  check_name        = test_check_name,
  check_message     = "Testnachricht: Tabelle hat 0 Zeilen",
  con               = con,
  asana_api_token   = asana_token,
  asana_project_gid = asana_project,
  context           = list(rows = 0, table = "processed.revenue"),
  logger            = logger
)
cat("Erwartet: DB-Row mit asana_task_gid, Asana-Ticket '[SANITY] ... |", test_check_name, "'\n")

cat("\n===== Test 3: Zweiter Aufruf -> kein Duplikat-Ticket (Ticket noch offen) =====\n")
report_sanity_check(
  check_passed      = FALSE,
  check_name        = test_check_name,
  check_message     = "Testnachricht: immer noch 0 Zeilen",
  con               = con,
  asana_api_token   = asana_token,
  asana_project_gid = asana_project,
  logger            = logger
)
cat("Erwartet: DB last_seen + message aktualisiert, KEIN zweites Asana-Ticket\n")

cat("\n===== Test 4: Nach manuellem Schliessen des Asana-Tickets -> neues Ticket =====\n")
cat("-> Bitte jetzt das Asana-Ticket manuell schliessen, dann Enter druecken\n")
readline()
report_sanity_check(
  check_passed      = FALSE,
  check_name        = test_check_name,
  check_message     = "Testnachricht: neues Ticket nach Schliessen",
  con               = con,
  asana_api_token   = asana_token,
  asana_project_gid = asana_project,
  logger            = logger
)
cat("Erwartet: neues Asana-Ticket, asana_task_gid in DB aktualisiert\n")

cat("\n===== Alle Tests abgeschlossen =====\n")
cat("Bitte Asana-Testtickets manuell loeschen und DB-Row bereinigen:\n")
cat("DELETE FROM raw.metadata_sanity_check_log WHERE check_name = '", test_check_name, "';\n", sep = "")
