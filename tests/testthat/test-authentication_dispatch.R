test_that("authentication_process dispatch enthaelt die scoped-msgraph services", {
  fmls <- names(formals(Billomatics::authentication_process))
  default_services <- eval(formals(Billomatics::authentication_process)$needed_services)
  expect_true("msgraph_scoped_app" %in% default_services)
  expect_true("msgraph_delegated"  %in% default_services)
})

test_that("authentication_process haelt die Default-Liste in fester Reihenfolge", {
  # authentication_process() liest args nach Position. Ein neuer oder
  # verschobener Service in der Default-Liste wuerde die Positionen aller
  # folgenden verschieben, deshalb ist die ganze Liste festgeschrieben.
  default_services <- eval(formals(Billomatics::authentication_process)$needed_services)
  expect_identical(default_services, c(
    "billomat", "crm", "crm_lm", "google sheet", "asana", "msgraph",
    "msgraph_scoped_app", "msgraph_delegated", "msgraph_sharepoint", "brevo",
    "google analytics", "bonusDB", "BigQuery", "BigQuery GA4", "cleverreach",
    "postgresql", "gemini", "openrouter", "openai_admin", "personio", "github",
    "metabase"
  ))
})

test_that("authentication_process dispatcht google drive mit dem Argument seiner Position", {
  # google drive steht nicht in der Default-Liste, muss aber per Name
  # aufrufbar sein und das Argument an seiner Position bekommen.
  seen <- NULL
  local_mocked_bindings(
    authentication_GDrive = function(args) { seen <<- args; "OK" },
    authentication_postgresql = function(args) "pg"
  )
  keys <- authentication_process(c("postgresql", "google drive"), args = c("a", "b"))
  expect_identical(seen, "b")
  expect_identical(keys$`google drive`, "OK")
})
