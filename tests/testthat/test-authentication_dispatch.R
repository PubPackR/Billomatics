test_that("authentication_process dispatch enthaelt die scoped-msgraph services", {
  fmls <- names(formals(Billomatics::authentication_process))
  default_services <- eval(formals(Billomatics::authentication_process)$needed_services)
  expect_true("msgraph_scoped_app" %in% default_services)
  expect_true("msgraph_delegated"  %in% default_services)
})

test_that("authentication_process dispatch enthaelt google drive am Ende der Default-Liste", {
  # authentication_process() liest args nach Position. Ein neuer Service in
  # der Mitte der Default-Liste wuerde die Positionen aller folgenden
  # verschieben.
  default_services <- eval(formals(Billomatics::authentication_process)$needed_services)
  expect_identical(utils::tail(default_services, 1), "google drive")
})
