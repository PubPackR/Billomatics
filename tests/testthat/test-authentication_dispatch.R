test_that("authentication_process dispatch enthaelt die scoped-msgraph services", {
  fmls <- names(formals(Billomatics::authentication_process))
  default_services <- eval(formals(Billomatics::authentication_process)$needed_services)
  expect_true("msgraph_scoped_app" %in% default_services)
  expect_true("msgraph_delegated"  %in% default_services)
})
