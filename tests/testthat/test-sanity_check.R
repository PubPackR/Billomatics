test_that("report_sanity_check returns invisible(TRUE) when check passes", {
  # In interactive mode the function returns before check_passed is evaluated,
  # so we mock interactive() to FALSE to reach the check_passed guard.
  local_mocked_bindings(interactive = function() FALSE, .package = "base")
  result <- report_sanity_check(
    check_passed      = TRUE,
    check_name        = "dummy",
    check_message     = "dummy",
    con               = NULL,
    asana_api_token   = "dummy",
    asana_project_gid = "dummy"
  )
  expect_true(isTRUE(result))
})

test_that("report_sanity_check returns invisible without action in interactive session", {
  # Default interactive() = TRUE (running inside RStudio / devtools::test())
  result <- report_sanity_check(
    check_passed      = FALSE,
    check_name        = "dummy",
    check_message     = "dummy",
    con               = NULL,
    asana_api_token   = "dummy",
    asana_project_gid = "dummy"
  )
  # Function must return check_passed unchanged without touching con or Asana
  expect_false(isTRUE(result))
})

test_that("create_asana_task returns NULL on network error without crashing", {
  logger <- log4r::logger("ERROR", appenders = log4r::console_appender())
  result <- create_asana_task(
    project_gid = "invalid_gid",
    task_name   = "Test task",
    api_token   = "invalid_token",
    logger      = logger
  )
  expect_null(result)
})

test_that("create_asana_task returns NULL with logger = NULL (no crash)", {
  result <- create_asana_task(
    project_gid = "invalid_gid",
    task_name   = "Test task",
    api_token   = "invalid_token",
    logger      = NULL
  )
  expect_null(result)
})
