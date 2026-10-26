test_that("create_crm_task() sends one escaped JSON body per row and returns the new task IDs", {
  sent <- new.env()
  sent$bodies <- character()
  post_mock <- function(url, headers, body, encode) {
    sent$bodies <- c(sent$bodies, body)
    structure(list(), class = "response")
  }

  mockery::stub(create_crm_task, "crm_POST", post_mock)
  mockery::stub(create_crm_task, "httr::status_code", 201)
  mockery::stub(create_crm_task, "httr::content", mockery::mock(
    '{"task":{"id":111}}', '{"task":{"id":222}}'
  ))

  result <- create_crm_task(
    headers = c("X-apikey" = "dummy", "content-type" = "application/json"),
    df = data.frame(
      attachable_id = c(1234567, 7654321),
      attachable_type = "people",
      user_id = c(123456, 654321),
      name = c('Rückruf "Übergabe"', "Lead nachfassen"),
      precise_time = as.POSIXct(c("2026-10-27 09:00", "2026-10-28 09:00"), tz = "Europe/Berlin")
    )
  )

  expect_equal(result$task_id, c(111L, 222L))
  expect_true(all(result$success))

  first <- jsonlite::fromJSON(sent$bodies[1])$task
  expect_equal(first$name, 'Rückruf "Übergabe"')
  expect_equal(first$attachable_type, "Person")
  expect_equal(first$precise_time, "2026-10-27T08:00:00Z")
  expect_null(first$badge)
})

test_that("create_crm_task() reports a failed row and continues with the rest", {
  mockery::stub(create_crm_task, "crm_POST", structure(list(), class = "response"))
  mockery::stub(create_crm_task, "httr::status_code", mockery::mock(422, 201))
  mockery::stub(create_crm_task, "httr::content", mockery::mock(
    '{"errors":{"user_id":["invalid"]}}', '{"task":{"id":333}}'
  ))

  expect_warning(
    result <- create_crm_task(
      headers = c("X-apikey" = "dummy", "content-type" = "application/json"),
      df = data.frame(
        attachable_id = c(1234567, 7654321),
        attachable_type = "people",
        user_id = 123456,
        name = "Lead nachfassen",
        precise_time = as.POSIXct("2026-10-27 09:00", tz = "Europe/Berlin"),
        badge = "call"
      )
    ),
    "Failed to create 1 tasks"
  )

  expect_equal(result$success, c(FALSE, TRUE))
  expect_equal(result$status_code, c(422L, 201L))
  expect_true(is.na(result$task_id[1]))
  expect_match(result$error_message[1], "user_id")
})

test_that("create_crm_task() skips protected test leads", {
  post_mock <- mockery::mock()
  mockery::stub(create_crm_task, "crm_POST", post_mock)

  result <- create_crm_task(
    headers = c("X-apikey" = "dummy", "content-type" = "application/json"),
    df = data.frame(
      attachable_id = 44595807,
      attachable_type = "people",
      user_id = 123456,
      name = "Lead nachfassen",
      precise_time = as.POSIXct("2026-10-27 09:00", tz = "Europe/Berlin")
    )
  )

  expect_null(result)
  mockery::expect_called(post_mock, 0)
})
