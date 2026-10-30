test_that("get_central_station_contacts() survives a page on which no person has tags or custom_fields", {
  # Setup: 500 entries -> 2 pages; page 1 mixed, page 2 without any tag / custom field.
  # jsonlite returns list() instead of a data.frame for every row of page 2.
  count_json <- '{"total_entries": 500}'
  page1_json <- '[{"person":{"id":1,"tags":[{"id":11,"name":"W Foo"}],"custom_fields":[{"id":21,"name":"x"}]}},
                  {"person":{"id":2,"tags":[],"custom_fields":[]}}]'
  page2_json <- '[{"person":{"id":3,"tags":[],"custom_fields":[]}},
                  {"person":{"id":4,"tags":[],"custom_fields":[]}}]'

  mockery::stub(get_central_station_contacts, "crm_GET",
                mockery::mock(NULL, NULL, NULL))
  mockery::stub(get_central_station_contacts, "httr::content",
                mockery::mock(count_json, page1_json, page2_json))

  # the function prints its page progress
  invisible(capture.output(
    result <- get_central_station_contacts(api_key = "dummy")
  ))

  expect_equal(nrow(result), 4L)

  # Bug: Can't combine `tags[[1]]` <data.frame> and `tags[[3]]` <list>
  tags <- tidyr::unnest(result, tags, names_sep = "_")
  expect_equal(tags$id, 1L)
  expect_equal(tags$tags_name, "W Foo")

  custom_fields <- tidyr::unnest(result, custom_fields, names_sep = "_")
  expect_equal(custom_fields$custom_fields_name, "x")
})

test_that("get_central_station_contacts() retries a page the CRM rate limits with 429 'Retry later'", {
  # Bug: the page loop called httr::GET directly, so the plain-text 429 body
  # "Retry later" reached jsonlite::fromJSON and aborted crm_update_leads.
  ok      <- structure(list(status_code = 200L, headers = list()), class = "response")
  limited <- structure(list(status_code = 429L, headers = list(`retry-after` = "1")), class = "response")

  # httr::GET is reached through crm_GET -> crm_request, so mock it in httr itself
  get_mock <- mockery::mock(ok, limited, ok)
  testthat::local_mocked_bindings(GET = get_mock, .package = "httr")
  mockery::stub(get_central_station_contacts, "httr::content",
                mockery::mock('{"total_entries": 1}', '[{"person":{"id":1}}]'))

  invisible(capture.output(suppressMessages(
    result <- get_central_station_contacts(api_key = "dummy")
  )))

  # count, page 1 rate limited, page 1 retried
  mockery::expect_called(get_mock, 3)
  expect_equal(result$id, 1L)
})
