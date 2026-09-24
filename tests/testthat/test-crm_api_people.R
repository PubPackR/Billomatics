test_that("get_central_station_contacts() survives a page on which no person has tags or custom_fields", {
  # Setup: 500 entries -> 2 pages; page 1 mixed, page 2 without any tag / custom field.
  # jsonlite returns list() instead of a data.frame for every row of page 2.
  count_json <- '{"total_entries": 500}'
  page1_json <- '[{"person":{"id":1,"tags":[{"id":11,"name":"W Foo"}],"custom_fields":[{"id":21,"name":"x"}]}},
                  {"person":{"id":2,"tags":[],"custom_fields":[]}}]'
  page2_json <- '[{"person":{"id":3,"tags":[],"custom_fields":[]}},
                  {"person":{"id":4,"tags":[],"custom_fields":[]}}]'

  mockery::stub(get_central_station_contacts, "httr::GET",
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
