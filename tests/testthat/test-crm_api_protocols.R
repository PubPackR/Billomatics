test_that("get_central_station_protocols() paginates beyond 250 entries when filter_by is specified", {
  # Setup: page 1 has a full 250 rows, page 2 has 3 rows (last page)
  df_page1 <- data.frame(id = seq_len(250L))
  df_page2 <- data.frame(id = 251:253)

  mockery::stub(get_central_station_protocols, "crm_GET2",
                mockery::mock(NULL, NULL))
  mockery::stub(get_central_station_protocols, "httr2::resp_status",
                mockery::mock(200L, 200L))
  mockery::stub(get_central_station_protocols, "httr2::resp_body_string",
                mockery::mock("stub", "stub"))
  mockery::stub(get_central_station_protocols, "jsonlite::fromJSON",
                mockery::mock(df_page1, df_page2))

  result <- get_central_station_protocols(
    api_key       = "dummy",
    filter_by     = "person_id",
    filter_vector = 99999L
  )

  # Bug: only page 1 fetched  -> 250 rows
  # Fix: pages 1 + 2 fetched  -> 253 rows
  expect_equal(nrow(result), 253L)
})
