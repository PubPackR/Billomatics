test_that("metabase_sanitize_card entfernt dynamische Felder", {
  card <- list(
    id                     = 12,
    name                   = "Umsatz pro Monat",
    dataset_query          = list(type = "native", native = list(query = "SELECT 1")),
    updated_at             = "2026-01-01T10:00:00Z",
    created_at             = "2025-01-01T10:00:00Z",
    query_average_duration = 1234,
    creator_id             = 5,
    can_write              = TRUE
  )

  out <- metabase_sanitize_card(card)

  expect_named(out, c("id", "name", "dataset_query"))
  expect_equal(out$dataset_query$native$query, "SELECT 1")
})

test_that("metabase_sanitize_card laesst Karten ohne dynamische Felder unveraendert", {
  card <- list(id = 1, name = "X", dataset_query = list(type = "query"))
  expect_equal(metabase_sanitize_card(card), card)
})

test_that("metabase_sanitize_card lehnt Nicht-Listen ab", {
  expect_error(metabase_sanitize_card("kein card objekt"), "Liste")
})

test_that("metabase_request baut Pfad und X-API-Key-Header korrekt", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  res <- metabase_request(
    "GET", c("api", "card", "42"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com"
  )

  expect_true(res$ok)
  expect_equal(captured$url, "https://metabase.example.com/api/card/42")

  # Der Header wird tatsaechlich mit dem korrekten Wert gesetzt ...
  real_headers <- httr2:::headers_flatten(captured$headers, redact = FALSE)
  expect_equal(real_headers[["X-API-Key"]], "geheim")

  # ... ist aber redacted, sobald der Request geprintet/gestr()t wird.
  expect_true(httr2:::is_redacted(captured$headers)[["X-API-Key"]])
  printed <- paste(utils::capture.output(print(captured)), collapse = "\n")
  expect_true(grepl("REDACTED", printed, fixed = TRUE))
  expect_false(grepl("geheim", printed, fixed = TRUE))
})

test_that("metabase_request meldet 401 verstaendlich und OHNE den Key", {
  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 401L)

  err <- expect_error(
    metabase_request("GET", c("api", "collection"),
                     api_key = "SUPERGEHEIM", base_url = "https://metabase.example.com")
  )

  expect_match(conditionMessage(err), "401")
  expect_false(grepl("SUPERGEHEIM", conditionMessage(err), fixed = TRUE))
})

test_that("metabase_request verlangt Key und Base-URL", {
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "", base_url = "https://x"),
    "API-Key"
  )
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = ""),
    "Base-URL"
  )
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = character(0), base_url = "https://x"),
    "API-Key"
  )
})
