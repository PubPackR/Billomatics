# Retry of Billomat requests whose response is not XML (billomat_get_xml).
# Trigger: crm-billomat-cross-system on 2026-11-25 stopped at page 206 with
# read_xml "Start tag expected, '<' not found".

fake_response <- function(status, body, headers = list()) {
  structure(
    list(status_code = status, content = charToRaw(body), headers = headers),
    class = "response"
  )
}

xml_page <- '<?xml version="1.0" encoding="UTF-8"?><offers><offer><id>1</id></offer></offers>'

mock_get_sequence <- function(responses) {
  calls <- 0
  list(
    get = function(...) {
      calls <<- calls + 1
      responses[[calls]]
    },
    calls = function() calls
  )
}

test_that("a page without XML is requested again and the second answer is used", {
  seq_mock <- mock_get_sequence(list(
    fake_response(200, ""),
    fake_response(200, xml_page)
  ))
  waits <- c()
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) waits <<- c(waits, seconds))

  result <- suppressMessages(billomat_get_xml("https://x.billomat.net/api/offers?api_key=secret"))

  expect_equal(seq_mock$calls(), 2)
  expect_equal(waits, 5)
  expect_equal(xml2::xml_length(result$body), 1)
  expect_s3_class(result$response, "response")
})

test_that("an HTTP error with an XML body is retried too", {
  seq_mock <- mock_get_sequence(list(
    fake_response(429, "<error>rate limit</error>"),
    fake_response(503, "<html>maintenance</html>"),
    fake_response(200, xml_page)
  ))
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) NULL)

  result <- suppressMessages(billomat_get_xml("https://x.billomat.net/api/offers"))

  expect_equal(seq_mock$calls(), 3)
  expect_equal(xml2::xml_length(result$body), 1)
})

test_that("it stops after the last attempt with status and body start, without the url", {
  seq_mock <- mock_get_sequence(rep(list(fake_response(502, "Bad Gateway")), 4))
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) NULL)

  err <- expect_error(
    suppressMessages(billomat_get_xml("https://x.billomat.net/api/offers?api_key=secret")),
    "failed after 4 attempts: HTTP 502, body starts with: Bad Gateway"
  )
  expect_false(grepl("secret", conditionMessage(err)))
  expect_equal(seq_mock$calls(), 4)
})

test_that("a good first answer needs one request and no wait", {
  seq_mock <- mock_get_sequence(list(fake_response(200, xml_page)))
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) stop("must not wait"))

  result <- billomat_get_xml("https://x.billomat.net/api/offers")

  expect_equal(seq_mock$calls(), 1)
  expect_equal(xml2::xml_length(result$body), 1)
})

test_that("a 429 waits until x-rate-limit-reset, capped at max_reset_wait", {
  reset_in_10_min <- as.character(floor(as.numeric(Sys.time())) + 600)
  reset_in_2_h <- as.character(floor(as.numeric(Sys.time())) + 7200)
  seq_mock <- mock_get_sequence(list(
    fake_response(429, "Rate limit", headers = list(`x-rate-limit-reset` = reset_in_10_min)),
    fake_response(429, "Rate limit", headers = list(`x-rate-limit-reset` = reset_in_2_h)),
    fake_response(200, xml_page)
  ))
  waits <- c()
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) waits <<- c(waits, seconds))

  suppressMessages(billomat_get_xml("https://x.billomat.net/api/offers"))

  expect_equal(seq_mock$calls(), 3)
  expect_gte(waits[1], 595)
  expect_lte(waits[1], 602)
  expect_equal(waits[2], 900)
})

test_that("a transport error is retried and the key never reaches the message", {
  calls <- 0
  get_mock <- function(...) {
    calls <<- calls + 1
    stop("Timeout was reached: https://x.billomat.net/api/offers?api_key=secret&page=1")
  }
  testthat::local_mocked_bindings(GET = get_mock, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) NULL)

  msgs <- character()
  err <- expect_error(
    withCallingHandlers(
      billomat_get_xml("https://x.billomat.net/api/offers?api_key=secret&page=1"),
      message = function(m) { msgs <<- c(msgs, conditionMessage(m)); invokeRestart("muffleMessage") }
    ),
    "failed after 4 attempts: request error: Timeout was reached"
  )
  expect_equal(calls, 4)
  expect_false(grepl("secret", conditionMessage(err)))
  expect_false(any(grepl("secret", msgs)))
})

test_that("curl_fetch_header retries too and returns the headers of the good answer", {
  seq_mock <- mock_get_sequence(list(
    fake_response(503, "<html>maintenance</html>"),
    fake_response(200, xml_page, headers = list(`x-total-count` = "20600",
                                                `x-rate-limit-remaining` = "250"))
  ))
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) NULL)

  invisible(capture.output(suppressMessages(
    result <- curl_fetch_header(content = "offers", page = 1, per_page = 1,
                                billomatApiKey = "secret", billomatID = "x")
  )))

  expect_equal(seq_mock$calls(), 2)
  expect_equal(result$headers$`x-total-count`, "20600")
})

test_that("curl_fetch_billomat survives one broken page", {
  seq_mock <- mock_get_sequence(list(
    fake_response(200, "Retry later"),
    fake_response(200, xml_page, headers = list(`x-rate-limit-remaining` = "100"))
  ))
  testthat::local_mocked_bindings(GET = seq_mock$get, .package = "httr")
  testthat::local_mocked_bindings(billomat_wait = function(seconds) NULL)

  invisible(capture.output(suppressMessages(
    result <- curl_fetch_billomat(content = "offers", page = 206, per_page = 100,
                                  billomatApiKey = "secret", billomatID = "x")
  )))

  expect_equal(seq_mock$calls(), 2)
  expect_equal(xml2::xml_length(result$body), 1)
})
