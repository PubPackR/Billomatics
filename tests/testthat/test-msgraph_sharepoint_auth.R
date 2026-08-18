test_that("Store write/read Roundtrip funktioniert und schreibt .bak", {
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  key <- "test-store-key"
  store <- list(
    refresh_token = "rt-1", obtained_at = "2026-08-18T10:00:00Z",
    last_refreshed_at = "2026-08-18T10:00:00Z",
    tenant_id = "tid", client_id = "cid", scopes = c("a", "b"))

  Billomatics:::msgraph_sp_store_write(store_path, key, store)
  got <- Billomatics:::msgraph_sp_store_read(store_path, key)
  expect_equal(got$refresh_token, "rt-1")
  expect_equal(got$scopes, c("a", "b"))
  expect_false(file.exists(paste0(store_path, ".bak")))

  store$refresh_token <- "rt-2"
  Billomatics:::msgraph_sp_store_write(store_path, key, store)
  expect_true(file.exists(paste0(store_path, ".bak")))
  expect_equal(Billomatics:::msgraph_sp_store_read(store_path, key)$refresh_token, "rt-2")
  # .bak enthaelt den Vorgaenger
  bak <- Billomatics:::msgraph_sp_store_read(paste0(store_path, ".bak"), key)
  expect_equal(bak$refresh_token, "rt-1")
})

test_that("Store read wirft verstaendlich, wenn Datei fehlt", {
  expect_error(
    Billomatics:::msgraph_sp_store_read(file.path(tempdir(), "gibtsnicht.txt"), "k"),
    "Bootstrap")
})

# --- Fakes fuer httr ----------------------------------------------------------

ok_response <- function(access = "at", refresh = "rt-neu", expires = 3599) {
  list(status_code = 200L,
       payload = list(access_token = access, refresh_token = refresh,
                      expires_in = expires))
}

err_response <- function(error = "invalid_grant", desc = "AADSTS70008: expired") {
  list(status_code = 400L,
       payload = list(error = error, error_description = desc))
}

fake_post <- function(response) {
  function(url, ...) response
}

fake_content <- function(x, as = "parsed", type = NULL, encoding = NULL) x$payload

with_fake_http <- function(response, code) {
  testthat::with_mocked_bindings(
    code(),
    POST = fake_post(response), content = fake_content,
    .package = "httr"
  )
}

test_that("Refresh parst Erfolgsantwort", {
  got <- with_fake_http(ok_response(), function() {
    Billomatics:::msgraph_sp_refresh("tid", "cid", "sec", "rt-alt", c("s1"))
  })
  expect_equal(got$access_token, "at")
  expect_equal(got$refresh_token, "rt-neu")
})

test_that("Refresh wirft mit AADSTS-Code bei HTTP != 200", {
  expect_error(
    with_fake_http(err_response(), function() {
      Billomatics:::msgraph_sp_refresh("tid", "cid", "sec", "rt", c("s1"))
    }),
    "AADSTS70008")
})
