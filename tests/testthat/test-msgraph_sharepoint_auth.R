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

# --- Token-Provider Tests -------------------------------------------------------

make_test_auth <- function(store_path) list(
  tenant_id = "tid", client_id = "cid", client_secret = "sec",
  store_key = "test-store-key", store_path = store_path,
  site_url = "https://example.sharepoint.com/sites/Test")

write_test_store <- function(store_path, refresh_token = "rt-1",
                             last_refreshed_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")) {
  Billomatics:::msgraph_sp_store_write(store_path, "test-store-key", list(
    refresh_token = refresh_token, obtained_at = "2026-08-18T10:00:00Z",
    last_refreshed_at = last_refreshed_at,
    tenant_id = "tid", client_id = "cid",
    scopes = Billomatics:::.msgraph_sp_default_scopes))
}

test_that("Provider liefert Token, persistiert Rotation und cached im Prozess", {
  withr::defer(Billomatics:::msgraph_sp_provider_cache_clear())
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  write_test_store(store_path)

  calls <- 0L
  testthat::local_mocked_bindings(
    msgraph_sp_refresh = function(...) {
      calls <<- calls + 1L
      list(access_token = "at-1", expires_in = 3599, refresh_token = "rt-2")
    }
  )

  provider <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_path))
  expect_equal(provider(), "at-1")
  expect_equal(provider(), "at-1")            # zweiter Call: aus dem Cache
  expect_equal(calls, 1L)     # nur EIN Refresh-POST
  # Rotation wurde persistiert
  expect_equal(
    Billomatics:::msgraph_sp_store_read(store_path, "test-store-key")$refresh_token,
    "rt-2")
})

test_that("Antwort ohne neues Refresh-Token laesst Store unveraendert", {
  withr::defer(Billomatics:::msgraph_sp_provider_cache_clear())
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  write_test_store(store_path)
  testthat::local_mocked_bindings(
    msgraph_sp_refresh = function(...) {
      list(access_token = "at-1", expires_in = 3599)
    }
  )
  provider <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_path))
  provider()
  expect_equal(
    Billomatics:::msgraph_sp_store_read(store_path, "test-store-key")$refresh_token,
    "rt-1")
  expect_false(file.exists(paste0(store_path, ".bak")))
})

test_that("Inaktivitaet > warn_inactive_days warnt", {
  withr::defer(Billomatics:::msgraph_sp_provider_cache_clear())
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  write_test_store(store_path, last_refreshed_at = "2026-01-01T00:00:00Z")
  testthat::local_mocked_bindings(
    msgraph_sp_refresh = function(...) {
      list(access_token = "at-1", expires_in = 3599)
    }
  )
  provider <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_path))
  expect_warning(provider(), "Tage")
})

test_that("Provider-Cache trennt nach client_id + store_path", {
  withr::defer(Billomatics:::msgraph_sp_provider_cache_clear())
  store_a <- file.path(withr::local_tempdir(), "a.txt")
  store_b <- file.path(withr::local_tempdir(), "b.txt")
  p1 <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_a))
  p2 <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_a))
  p3 <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_b))
  expect_identical(p1, p2)
  expect_false(identical(p1, p3))
})

test_that("authentication_msgraph_sharepoint liest und validiert das Key-JSON", {
  root <- withr::local_tempdir()
  dir.create(file.path(root, "keys", "Microsoft365R"), recursive = TRUE)
  dir.create(file.path(root, "apps", "x"), recursive = TRUE)
  auth <- list(
    tenant_id = "tid", client_id = "cid", client_secret = "sec",
    store_key = "sk", store_path = "../../keys/Microsoft365R/msgraph_sharepoint_refresh.txt",
    site_url = "https://example.sharepoint.com/sites/Test")
  cipher <- safer::encrypt_string(
    as.character(jsonlite::toJSON(auth, auto_unbox = TRUE)), key = "dec-key")
  writeLines(gsub("[\r\n]", "", cipher),
             file.path(root, "keys", "Microsoft365R", "msgraph_sharepoint.txt"))

  withr::local_dir(file.path(root, "apps", "x"))
  got <- Billomatics:::authentication_msgraph_sharepoint("dec-key")
  expect_equal(got$client_id, "cid")
  expect_equal(got$site_url, "https://example.sharepoint.com/sites/Test")
})

test_that("authentication_msgraph_sharepoint wirft bei unvollstaendigem JSON", {
  root <- withr::local_tempdir()
  dir.create(file.path(root, "keys", "Microsoft365R"), recursive = TRUE)
  dir.create(file.path(root, "apps", "x"), recursive = TRUE)
  cipher <- safer::encrypt_string('{"tenant_id":"tid"}', key = "dec-key")
  writeLines(gsub("[\r\n]", "", cipher),
             file.path(root, "keys", "Microsoft365R", "msgraph_sharepoint.txt"))
  withr::local_dir(file.path(root, "apps", "x"))
  expect_error(Billomatics:::authentication_msgraph_sharepoint("dec-key"),
               "unvollstaendig")
})

# --- Bootstrap-Tests -----------------------------------------------

test_that("msgraph_sp_extract_code extrahiert den Code aus allen Eingabeformen", {
  expect_equal(Billomatics:::msgraph_sp_extract_code("ABC.123-xyz"), "ABC.123-xyz")
  expect_equal(Billomatics:::msgraph_sp_extract_code("code=ABC.123"), "ABC.123")
  expect_equal(Billomatics:::msgraph_sp_extract_code(
    "http://localhost:1410/?code=ABC.123&state=xyz#frag"), "ABC.123")
  expect_error(Billomatics:::msgraph_sp_extract_code("   "), "leer")
})

test_that("Bootstrap-URL enthaelt alle Pflicht-Parameter", {
  auth <- list(tenant_id = "tid", client_id = "cid")
  url <- Billomatics::msgraph_sharepoint_bootstrap_url(auth)
  expect_match(url, "^https://login\\.microsoftonline\\.com/tid/oauth2/v2\\.0/authorize\\?")
  expect_match(url, "client_id=cid", fixed = TRUE)
  expect_match(url, "response_type=code", fixed = TRUE)
  expect_match(url, "offline_access")
  expect_match(url, "redirect_uri=http%3A%2F%2Flocalhost%3A1410%2F", fixed = TRUE)
})

test_that("Bootstrap tauscht Code, schreibt Store und wirft ohne Refresh-Token", {
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  auth <- list(tenant_id = "tid", client_id = "cid", client_secret = "sec",
               store_key = "sk", store_path = store_path,
               site_url = "https://example.sharepoint.com/sites/Test")

  ok_response <- structure(list(status_code = 200L), class = "response")

  testthat::with_mocked_bindings(
    {
      Billomatics::msgraph_sharepoint_bootstrap(auth, "code=ABC")
      store <- Billomatics:::msgraph_sp_store_read(store_path, "sk")
      expect_equal(store$refresh_token, "rt-boot")
      expect_equal(store$client_id, "cid")
    },
    POST = function(url, ...) ok_response,
    content = function(x, as = "parsed", type = NULL, encoding = NULL) {
      list(access_token = "at", expires_in = 3599, refresh_token = "rt-boot")
    },
    GET = function(url, ...) ok_response,
    .package = "httr"
  )

  testthat::with_mocked_bindings(
    {
      expect_error(Billomatics::msgraph_sharepoint_bootstrap(auth, "ABC"),
                   "offline_access")
    },
    POST = function(url, ...) ok_response,
    content = function(x, as = "parsed", type = NULL, encoding = NULL) {
      list(access_token = "at", expires_in = 3599)
    },
    GET = function(url, ...) ok_response,
    .package = "httr"
  )
})
