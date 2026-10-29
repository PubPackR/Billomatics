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

test_that("metabase_sanitize_card lehnt unbenannte Listen ab", {
  expect_error(metabase_sanitize_card(list(1, 2, 3)), "benannte Liste")
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

test_that("metabase_request baut eine einwertige Query korrekt in die URL", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  metabase_request(
    "GET", c("api", "collection", "7", "items"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com",
    query    = list(models = "card")
  )

  expect_equal(captured$url, "https://metabase.example.com/api/collection/7/items?models=card")
})

test_that("metabase_request baut eine mehrwertige Query mit allen Werten in die URL (Regression fuer .multi='explode')", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  metabase_request(
    "GET", c("api", "collection", "7", "items"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com",
    query    = list(models = c("card", "dataset"))
  )

  expect_equal(
    captured$url,
    "https://metabase.example.com/api/collection/7/items?models=card&models=dataset"
  )
})

test_that("metabase_request setzt den Body bei PUT-Requests", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  metabase_request(
    "PUT", c("api", "card", "9"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com",
    body     = list(name = "Neu")
  )

  # Der serialisierte Body selbst ist ueber das request-Objekt nicht direkt
  # einsehbar (httr2 haelt ihn intern als Roh-Datenstruktur + Typ vor), daher
  # pruefen wir Praesenz und Typ des Body-Elements auf dem gebauten Request.
  expect_false(is.null(captured$body))
  expect_equal(captured$body$type, "json")
  expect_equal(captured$body$data$name, "Neu")
})

test_that("metabase_request verlangt einen Pfad", {
  expect_error(
    metabase_request("GET", NULL, api_key = "k", base_url = "https://x"),
    "Pfad"
  )
  expect_error(
    metabase_request("GET", character(0), api_key = "k", base_url = "https://x"),
    "Pfad"
  )
})

test_that("metabase_request verlangt Key und Base-URL (character(0)-Base-URL)", {
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = character(0)),
    "Base-URL"
  )
})

test_that("metabase_request haengt bei Fehlern >=400 (ausser 401/403) den Response-Body an", {
  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 400L)
  mockery::stub(metabase_request, "httr2::resp_body_string",
                function(resp, ...) '{"errors":{"name":"darf nicht leer sein"}}')

  err <- expect_error(
    metabase_request("PUT", c("api", "card", "9"),
                     api_key = "geheim", base_url = "https://metabase.example.com")
  )

  expect_match(conditionMessage(err), "400")
  expect_match(conditionMessage(err), "darf nicht leer sein", fixed = TRUE)
  expect_false(grepl("geheim", conditionMessage(err), fixed = TRUE))
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

test_that("metabase_request setzt den Timeout (Default 30s) auf dem Request", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  metabase_request(
    "GET", c("api", "card", "42"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com"
  )

  expect_equal(captured$options$timeout_ms, 30000)
})

test_that("metabase_request uebernimmt einen abweichenden timeout_s", {
  captured <- NULL

  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured <<- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))

  metabase_request(
    "GET", c("api", "card", "42"),
    api_key   = "geheim",
    base_url  = "https://metabase.example.com",
    timeout_s = 5
  )

  expect_equal(captured$options$timeout_ms, 5000)
})

test_that("metabase_request lehnt ungueltige timeout_s-Werte ab", {
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = "https://x",
                     timeout_s = 0),
    "timeout_s"
  )
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = "https://x",
                     timeout_s = -1),
    "timeout_s"
  )
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = "https://x",
                     timeout_s = NA),
    "timeout_s"
  )
  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k", base_url = "https://x",
                     timeout_s = "abc"),
    "timeout_s"
  )
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

test_that("metabase_get_card ruft den richtigen Pfad auf", {
  args <- NULL
  mockery::stub(metabase_get_card, "metabase_request",
                function(method, path, ...) { args <<- list(method = method, path = path); list(id = 42) })

  res <- metabase_get_card(42, api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$method, "GET")
  expect_equal(args$path, c("api", "card", "42"))
  expect_equal(res$id, 42)
})

test_that("metabase_get_card reicht einen abweichenden timeout_s/max_retries durch", {
  args <- NULL
  mockery::stub(metabase_get_card, "metabase_request",
                function(method, path, api_key, base_url, ..., timeout_s = 30, max_retries = 3) {
                  args <<- list(timeout_s = timeout_s, max_retries = max_retries)
                  list(id = 42)
                })

  metabase_get_card(42, api_key = "k", base_url = "https://mb.example.com",
                    timeout_s = 5, max_retries = 0)

  expect_equal(args$timeout_s, 5)
  expect_equal(args$max_retries, 0)
})

test_that("metabase_get_card reicht die Default-Werte fuer timeout_s/max_retries durch", {
  args <- NULL
  mockery::stub(metabase_get_card, "metabase_request",
                function(method, path, api_key, base_url, ..., timeout_s = 30, max_retries = 3) {
                  args <<- list(timeout_s = timeout_s, max_retries = max_retries)
                  list(id = 42)
                })

  metabase_get_card(42, api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$timeout_s, 30)
  expect_equal(args$max_retries, 3)
})

test_that("metabase_update_card reicht einen abweichenden timeout_s/max_retries durch", {
  args <- NULL
  mockery::stub(metabase_update_card, "metabase_request",
                function(method, path, api_key, base_url, query = NULL, body = NULL, ...,
                         timeout_s = 30, max_retries = 3) {
                  args <<- list(timeout_s = timeout_s, max_retries = max_retries)
                  list(id = 9)
                })

  metabase_update_card(9, body = list(name = "Neu"),
                       api_key = "k", base_url = "https://mb.example.com",
                       timeout_s = 5, max_retries = 0)

  expect_equal(args$timeout_s, 5)
  expect_equal(args$max_retries, 0)
})

test_that("metabase_update_card reicht die Default-Werte fuer timeout_s/max_retries durch", {
  args <- NULL
  mockery::stub(metabase_update_card, "metabase_request",
                function(method, path, api_key, base_url, query = NULL, body = NULL, ...,
                         timeout_s = 30, max_retries = 3) {
                  args <<- list(timeout_s = timeout_s, max_retries = max_retries)
                  list(id = 9)
                })

  metabase_update_card(9, body = list(name = "Neu"),
                       api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$timeout_s, 30)
  expect_equal(args$max_retries, 3)
})

test_that("metabase_get_collection_items reicht den models-Filter als Query durch", {
  args <- NULL
  mockery::stub(metabase_get_collection_items, "metabase_request",
                function(method, path, api_key, base_url, query = NULL, ...) {
                  args <<- list(path = path, query = query); list(data = list())
                })

  metabase_get_collection_items(7, api_key = "k", base_url = "https://mb.example.com",
                                models = "card")

  expect_equal(args$path, c("api", "collection", "7", "items"))
  expect_equal(args$query$models, "card")
})

test_that("metabase_update_card sendet PUT mit Body", {
  args <- NULL
  mockery::stub(metabase_update_card, "metabase_request",
                function(method, path, api_key, base_url, query = NULL, body = NULL, ...) {
                  args <<- list(method = method, path = path, body = body); list(id = 9)
                })

  metabase_update_card(9, body = list(name = "Neu"),
                       api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$method, "PUT")
  expect_equal(args$path, c("api", "card", "9"))
  expect_equal(args$body$name, "Neu")
})

test_that("metabase_get_collections ruft den richtigen Pfad auf", {
  args <- NULL
  mockery::stub(metabase_get_collections, "metabase_request",
                function(method, path, ...) {
                  args <<- list(method = method, path = path)
                  list(id = 1, name = "Sales")
                })

  res <- metabase_get_collections(api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$method, "GET")
  expect_equal(args$path, c("api", "collection"))
  expect_equal(res, list(id = 1, name = "Sales"))
})

test_that("metabase_get_cards ruft den richtigen Pfad auf", {
  args <- NULL
  mockery::stub(metabase_get_cards, "metabase_request",
                function(method, path, ...) {
                  args <<- list(method = method, path = path)
                  list(id = 2, name = "Umsatz")
                })

  res <- metabase_get_cards(api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$method, "GET")
  expect_equal(args$path, c("api", "card"))
  expect_equal(res, list(id = 2, name = "Umsatz"))
})

test_that("metabase_get_tables ruft den richtigen Pfad auf", {
  args <- NULL
  mockery::stub(metabase_get_tables, "metabase_request",
                function(method, path, ...) {
                  args <<- list(method = method, path = path)
                  list(id = 3, name = "orders")
                })

  res <- metabase_get_tables(api_key = "k", base_url = "https://mb.example.com")

  expect_equal(args$method, "GET")
  expect_equal(args$path, c("api", "table"))
  expect_equal(res, list(id = 3, name = "orders"))
})

# --- Unix socket (run-image --metabase) -------------------------------------

# A stand-in for the socket: metabase_request only checks that the path exists.
local_socket_file <- function(env = parent.frame()) {
  socket <- withr::local_tempfile(.local_envir = env)
  writeLines("", socket)
  socket
}

stub_metabase_perform <- function(env = parent.frame()) {
  captured <- new.env()
  mockery::stub(metabase_request, "httr2::req_perform", function(req, ...) {
    captured$req <- req
    structure(list(), class = "metabase_fake_response")
  })
  mockery::stub(metabase_request, "httr2::resp_status", function(resp) 200L)
  mockery::stub(metabase_request, "httr2::resp_body_json", function(resp, ...) list(ok = TRUE))
  assign("metabase_request", metabase_request, envir = env)
  captured
}

test_that("metabase_request sets the Unix socket option when unix_socket_path is given", {
  socket <- local_socket_file()
  captured <- stub_metabase_perform()

  metabase_request(
    "GET", c("api", "card", "42"),
    api_key          = "geheim",
    base_url         = "http://localhost",
    unix_socket_path = socket
  )

  expect_equal(captured$req$options$unix_socket_path, socket)
  expect_equal(captured$req$url, "http://localhost/api/card/42")
})

test_that("metabase_request sets no socket option without unix_socket_path", {
  captured <- stub_metabase_perform()

  metabase_request(
    "GET", c("api", "card", "42"),
    api_key  = "geheim",
    base_url = "https://metabase.example.com"
  )

  expect_null(captured$req$options$unix_socket_path)
  expect_equal(captured$req$url, "https://metabase.example.com/api/card/42")
})

test_that("metabase_request refuses an empty unix_socket_path and names METABASE_SOCKET", {
  mockery::stub(metabase_request, "httr2::req_perform",
                function(req, ...) stop("no request may be sent"))

  for (bad in list("", NA_character_, c("/a.sock", "/b.sock"), 1)) {
    expect_error(
      metabase_request("GET", c("api", "card"), api_key = "k",
                       base_url = "http://localhost", unix_socket_path = bad),
      "METABASE_SOCKET"
    )
  }
})

test_that("metabase_request refuses a socket path that does not exist", {
  mockery::stub(metabase_request, "httr2::req_perform",
                function(req, ...) stop("no request may be sent"))

  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k",
                     base_url = "http://localhost",
                     unix_socket_path = file.path(tempdir(), "no-such-metabase.sock")),
    "no socket at"
  )
})

test_that("metabase_request refuses a directory at the socket path", {
  mockery::stub(metabase_request, "httr2::req_perform",
                function(req, ...) stop("no request may be sent"))
  socket_dir <- withr::local_tempdir()

  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k",
                     base_url = "http://localhost", unix_socket_path = socket_dir),
    "no socket at"
  )
})

test_that("metabase_request refuses an NA base_url through a Unix socket", {
  socket <- local_socket_file()
  mockery::stub(metabase_request, "httr2::req_perform",
                function(req, ...) stop("no request may be sent"))

  expect_error(
    metabase_request("GET", c("api", "card"), api_key = "k",
                     base_url = NA_character_, unix_socket_path = socket),
    "http://"
  )
})

test_that("metabase_request refuses https through a Unix socket", {
  socket <- local_socket_file()
  mockery::stub(metabase_request, "httr2::req_perform",
                function(req, ...) stop("no request may be sent"))

  err <- expect_error(
    metabase_request("GET", c("api", "card"), api_key = "geheim",
                     base_url = "https://metabase.studyflix.info",
                     unix_socket_path = socket),
    "http://"
  )
  expect_false(grepl("geheim", conditionMessage(err), fixed = TRUE))
})

test_that("every exported Metabase function passes unix_socket_path on, NULL by default", {
  calls <- list(
    metabase_get_collections      = function(f, ...) f(api_key = "k", ...),
    metabase_get_collection_items = function(f, ...) f(7, api_key = "k", ...),
    metabase_get_cards            = function(f, ...) f(api_key = "k", ...),
    metabase_get_card             = function(f, ...) f(42, api_key = "k", ...),
    metabase_get_tables           = function(f, ...) f(api_key = "k", ...),
    metabase_update_card          = function(f, ...) f(9, list(name = "x"), api_key = "k", ...),
    metabase_compile_query        = function(f, ...) f(list(), api_key = "k", ...)
  )

  for (name in names(calls)) {
    seen <- "not called"
    fn <- get(name)
    mockery::stub(fn, "metabase_request", function(...) {
      seen <<- list(...)$unix_socket_path
      list(query = "SELECT 1")
    })

    calls[[name]](fn, base_url = "http://localhost", unix_socket_path = "/srv/x.sock")
    expect_identical(seen, "/srv/x.sock", info = name)

    calls[[name]](fn, base_url = "http://localhost")
    expect_null(seen, info = name)
  }
})

test_that("metabase_get_cards keeps its positional arguments (base-66 calls it so)", {
  args <- NULL
  mockery::stub(metabase_get_cards, "metabase_request",
                function(method, path, api_key, base_url, ...) {
                  args <<- c(list(api_key = api_key, base_url = base_url), list(...))
                  list()
                })

  metabase_get_cards("k", "http://127.0.0.1:3000")

  expect_equal(args$api_key, "k")
  expect_equal(args$base_url, "http://127.0.0.1:3000")
  expect_null(args$unix_socket_path)
})
