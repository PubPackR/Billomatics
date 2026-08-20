# Characterization tests for the 22 authentication_*() functions.
#
# These assert the CONTRACT each function exposes to authentication_process(),
# not its internals: which secret it asks for, and what shape it returns. The
# equivalence proof across both backends lives in Plan C2b and runs on the
# server; this file is what can be checked locally.

test_that("every plain-string service resolves through the seam", {
  # All twelve, not a sample. An earlier draft called three and claimed the
  # assertion proved no function reads a file directly; it proved nothing about
  # the other nine.
  plain_services <- list(
    list(fn = authentication_crm,                secret = "studyflix-crm-api-key"),
    list(fn = authentication_crm_lm,             secret = "studyflix-crm-lm-api-key"),
    list(fn = authentication_msgraph,            secret = "studyflix-msgraph-secret"),
    list(fn = authentication_msgraph_scoped_app, secret = "studyflix-msgraph-scoped-app-secret"),
    list(fn = authentication_brevo,              secret = "studyflix-brevo-smtp-key"),
    list(fn = authentication_bonus_db,           secret = "studyflix-bonusdb-key"),
    list(fn = authentication_cleverreach,        secret = "studyflix-cleverreach-token"),
    list(fn = authentication_gemini,             secret = "studyflix-gemini-api-key"),
    list(fn = authentication_openrouter,         secret = "studyflix-openrouter-api-key"),
    list(fn = authentication_openai_admin,       secret = "studyflix-openai-admin-api-key"),
    list(fn = authentication_github,             secret = "studyflix-github-token"),
    list(fn = authentication_metabase,           secret = "studyflix-metabase-api-key")
  )

  requested <- character(0)
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) {
      requested <<- c(requested, name)
      paste0("value-for-", name)
    }
  )

  for (s in plain_services) {
    expect_identical(s$fn("pw"), paste0("value-for-", s$secret))
  }
  # Order matters as much as membership: a copy-paste that gives two services
  # the same secret name would otherwise pass every individual assertion.
  expect_identical(requested, vapply(plain_services, `[[`, "", "secret"))
})

test_that("the plain-string services return a bare character scalar", {
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) "the-key"
  )
  out <- authentication_crm("pw")
  expect_type(out, "character")
  expect_length(out, 1L)
  expect_null(names(out))
})

test_that("the prompt reaches the seam, so an operator is told which service", {
  # Twelve identical "Enter the password:" prompts in one job is unusable.
  seen <- NULL
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) {
      seen <<- prompt
      "v"
    }
  )
  authentication_metabase("pw")
  expect_match(seen, "Metabase", fixed = TRUE)
})

# ---- billomat and asana: two DIFFERENT legacy data keys --------------------

test_that("billomat and asana take their data keys from separate secrets", {
  # base-02-asana_auswertung encrypts its pipeline output under the Asana
  # password and base-18 reads it back; ~49 other sites use the Billomat one.
  # Those are different strings, so one shared key would leave base-02's data
  # unreadable with no error at the time.
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) paste0("v-", name),
    .package = "secretsR"
  )
  b <- authentication_billomat("ignored")
  expect_length(b, 2L)
  expect_identical(b[1], "v-studyflix-legacy-data-key-billomat")
  expect_identical(b[2], "v-studyflix-billomat-api-key")

  a <- authentication_asana("ignored")
  expect_length(a, 2L)
  expect_identical(a[1], "v-studyflix-legacy-data-key-asana")
  expect_identical(a[2], "v-studyflix-asana-token")

  expect_false(identical(a[1], b[1]))
})

test_that("element [1] is the supplied password under the file backend", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) paste0("v-", name),
    .package = "secretsR"
  )
  # Under file the data key IS the password the caller passed - there is no
  # keys/ file holding it, because it is the key those files are encrypted with.
  expect_identical(authentication_billomat("pw")[1], "pw")
  expect_identical(authentication_asana("pw")[1], "pw")
})

test_that("billomat and asana prompt once, not twice", {
  # Two prompts means element [1] and the credential can derive from DIFFERENT
  # passwords if the operator types differently the second time - and element
  # [1] is what ~49 call sites use to decrypt base-data/. Nothing would detect
  # it.
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) file_key,
    .package = "secretsR"
  )
  local_mocked_bindings(billomatics_interactive = function() TRUE)

  for (fn in list(authentication_billomat, authentication_asana)) {
    prompts <- 0L
    local_mocked_bindings(
      getPass = function(msg) {
        prompts <<- prompts + 1L
        "typed"
      },
      .package = "getPass"
    )
    out <- fn(NULL)
    expect_identical(prompts, 1L)
    expect_identical(out[1], "typed")
    expect_identical(out[2], "typed")
  }
})

# ---- postgresql: one JSON secret on gsm, two files on file -----------------

test_that("postgresql composes character[5] from the single GSM secret", {
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      expect_identical(name, "studyflix-postgresql-connection")
      '{"host":"h.rds.amazonaws.com","port":"5432","dbname":"db","user":"u","password":"pw"}'
    },
    .package = "secretsR"
  )
  # The order postgres_connect() consumes, documented at postgres_connect.R:882-886.
  expect_identical(authentication_postgresql("ignored"),
                   c("pw", "u", "db", "h.rds.amazonaws.com", "5432"))
})

test_that("postgresql composes the same vector from the two legacy files", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      switch(name,
        "file:postgresql-credentials" = "pw",
        "file:postgresql-server"      = "u, db, h.rds.amazonaws.com, 5432",
        stop("unexpected name: ", name))
    },
    .package = "secretsR"
  )
  expect_identical(authentication_postgresql("pw-arg"),
                   c("pw", "u", "db", "h.rds.amazonaws.com", "5432"))
})

test_that("a malformed postgres secret does not reach the error message", {
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) "PGPASS-SECRET-abc123",
    .package = "secretsR"
  )
  msg <- tryCatch(authentication_postgresql("x"), error = conditionMessage)
  expect_false(grepl("PGPASS-SECRET-abc123", msg, fixed = TRUE))
})

test_that("a short server string fails loudly instead of returning a short vector", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      if (name == "file:postgresql-server") "u, db" else "pw"
    },
    .package = "secretsR"
  )
  # The original returned c(credentials, <n fields>) of whatever length, which
  # becomes a wrong postgres_connect() call rather than an error.
  expect_error(authentication_postgresql("pw"), "expected 4")
})

test_that("postgresql keeps its interactive short-circuit, bytes intact", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(billomatics_interactive = function() TRUE)
  # These are RETURN VALUES, not prompts: umlauts stay exactly as they are,
  # because a local call site may compare against the German string. Written as
  # \u00f6 escapes so this file stays ASCII.
  expect_identical(
    authentication_postgresql(character(0)),
    c("Postgres-Credentials werden lokal nicht ben\u00f6tigt",
      "Postgres-Server-Info wird lokal nicht ben\u00f6tigt")
  )
})

# ---- multi-secret services: one password, several secrets ------------------

test_that("the multi-secret services keep their list shapes and values", {
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) {
      switch(name,
        "studyflix-msgraph-delegated-secret"   = "cs",
        "studyflix-msgraph-delegated-storekey" = "sk",
        "studyflix-msgraph-sharepoint-config"  =
          '{"tenant_id":"t","client_id":"c","client_secret":"s","store_key":"k","store_path":"p","site_url":"u"}',
        "studyflix-personio-client-id"         = "cid",
        "studyflix-personio-client-secret"     = "csec",
        stop("unexpected name: ", name))
    }
  )
  d <- authentication_msgraph_delegated("pw")
  expect_identical(names(d), c("client_secret", "store_key"))
  # Values, not just names: swapping the two would pass a names-only assertion.
  expect_identical(d$client_secret, "cs")
  expect_identical(d$store_key, "sk")

  sp <- authentication_msgraph_sharepoint("pw")
  expect_setequal(names(sp), c("tenant_id", "client_id", "client_secret",
                               "store_key", "store_path", "site_url"))
  expect_identical(sp$site_url, "u")
  expect_identical(sp$client_secret, "s")
})

test_that("msgraph_sharepoint still rejects an incomplete config", {
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) '{"tenant_id":"t"}'
  )
  err <- tryCatch(authentication_msgraph_sharepoint("pw"), error = conditionMessage)
  expect_match(err, "unvollstaendig")
  # The missing fields must be named, or an operator cannot act on it.
  expect_match(err, "client_id")
})

test_that("a malformed sharepoint config does not reach the error message", {
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) "SP-SECRET-abc123"
  )
  msg <- tryCatch(authentication_msgraph_sharepoint("pw"), error = conditionMessage)
  expect_false(grepl("SP-SECRET-abc123", msg, fixed = TRUE))
})

test_that("personio returns its three-element list and does not swap the fields", {
  local_mocked_bindings(
    billomatics_secret = function(name, args, prompt = "", key = NULL) {
      if (name == "studyflix-personio-client-id") "cid" else "csec"
    }
  )
  local_mocked_bindings(
    POST        = function(...) structure(list(), class = "response"),
    status_code = function(r) 200L,
    content     = function(r, as) list(data = list(token = "tok")),
    .package = "httr"
  )
  out <- authentication_personio("pw")
  expect_identical(names(out), c("client_id", "client_secret", "access_token"))
  expect_identical(out$client_id, "cid")
  expect_identical(out$client_secret, "csec")
  expect_identical(out$access_token, "tok")
})

test_that("each multi-secret service prompts once, not per secret", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      if (grepl("sharepoint", name)) {
        '{"tenant_id":"t","client_id":"c","client_secret":"s","store_key":"k","store_path":"p","site_url":"u"}'
      } else "v"
    },
    .package = "secretsR"
  )
  local_mocked_bindings(billomatics_interactive = function() TRUE)
  local_mocked_bindings(
    POST        = function(...) structure(list(), class = "response"),
    status_code = function(r) 200L,
    content     = function(r, as) list(data = list(token = "tok")),
    .package = "httr"
  )
  for (fn in list(authentication_msgraph_delegated, authentication_personio)) {
    prompts <- 0L
    local_mocked_bindings(
      getPass = function(msg) { prompts <<- prompts + 1L; "typed" },
      .package = "getPass"
    )
    fn(NULL)
    expect_identical(prompts, 1L)
  }
})

# ---- the four service-account JSON services --------------------------------

test_that("the service-account services signal failure instead of reporting success", {
  # Today all four report success whatever happens, in THREE different ways:
  # GSheet and Google_Analytics end their finally block with return("No Key");
  # Google_BigQuery returns its own 96-character error message where a
  # credential belongs; Google_BigQuery_GA4 returns NULL on success and a
  # string on failure, so the natural is.null() guard is inverted.
  local_mocked_bindings(billomatics_require = function(pkg) invisible(NULL))
  local_mocked_bindings(
    billomatics_sa_json = function(name, args, prompt = "") stop("decryption failed")
  )
  expect_error(authentication_GSheet("pw"), "decryption failed")
  expect_error(authentication_Google_Analytics("pw"), "decryption failed")
  expect_error(authentication_Google_BigQuery("pw"), "decryption failed")
  expect_error(authentication_Google_BigQuery_GA4("pw"), "decryption failed")
})

test_that("the JSON actually reaches the Google client", {
  # Without capturing the argument, deleting the gs4_auth() call entirely leaves
  # the test green: it would assert only the return value, which is a constant.
  seen <- NULL
  local_mocked_bindings(
    billomatics_sa_json = function(name, args, prompt = "") '{"type":"service_account"}'
  )
  local_mocked_bindings(
    gs4_auth = function(path) { seen <<- path; invisible(NULL) },
    .package = "googlesheets4"
  )
  out <- authentication_GSheet("pw")
  expect_identical(seen, '{"type":"service_account"}')
  expect_identical(out, "OK")
})

test_that("each service asks for its own secret", {
  asked <- character(0)
  local_mocked_bindings(billomatics_require = function(pkg) invisible(NULL))
  local_mocked_bindings(
    billomatics_sa_json = function(name, args, prompt = "") {
      asked <<- c(asked, name)
      stop("stop here, the Google clients are not the subject")
    }
  )
  for (fn in list(authentication_GSheet, authentication_Google_Analytics,
                  authentication_Google_BigQuery, authentication_Google_BigQuery_GA4)) {
    try(fn("pw"), silent = TRUE)
  }
  expect_identical(asked, c("studyflix-gsheets-service-account",
                            "studyflix-google-analytics-service-account",
                            "studyflix-bigquery-gsc-service-account",
                            "studyflix-bigquery-ga4-service-account"))
})

test_that("a BigQuery service refuses actionably when bigrquery is absent", {
  # bigrquery is a Suggests, so this is a real condition rather than a mocked
  # one on any host that has not installed it. Requiring it BEFORE the decrypt
  # means a credential is not decrypted just to be thrown away.
  skip_if(requireNamespace("bigrquery", quietly = TRUE),
          "bigrquery is installed here, so absence cannot be exercised")
  expect_error(authentication_Google_BigQuery("pw"),
               "is required for this authentication service")
})

test_that("returning OK keeps authentication_process's list at full length", {
  # keys[[service]] <- NULL REMOVES the element. Returning invisible(NULL) would
  # shrink a 22-service call to 18 entries, changing the documented return shape
  # of the one function this plan does not touch.
  keys <- list()
  keys[["postgresql"]] <- c("a", "b")
  keys[["google sheet"]] <- "OK"
  expect_length(keys, 2L)
  expect_true("google sheet" %in% names(keys))

  shrunk <- list()
  shrunk[["postgresql"]] <- c("a", "b")
  shrunk[["google sheet"]] <- NULL
  expect_length(shrunk, 1L)
  expect_false("google sheet" %in% names(shrunk))
})
