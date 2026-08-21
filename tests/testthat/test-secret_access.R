test_that("billomatics_file_key normalises every shape args arrives in", {
  # FlowForce: commandArgs() gives a character vector, so args[pos] is character[1].
  expect_identical(billomatics_file_key("pw"), "pw")
  # Shiny: args is list(shinymanager::custom_access_keys_2(...)), so args[pos]
  # is a length-1 LIST. secret_get() validates file_key as a character scalar
  # and rejects a list outright.
  expect_identical(billomatics_file_key(list("pw")), "pw")
  expect_null(billomatics_file_key(character(0)))
  expect_null(billomatics_file_key(NA_character_))
  expect_null(billomatics_file_key(NULL))
  expect_null(billomatics_file_key(list()))
  expect_null(billomatics_file_key(""))
  # The realistic Shiny miss: needed_services longer than the supplied keys, so
  # args[pos] is list(NULL). This is the shape most likely to occur in
  # production and it went untested through three drafts of the plan.
  expect_null(billomatics_file_key(list(NULL)))
  expect_null(billomatics_file_key(list(NA_character_)))
})

test_that("billomatics_secret passes the per-call key to the file backend", {
  seen <- NULL
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      seen <<- list(name = name, file_key = file_key)
      "value"
    },
    secret_backend = function() "file",
    .package = "secretsR"
  )
  expect_identical(billomatics_secret("studyflix-crm-api-key", "pw"), "value")
  expect_identical(seen$name, "studyflix-crm-api-key")
  # Per call, never a process-wide SF_SECRET_FILE_KEY: each service has its own
  # password (design 3.3).
  expect_identical(seen$file_key, "pw")
})

test_that("billomatics_secret does not prompt under the gsm backend", {
  # The mock returns a constant keyed on nothing, so it also has to ASSERT the
  # requested name -- otherwise a mutation that requests the wrong secret is
  # caught only incidentally, by whichever other test happens to route through
  # this function.
  asked <- NULL
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      asked <<- name
      "value"
    },
    secret_backend = function() "gsm",
    .package = "secretsR"
  )
  local_mocked_bindings(billomatics_interactive = function() TRUE)
  local_mocked_bindings(
    getPass = function(msg) stop("must not prompt under gsm"),
    .package = "getPass"
  )
  expect_identical(billomatics_secret("studyflix-crm-api-key", NULL), "value")
  expect_identical(asked, "studyflix-crm-api-key")
})

test_that("a key resolved once is reused, not prompted for twice", {
  # The defect this guards: billomat and asana derive element [1] and the API
  # key from the same password. Two prompts means an operator who types
  # differently the second time gets a data key that does not match the
  # credential, and nothing detects it.
  prompts <- 0L
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(billomatics_interactive = function() TRUE)
  local_mocked_bindings(
    getPass = function(msg) {
      prompts <<- prompts + 1L
      "typed"
    },
    .package = "getPass"
  )
  expect_identical(billomatics_resolve_key(NULL, "enter: "), "typed")
  expect_identical(prompts, 1L)
})

test_that("a missing key in a non-interactive session errors instead of hanging", {
  # getPass() blocks on stdin forever in a FlowForce job. Crash instead.
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(billomatics_interactive = function() FALSE)
  expect_error(billomatics_resolve_key(NULL, "enter: "), "not interactive")
})

test_that("billomatics_parse_json never puts the payload in the error", {
  # jsonlite's lexer error reproduces its input verbatim, and these payloads are
  # credentials. Without this wrapper a malformed secret lands in the FlowForce
  # log in full.
  msg <- tryCatch(billomatics_parse_json("SUPER-SECRET-abc123", "studyflix-x"),
                  error = conditionMessage)
  expect_false(grepl("SUPER-SECRET-abc123", msg, fixed = TRUE))
  expect_match(msg, "studyflix-x")
  expect_identical(billomatics_parse_json('{"a":"b"}', "studyflix-x")$a, "b")
})

test_that("billomatics_legacy_data_key rejects an unknown key and picks the right secret", {
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) name,
    .package = "secretsR"
  )
  expect_identical(billomatics_legacy_data_key("billomat", NULL),
                   "studyflix-legacy-data-key-billomat")
  expect_identical(billomatics_legacy_data_key("asana", NULL),
                   "studyflix-legacy-data-key-asana")
  expect_error(billomatics_legacy_data_key("crm", NULL), "unknown legacy data key")
})

test_that("billomatics_sa_json decrypts into a private directory that is removed", {
  skip_if_not_installed("safer")
  plain <- withr::local_tempfile(fileext = ".json")
  writeLines('{"type":"service_account"}', plain)
  enc <- withr::local_tempfile(fileext = ".bin")
  # safer::decrypt_file() asserts !file.exists(outfile), so the implementation
  # must NOT pre-create the target. That assertion is why the previous design
  # failed 100% of the time.
  safer::encrypt_file(infile = plain, key = "pw", outfile = enc)

  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  local_mocked_bindings(billomatics_sa_encrypted_path = function(name) enc)
  seen_dir <- NULL
  local_mocked_bindings(
    billomatics_sa_dir = function() {
      seen_dir <<- file.path(tempdir(), paste0("sa-test-", as.integer(runif(1) * 1e6)))
      dir.create(seen_dir, mode = "0700")
      seen_dir
    }
  )
  out <- billomatics_sa_json("studyflix-gsheets-service-account", "pw")
  expect_match(out, "service_account", fixed = TRUE)
  # Assert on the specific directory, not a tempdir() glob: a glob fails on any
  # unrelated leftover in the session's tempdir.
  expect_false(dir.exists(seen_dir))
})

test_that("billomatics_sa_encrypted_path covers exactly the four blob services", {
  expect_match(billomatics_sa_encrypted_path("studyflix-gsheets-service-account"),
               "encrypted_google_sheets.bin", fixed = TRUE)
  expect_match(billomatics_sa_encrypted_path("studyflix-bigquery-ga4-service-account"),
               "encrypted_ga4_bigquery.bin", fixed = TRUE)
  expect_error(billomatics_sa_encrypted_path("studyflix-crm-api-key"),
               "unknown service-account secret")
})

test_that("billomatics_require errors actionably for an absent optional package", {
  # bigrquery is a Suggests. A hard Imports would make installing this package
  # fail on every host that lacks it.
  expect_error(billomatics_require("a.package.that.does.not.exist"),
               "is required for this authentication service")
  expect_null(billomatics_require("stats"))
})

test_that("get_billomatApiKey never prints the password", {
  # The previous implementation ran message("key is set to: ", key) -- the
  # password, on stderr, on every call, from an EXPORTED function in a public
  # repository. That is the single most direct credential leak in the package.
  local_mocked_bindings(billomatics_secret = function(name, args, prompt = "", key = NULL) "apikey")
  expect_silent(out <- get_billomatApiKey("../../keys"))
  expect_identical(out, "apikey")
})

test_that("get_billomatApiKey returns invisibly, as it always did", {
  local_mocked_bindings(billomatics_secret = function(name, args, prompt = "", key = NULL) "apikey")
  expect_invisible(get_billomatApiKey())
})

test_that("get_billomatApiKey_server keeps working from the environment off gsm", {
  # ENCRYPTION_PAYLOAD/ENCRYPTION_SECRET are live GitHub Actions secrets, and a
  # CI runner has no keys/ checkout. Routing this through the file backend would
  # turn a working function into one that always errors.
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  payload <- safer::encrypt_string("apikey", key = "envkey")
  withr::with_envvar(c(ENCRYPTION_PAYLOAD = payload, ENCRYPTION_SECRET = "envkey"), {
    expect_identical(get_billomatApiKey_server(), "apikey")
  })
})

test_that("get_billomatApiKey_server uses Secret Manager under gsm", {
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      expect_identical(name, "studyflix-billomat-api-key")
      "from-gsm"
    },
    .package = "secretsR"
  )
  expect_identical(get_billomatApiKey_server(), "from-gsm")
})

test_that("get_billomatApiKey_server says why, rather than failing obscurely", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  withr::with_envvar(c(ENCRYPTION_PAYLOAD = NA, ENCRYPTION_SECRET = NA), {
    expect_error(get_billomatApiKey_server(), "ENCRYPTION_PAYLOAD")
  })
})

test_that("get_deletion_pepper prefers secret_get, and keeps its env fallback off gsm", {
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      expect_identical(name, "studyflix-deletion-log-pepper")
      "pepper-from-gsm"
    },
    .package = "secretsR"
  )
  withr::with_envvar(c(DELETION_LOG_PEPPER = "pepper-from-env"), {
    expect_identical(get_deletion_pepper(), "pepper-from-gsm")
  })
})

test_that("get_deletion_pepper still honours the env var and the key file off gsm", {
  local_mocked_bindings(secret_backend = function() "file", .package = "secretsR")
  withr::with_envvar(c(DELETION_LOG_PEPPER = "pepper-from-env"), {
    expect_identical(get_deletion_pepper(), "pepper-from-env")
  })
  kf <- withr::local_tempfile()
  writeLines("  pepper-from-file  ", kf)
  withr::with_envvar(c(DELETION_LOG_PEPPER = NA), {
    expect_identical(get_deletion_pepper(key_file = kf), "pepper-from-file")
    expect_error(get_deletion_pepper(), "Pepper")
  })
})

test_that("billomatics_sa_json reads the secret directly under gsm", {
  # Found by review: this branch had zero coverage. Mutating it to request a
  # different secret name left 41/41 tests passing, because the only sa_json
  # test exercised the file branch and the four service-account tests mock
  # sa_json itself, never entering its body. Dormant while file is the default;
  # Plan C2b depends on this path.
  seen <- NULL
  local_mocked_bindings(secret_backend = function() "gsm", .package = "secretsR")
  local_mocked_bindings(
    secret_get = function(name, version = "latest", file_key = NULL) {
      seen <<- name
      '{"type":"service_account"}'
    },
    .package = "secretsR"
  )
  # Under gsm nothing should touch the legacy encrypted-file path at all.
  local_mocked_bindings(
    billomatics_sa_encrypted_path = function(name) stop("must not resolve a keys/ path under gsm")
  )
  out <- billomatics_sa_json("studyflix-gsheets-service-account", NULL)
  expect_identical(seen, "studyflix-gsheets-service-account")
  expect_identical(out, '{"type":"service_account"}')
})
