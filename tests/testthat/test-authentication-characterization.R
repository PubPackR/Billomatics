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
