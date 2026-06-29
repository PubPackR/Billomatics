test_that("dsgvo_normalize_email lowercases + trims; phone canonicalises", {
  expect_equal(Billomatics::dsgvo_normalize_email("  A@B.DE "), "a@b.de")
  expect_equal(Billomatics::dsgvo_normalize_phone("+49 (0)30 12345678"), "493012345678")
})

test_that("dsgvo_hash_* is deterministic, pepper-dependent, format-insensitive, 64-hex", {
  expect_equal(Billomatics::dsgvo_hash_email("a@b.de", "p"), Billomatics::dsgvo_hash_email(" A@B.DE ", "p"))
  expect_false(Billomatics::dsgvo_hash_email("a@b.de", "p1") == Billomatics::dsgvo_hash_email("a@b.de", "p2"))
  expect_match(Billomatics::dsgvo_hash_email("a@b.de", "p"), "^[0-9a-f]{64}$")
  expect_equal(Billomatics::dsgvo_hash_phone("+49 30 12345678", "p"), Billomatics::dsgvo_hash_phone("0049 30 12345678", "p"))
})

test_that("dsgvo_email_tombstone is stable + person-bound", {
  h <- Billomatics::dsgvo_hash_email("a@b.de", "p")
  expect_equal(Billomatics::dsgvo_email_tombstone(h), paste0("[geloescht]-", h))
})

test_that("get_deletion_pepper reads env then key-file, errors when neither", {
  withr::with_envvar(c(DELETION_LOG_PEPPER = "secret-xyz"), {
    expect_equal(Billomatics::get_deletion_pepper(), "secret-xyz")
  })
  withr::with_envvar(c(DELETION_LOG_PEPPER = NA), {
    kf <- tempfile(); writeLines("file-pepper", kf); on.exit(unlink(kf))
    expect_equal(Billomatics::get_deletion_pepper(key_file = kf), "file-pepper")
    expect_error(Billomatics::get_deletion_pepper())
  })
})

make_cmr <- function() data.frame(
  callId = c("c1", "c2"),
  call_identity_mail = c("target@x.de", "keep@x.de"),
  call_identity_name = c("Target Person", "Keep Person"),
  stringsAsFactors = FALSE
)

test_that("dsgvo_suppress_msgraph_record tombstones matched mail + nulls name, leaves rest", {
  pepper <- "p"
  blocked <- Billomatics::dsgvo_hash_email("target@x.de", pepper)
  out <- Billomatics::dsgvo_suppress_msgraph_record(make_cmr(), blocked, pepper)
  expect_equal(out$call_identity_mail[1], Billomatics::dsgvo_email_tombstone(blocked))
  expect_true(is.na(out$call_identity_name[1]))
  expect_equal(out$call_identity_mail[2], "keep@x.de")
  expect_equal(out$call_identity_name[2], "Keep Person")
  expect_equal(nrow(out), 2L)
})

test_that("dsgvo_suppress_msgraph_record is a no-op for an empty blocklist", {
  out <- Billomatics::dsgvo_suppress_msgraph_record(make_cmr(), character(0), "p")
  expect_equal(out$call_identity_mail, c("target@x.de", "keep@x.de"))
})

test_that("dsgvo_load_suppression returns email + phone hash sets", {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:"); on.exit(DBI::dbDisconnect(con))
  DBI::dbExecute(con, "ATTACH DATABASE ':memory:' AS config")
  DBI::dbExecute(con, "CREATE TABLE config.privacy_deletion_log (id INTEGER, email_hash TEXT, phone_hashes TEXT)")
  DBI::dbExecute(con, "INSERT INTO config.privacy_deletion_log VALUES (1,'he1','{hp1,hp2}'),(2,'he2','{}')")
  sup <- Billomatics::dsgvo_load_suppression(con)
  expect_setequal(sup$email_hashes, c("he1", "he2"))
  expect_setequal(sup$phone_hashes, c("hp1", "hp2"))
})
