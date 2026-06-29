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
