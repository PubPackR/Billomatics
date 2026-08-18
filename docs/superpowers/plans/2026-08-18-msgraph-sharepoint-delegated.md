# MSGraph-SharePoint Delegated Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delegierter MS-Graph-SharePoint-Zugriff (Refresh-Token-Rotation, n8n-App) als Billomatics-Service `msgraph_sharepoint` + delegierte File-Funktionen in Package-01-MSGraph, Migration base-18.

**Architecture:** Billomatics liefert Auth (Key-JSON entschlüsseln, Token-Provider mit rotierendem verschlüsseltem Refresh-Token-Store, Headless-Bootstrap). Package-01-MSGraph liefert delegierte Pendants der SharePoint-File-Funktionen (Drive-Auflösung über Site-URL statt Gruppen-GUID). base-18 wird als erster Konsument migriert. Spec: `docs/superpowers/specs/2026-08-18-msgraph-sharepoint-delegated-design.md` (im Billomatics-Repo).

**Tech Stack:** R, httr, jsonlite, safer, testthat (edition 3) + mockery/withr, roxygen2.

## Global Constraints

- **Bestehende Funktionen niemals entfernen oder umbenennen** — nur neue Funktionen mit neuen Namen (`*_delegated`, `msgraph_sharepoint_*`). Alte app-only-Funktionen bleiben lauffähig stehen.
- Neue Funktionen mit deutscher roxygen2-Doku im Bestandsstil, Funktionskörper beginnt mit `# ---- start ---- #`-Marker.
- Kein blanket `tryCatch` — Fehler crashen laut mit AADSTS-Code/HTTP-Status (FlowForce-Prinzip).
- Keine echten Secrets in Code, Tests oder Fixtures — nur Dummy-Werte.
- Interne Billomatics-Helfer (Store/Refresh) werden **nicht exportiert** (kein `@export`) — Namenskonflikt mit dbconnectorR vermeiden. Tests greifen per `Billomatics:::` zu.
- Key-JSON-Felder exakt: `tenant_id`, `client_id`, `client_secret`, `store_key`, `store_path`, `site_url`. Store-JSON-Felder exakt: `refresh_token`, `obtained_at`, `last_refreshed_at`, `tenant_id`, `client_id`, `scopes`.
- Default-Scopes exakt: `https://graph.microsoft.com/Files.ReadWrite.All`, `https://graph.microsoft.com/User.Read`, `offline_access`.
- Repos/Branches: **Billomatics** → Branch `feat/msgraph-sharepoint-delegated` (existiert, enthält Spec-Commit; die unstaged Änderung an `man/update_crm_company.Rd` NICHT anfassen/stagen). **Package-01-MSGraph** → neuer Branch `feat/delegated-sharepoint` off `master` (untracked `MSGraph_*.md/pdf/html`, `.vscode/`, `nul` NICHT anfassen). **base-18** → zuerst `git checkout main && git pull` (lokal 1 Commit hinter origin), dann neuer Branch `feat/msgraph-sharepoint-delegated`.
- Arbeitsverzeichnis für Tests/Kommandos: das jeweilige Repo-Root (`C:\Users\HEMM036\Github\packages\Billomatics` usw.).
- Nur selbst erstellte/geänderte Dateien stagen — nie `git add -A`.

**Scope-Hinweis:** Die Migration der übrigen Konsumenten-Repos (base-14, base-15, base-07, base-19, base-48, base-43, shiny-29, shiny-99-modules, base-11) folgt NACH diesem Plan nach dem in Task 10 etablierten Muster (Spec §6) — bewusst nicht Teil dieses Plans. Server-Setup (Spec §5) ist human-only und in Task 11 als Runbook dokumentiert.

---

### Task 1: Billomatics — interne Store- und Refresh-Helfer

**Files:**
- Create: `packages/Billomatics/R/msgraph_sharepoint_auth.R`
- Test: `packages/Billomatics/tests/testthat/test-msgraph_sharepoint_auth.R`

**Interfaces:**
- Consumes: nichts (Basisschicht).
- Produces (intern, nicht exportiert):
  - `msgraph_sp_store_read(path, key) -> list(refresh_token, obtained_at, last_refreshed_at, tenant_id, client_id, scopes)`
  - `msgraph_sp_store_write(path, key, store) -> invisible(path)` (atomar: tmp + rename, Vorgänger als `.bak`)
  - `msgraph_sp_refresh(tenant_id, client_id, client_secret, refresh_token, scopes) -> list(access_token, expires_in, refresh_token?)` (wirft bei HTTP != 200 mit AADSTS-Code)
  - Konstante `.msgraph_sp_default_scopes` (character(3), siehe Global Constraints)

- [ ] **Step 1: Failing Tests schreiben**

`packages/Billomatics/tests/testthat/test-msgraph_sharepoint_auth.R`:

```r
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

test_that("Refresh parst Erfolgsantwort", {
  fake_response <- structure(list(status_code = 200L), class = "response")
  mockery::stub(Billomatics:::msgraph_sp_refresh, "httr::POST", fake_response)
  mockery::stub(Billomatics:::msgraph_sp_refresh, "httr::content",
                list(access_token = "at", expires_in = 3599, refresh_token = "rt-neu"))
  got <- Billomatics:::msgraph_sp_refresh("tid", "cid", "sec", "rt-alt", c("s1"))
  expect_equal(got$access_token, "at")
  expect_equal(got$refresh_token, "rt-neu")
})

test_that("Refresh wirft mit AADSTS-Code bei HTTP != 200", {
  fake_response <- structure(list(status_code = 400L), class = "response")
  mockery::stub(Billomatics:::msgraph_sp_refresh, "httr::POST", fake_response)
  mockery::stub(Billomatics:::msgraph_sp_refresh, "httr::content",
                list(error = "invalid_grant",
                     error_description = "AADSTS70008: expired"))
  expect_error(
    Billomatics:::msgraph_sp_refresh("tid", "cid", "sec", "rt", c("s1")),
    "invalid_grant.*AADSTS70008|AADSTS70008", perl = TRUE)
})
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run (im Billomatics-Root): `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_sharepoint_auth.R')"`
Expected: FAIL mit „object 'msgraph_sp_store_write' not found" (o. ä.).

- [ ] **Step 3: Implementierung schreiben**

`packages/Billomatics/R/msgraph_sharepoint_auth.R` (Datei-Anfang; die Datei wächst in Task 2/4 weiter):

```r
################################################################################-
# ----- Description -------------------------------------------------------------
#
# Delegierte MSGraph-Auth fuer SharePoint-Dateizugriff (Service msgraph_sharepoint).
# Authorization-Code-Flow mit Confidential Client: einmaliger Bootstrap auf dem
# Server, danach unbeaufsichtigte Refreshs aus einem verschluesselten Token-Store
# mit Rotation (n8n-Muster). Ein gemeinsamer Store fuer alle Konsumenten-Repos.
# Spec: docs/superpowers/specs/2026-08-18-msgraph-sharepoint-delegated-design.md
#
# Interne Helfer (msgraph_sp_*) sind bewusst NICHT exportiert - Namenskonflikte
# mit dbconnectorR (base-62) vermeiden.
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/08
#

.msgraph_sp_default_scopes <- c(
  "https://graph.microsoft.com/Files.ReadWrite.All",
  "https://graph.microsoft.com/User.Read",
  "offline_access")

#' Verschluesselten SharePoint-Token-Store lesen (intern)
#'
#' @param path Pfad zur verschluesselten Store-Datei.
#' @param key Entschluesselungsschluessel.
#' @return Liste mit refresh_token, obtained_at, last_refreshed_at, tenant_id,
#'   client_id, scopes.
#' @noRd
msgraph_sp_store_read <- function(path, key) {
  # ---- start ---- #
  if (!file.exists(path)) {
    stop("Token-Store nicht gefunden: ", path,
         "\nmsgraph_sharepoint_bootstrap() ausfuehren, um ihn anzulegen.",
         call. = FALSE)
  }
  cipher <- paste(readLines(path, warn = FALSE), collapse = "")
  json <- safer::decrypt_string(cipher, key = key)
  jsonlite::fromJSON(json, simplifyVector = TRUE)
}

#' Verschluesselten SharePoint-Token-Store schreiben (intern)
#'
#' Schreibt ueber eine Temp-Datei und legt den Vorgaenger als .bak ab.
#'
#' @param path Zielpfad.
#' @param key Verschluesselungsschluessel.
#' @param store Liste wie von msgraph_sp_store_read() geliefert.
#' @return invisible(path)
#' @noRd
msgraph_sp_store_write <- function(path, key, store) {
  # ---- start ---- #
  json <- as.character(jsonlite::toJSON(store, auto_unbox = TRUE))
  cipher <- safer::encrypt_string(json, key = key)
  # Zeilenumbrueche im Chiffrat wuerden das einzeilige Wiedereinlesen zerstoeren
  cipher <- gsub("[\r\n]", "", cipher)

  tmp <- paste0(path, ".tmp")
  writeLines(cipher, tmp)

  if (file.exists(path)) {
    file.copy(path, paste0(path, ".bak"), overwrite = TRUE)
    # file.rename() scheitert unter Windows, wenn das Ziel existiert
    unlink(path)
  }
  if (!file.rename(tmp, path)) {
    unlink(tmp)
    stop("Token-Store konnte nicht ersetzt werden: ", path, call. = FALSE)
  }
  invisible(path)
}

#' Access-Token ueber ein Refresh-Token erneuern (intern)
#'
#' Confidential Client: client_secret geht mit. Wirft bei HTTP != 200 mit dem
#' AADSTS-Code, damit im Log sofort die Ursache steht.
#'
#' @param tenant_id Tenant-ID.
#' @param client_id Client-ID der App-Registrierung.
#' @param client_secret Client-Secret.
#' @param refresh_token Aktuelles Refresh-Token.
#' @param scopes Character-Vektor der Scopes.
#' @return Geparste Entra-Antwort (access_token, expires_in, meist refresh_token).
#' @noRd
msgraph_sp_refresh <- function(tenant_id, client_id, client_secret,
                               refresh_token, scopes) {
  # ---- start ---- #
  uri <- paste0("https://login.microsoftonline.com/", tenant_id, "/oauth2/v2.0/token")
  response <- httr::POST(uri, encode = "form", body = list(
    grant_type    = "refresh_token",
    client_id     = client_id,
    client_secret = client_secret,
    refresh_token = refresh_token,
    scope         = paste(scopes, collapse = " ")
  ))
  parsed <- httr::content(response, as = "parsed", type = "application/json")
  status <- response$status_code
  if (status != 200) {
    error_code <- if (is.null(parsed$error)) "unbekannt" else parsed$error
    error_desc <- if (is.null(parsed$error_description)) "keine Beschreibung" else parsed$error_description
    stop(sprintf(paste0(
      "MSGraph-SharePoint-Token-Refresh fehlgeschlagen (HTTP %s): %s\n%s\n",
      "msgraph_sharepoint_bootstrap() erneut ausfuehren, um ein neues Refresh-Token zu holen."),
      status, error_code, error_desc), call. = FALSE)
  }
  parsed
}
```

- [ ] **Step 4: Tests laufen lassen — müssen bestehen**

Run: `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_sharepoint_auth.R')"`
Expected: alle PASS.

- [ ] **Step 5: Commit**

```bash
cd C:/Users/HEMM036/Github/packages/Billomatics
git add R/msgraph_sharepoint_auth.R tests/testthat/test-msgraph_sharepoint_auth.R
git commit -m "feat(msgraph-sharepoint): interne Store- und Refresh-Helfer"
```

---

### Task 2: Billomatics — Token-Provider mit Session-Cache und Rotation

**Files:**
- Modify: `packages/Billomatics/R/msgraph_sharepoint_auth.R` (ans Dateiende anhängen)
- Test: `packages/Billomatics/tests/testthat/test-msgraph_sharepoint_auth.R` (anhängen)

**Interfaces:**
- Consumes: `msgraph_sp_store_read/write`, `msgraph_sp_refresh`, `.msgraph_sp_default_scopes` (Task 1).
- Produces (exportiert):
  - `msgraph_sharepoint_token_provider(auth, scopes = .msgraph_sp_default_scopes, refresh_buffer_seconds = 300, warn_inactive_days = 60) -> function(force_refresh = FALSE) -> character(1)` — `auth` ist die Liste aus `authentication_msgraph_sharepoint()` (Task 3); Provider wird pro (client_id, store_path) im Package-Env gecacht.
  - intern: `msgraph_sp_provider_cache_clear()` (für Tests).

- [ ] **Step 1: Failing Tests anhängen**

```r
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

  refresh_mock <- mockery::mock(
    list(access_token = "at-1", expires_in = 3599, refresh_token = "rt-2"))
  mockery::stub(Billomatics:::msgraph_sp_provider_build, "msgraph_sp_refresh", refresh_mock)

  provider <- Billomatics::msgraph_sharepoint_token_provider(make_test_auth(store_path))
  expect_equal(provider(), "at-1")
  expect_equal(provider(), "at-1")            # zweiter Call: aus dem Cache
  mockery::expect_called(refresh_mock, 1)     # nur EIN Refresh-POST
  # Rotation wurde persistiert
  expect_equal(
    Billomatics:::msgraph_sp_store_read(store_path, "test-store-key")$refresh_token,
    "rt-2")
})

test_that("Antwort ohne neues Refresh-Token laesst Store unveraendert", {
  withr::defer(Billomatics:::msgraph_sp_provider_cache_clear())
  store_path <- file.path(withr::local_tempdir(), "store.txt")
  write_test_store(store_path)
  mockery::stub(Billomatics:::msgraph_sp_provider_build, "msgraph_sp_refresh",
                list(access_token = "at-1", expires_in = 3599))
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
  mockery::stub(Billomatics:::msgraph_sp_provider_build, "msgraph_sp_refresh",
                list(access_token = "at-1", expires_in = 3599))
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
```

**Hinweis für den Implementierer:** Damit `mockery::stub()` auf den Refresh greifen kann, wird der eigentliche Closure-Bau in eine eigene interne Funktion `msgraph_sp_provider_build()` gezogen; `msgraph_sharepoint_token_provider()` macht nur Cache-Lookup + Delegation.

- [ ] **Step 2: Tests laufen lassen — neue Tests müssen fehlschlagen**

Run: `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_sharepoint_auth.R')"`
Expected: Task-1-Tests PASS, neue Tests FAIL („msgraph_sharepoint_token_provider not found").

- [ ] **Step 3: Implementierung anhängen**

```r
.msgraph_sp_provider_cache <- new.env(parent = emptyenv())

#' Provider-Cache leeren (intern, fuer Tests)
#' @noRd
msgraph_sp_provider_cache_clear <- function() {
  # ---- start ---- #
  rm(list = ls(.msgraph_sp_provider_cache), envir = .msgraph_sp_provider_cache)
  invisible(NULL)
}

#' Provider-Closure bauen (intern)
#' @noRd
msgraph_sp_provider_build <- function(auth, scopes, refresh_buffer_seconds,
                                      warn_inactive_days) {
  # ---- start ---- #
  cache <- new.env(parent = emptyenv())
  cache$token <- NULL
  cache$exp <- as.POSIXct(NA)

  function(force_refresh = FALSE) {
    now <- Sys.time()
    needs_refresh <- force_refresh ||
      is.null(cache$token) ||
      is.na(cache$exp) ||
      as.numeric(difftime(cache$exp, now, units = "secs")) < refresh_buffer_seconds
    if (!needs_refresh) {
      return(cache$token)
    }

    store <- msgraph_sp_store_read(auth$store_path, auth$store_key)

    last <- as.POSIXct(store$last_refreshed_at,
                       format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    inactive_days <- as.numeric(difftime(now, last, units = "days"))
    if (is.finite(inactive_days) && inactive_days > warn_inactive_days) {
      warning(sprintf(paste0(
        "MSGraph-SharePoint: letzter erfolgreicher Refresh ist %.0f Tage her ",
        "(Warnschwelle %s Tage). Refresh-Token verfaellt nach ~90 Tagen Inaktivitaet."),
        inactive_days, warn_inactive_days), call. = FALSE)
    }

    credentials <- msgraph_sp_refresh(auth$tenant_id, auth$client_id,
                                      auth$client_secret, store$refresh_token, scopes)

    rotated <- !is.null(credentials$refresh_token) &&
      !identical(credentials$refresh_token, store$refresh_token)
    if (rotated) {
      store$refresh_token <- credentials$refresh_token
      store$last_refreshed_at <- format(now, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      msgraph_sp_store_write(auth$store_path, auth$store_key, store)
    }

    cache$token <- credentials$access_token
    cache$exp <- now + as.numeric(credentials$expires_in)
    cache$token
  }
}

#' Token-Provider fuer den delegierten SharePoint-Zugriff
#'
#' Liefert eine Closure `function(force_refresh = FALSE)` mit dem Access-Token
#' als String — gleiche Signatur wie die uebrigen Token-Provider im Stack.
#' Der Provider wird pro (client_id, store_path) fuer die Prozesslaufzeit
#' gecacht: mehrere SharePoint-Calls in einem Skript teilen sich ein
#' Access-Token statt jedes Mal zu refreshen. Bei Rotation wird das neue
#' Refresh-Token sofort in den verschluesselten Store geschrieben.
#'
#' @param auth Liste aus authentication_process()$msgraph_sharepoint
#'   (tenant_id, client_id, client_secret, store_key, store_path, site_url).
#' @param scopes Character-Vektor der Scopes.
#' @param refresh_buffer_seconds Vorlauf, ab dem vorsorglich erneuert wird.
#' @param warn_inactive_days Tage seit letztem Refresh, ab denen gewarnt wird.
#' @return function(force_refresh = FALSE), liefert das Access-Token als String.
#' @export
msgraph_sharepoint_token_provider <- function(auth,
                                              scopes = .msgraph_sp_default_scopes,
                                              refresh_buffer_seconds = 300,
                                              warn_inactive_days = 60) {
  # ---- start ---- #
  cache_key <- paste(auth$client_id, auth$store_path, sep = "|")
  existing <- .msgraph_sp_provider_cache[[cache_key]]
  if (!is.null(existing)) {
    return(existing)
  }
  provider <- msgraph_sp_provider_build(auth, scopes, refresh_buffer_seconds,
                                        warn_inactive_days)
  .msgraph_sp_provider_cache[[cache_key]] <- provider
  provider
}
```

- [ ] **Step 4: Tests laufen lassen — müssen bestehen**

Run: `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_sharepoint_auth.R')"`
Expected: alle PASS.

- [ ] **Step 5: Commit**

```bash
git add R/msgraph_sharepoint_auth.R tests/testthat/test-msgraph_sharepoint_auth.R
git commit -m "feat(msgraph-sharepoint): Token-Provider mit Session-Cache und Rotation"
```

---

### Task 3: Billomatics — Service `msgraph_sharepoint` in authentication_process

**Files:**
- Modify: `packages/Billomatics/R/general_authentication.R` (Service-Registrierung Zeile ~42-66 + neue Funktion nach `authentication_msgraph_delegated`, Zeile ~284)
- Test: `packages/Billomatics/tests/testthat/test-msgraph_sharepoint_auth.R` (anhängen)

**Interfaces:**
- Consumes: nichts Neues.
- Produces (exportiert via authentication_process-Dispatch):
  - `authentication_msgraph_sharepoint(args) -> list(tenant_id, client_id, client_secret, store_key, store_path, site_url)`
  - Service-Name in `authentication_process()`: `"msgraph_sharepoint"`

- [ ] **Step 1: Failing Test anhängen**

```r
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
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run: `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_sharepoint_auth.R')"`
Expected: neue Tests FAIL („authentication_msgraph_sharepoint not found").

- [ ] **Step 3: Implementierung**

In `general_authentication.R`, Signatur-Default von `authentication_process()` (Zeile 42): in den Vektor nach `"msgraph_delegated"` den Eintrag `"msgraph_sharepoint"` einfügen. In der `auth_functions`-Liste (nach Zeile 52 `msgraph_delegated = ...`):

```r
    msgraph_sharepoint = authentication_msgraph_sharepoint,
```

Neue Funktion direkt nach `authentication_msgraph_delegated` (nach Zeile ~283):

```r
#' authentication_msgraph_sharepoint
#'
#' Decryptet die Konfiguration des delegierten SharePoint-Zugriffs (n8n-App):
#' ein JSON mit tenant_id, client_id, client_secret, store_key, store_path,
#' site_url. Siehe Spec docs/superpowers/specs/2026-08-18-msgraph-sharepoint-
#' delegated-design.md.
#' @param args FlowForce-Decryption-Key.
#' @return Named list(tenant_id, client_id, client_secret, store_key,
#'   store_path, site_url).
authentication_msgraph_sharepoint <- function(args) {
  # ---- start ---- #
  if (interactive() & (length(args) == 0 | is.na(args[1]))) {
    decrypt_key <- getPass::getPass("Bitte Decryption_Key fuer MSGraph SharePoint eingeben: ")
  } else {
    decrypt_key <- args
  }
  json <- safer::decrypt_string(
    readLines("../../keys/Microsoft365R/msgraph_sharepoint.txt"), key = decrypt_key)
  auth <- jsonlite::fromJSON(json, simplifyVector = TRUE)
  required <- c("tenant_id", "client_id", "client_secret",
                "store_key", "store_path", "site_url")
  missing <- setdiff(required, names(auth))
  if (length(missing)) {
    stop("msgraph_sharepoint.txt unvollstaendig, fehlt: ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  auth
}
```

- [ ] **Step 4: Tests laufen lassen — müssen bestehen** (gleicher Befehl wie Step 2). Zusätzlich den bestehenden Dispatch-Test grün halten: `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-authentication_dispatch.R')"` — Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add R/general_authentication.R tests/testthat/test-msgraph_sharepoint_auth.R
git commit -m "feat(msgraph-sharepoint): Service msgraph_sharepoint in authentication_process"
```

---

### Task 4: Billomatics — Headless-Bootstrap

**Files:**
- Modify: `packages/Billomatics/R/msgraph_sharepoint_auth.R` (anhängen)
- Test: `packages/Billomatics/tests/testthat/test-msgraph_sharepoint_auth.R` (anhängen)

**Interfaces:**
- Consumes: `msgraph_sp_store_write`, `.msgraph_sp_default_scopes` (Task 1).
- Produces (exportiert):
  - `msgraph_sharepoint_bootstrap_url(auth, scopes = .msgraph_sp_default_scopes, redirect_uri = "http://localhost:1410/") -> character(1)` — Login-URL zum Öffnen im lokalen Browser.
  - `msgraph_sharepoint_bootstrap(auth, auth_code, scopes = .msgraph_sp_default_scopes, redirect_uri = "http://localhost:1410/") -> invisible(store_path)` — `auth_code` akzeptiert puren Code ODER die komplette kopierte Redirect-URL.
  - intern: `msgraph_sp_extract_code(raw_in) -> character(1)`

- [ ] **Step 1: Failing Tests anhängen**

```r
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
  mockery::stub(Billomatics::msgraph_sharepoint_bootstrap, "httr::POST", ok_response)
  mockery::stub(Billomatics::msgraph_sharepoint_bootstrap, "httr::content",
                list(access_token = "at", expires_in = 3599, refresh_token = "rt-boot"))
  mockery::stub(Billomatics::msgraph_sharepoint_bootstrap, "httr::GET",
                structure(list(status_code = 200L), class = "response"))

  Billomatics::msgraph_sharepoint_bootstrap(auth, "code=ABC")
  store <- Billomatics:::msgraph_sp_store_read(store_path, "sk")
  expect_equal(store$refresh_token, "rt-boot")
  expect_equal(store$client_id, "cid")

  mockery::stub(Billomatics::msgraph_sharepoint_bootstrap, "httr::content",
                list(access_token = "at", expires_in = 3599))
  expect_error(Billomatics::msgraph_sharepoint_bootstrap(auth, "ABC"),
               "offline_access")
})
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen** (gleicher Befehl wie in Task 1 Step 2).

- [ ] **Step 3: Implementierung anhängen**

```r
#' Authorization-Code aus Roh-Eingabe extrahieren (intern)
#'
#' Akzeptiert den reinen Code, 'code=...' oder die komplette Redirect-URL.
#' @noRd
msgraph_sp_extract_code <- function(raw_in) {
  # ---- start ---- #
  code <- trimws(raw_in)
  code <- sub("^.*?code=", "", code)
  code <- sub("[&#].*$", "", code)
  code <- trimws(code)
  if (!nzchar(code)) stop("Authorization-Code ist leer.", call. = FALSE)
  code
}

#' Login-URL fuer den einmaligen SharePoint-Bootstrap bauen
#'
#' Die URL im LOKALEN Browser oeffnen und als Service-Account anmelden. Der
#' Browser landet danach auf redirect_uri?code=... (Seite laedt nicht - es
#' lauscht nichts); den Code aus der Adresszeile kopieren und an
#' msgraph_sharepoint_bootstrap() uebergeben.
#'
#' Voraussetzung App-Registrierung: redirect_uri als *Web*-Redirect,
#' delegierte Scopes inkl. offline_access mit Admin-Consent.
#'
#' @param auth Liste mit tenant_id und client_id (z. B. aus
#'   authentication_process()$msgraph_sharepoint).
#' @param scopes Character-Vektor der Scopes.
#' @param redirect_uri Redirect-URI (Typ Web an der App-Registrierung).
#' @return Login-URL als String.
#' @export
msgraph_sharepoint_bootstrap_url <- function(auth,
                                             scopes = .msgraph_sp_default_scopes,
                                             redirect_uri = "http://localhost:1410/") {
  # ---- start ---- #
  paste0(
    "https://login.microsoftonline.com/", auth$tenant_id, "/oauth2/v2.0/authorize?",
    "client_id=", auth$client_id,
    "&response_type=code&response_mode=query",
    "&redirect_uri=", utils::URLencode(redirect_uri, reserved = TRUE),
    "&scope=", utils::URLencode(paste(scopes, collapse = " "), reserved = TRUE),
    "&prompt=login")
}

#' Einmaliger Bootstrap des SharePoint-Token-Stores (auf dem Server ausfuehren)
#'
#' Tauscht den Authorization-Code gegen Access- + Refresh-Token (der Tausch
#' passiert auf der Maschine, auf der diese Funktion laeuft - Bertelsmann-
#' Vorgabe: Token entsteht AUF dem Server), schreibt den verschluesselten
#' Store und macht eine /me-Probe. Der Code ist einmalig und laeuft nach
#' ~10 Minuten ab - zuegig einfuegen.
#'
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @param auth_code Purer Code ODER komplette kopierte Redirect-URL.
#' @param scopes Character-Vektor der Scopes (muessen zu bootstrap_url passen).
#' @param redirect_uri Muss identisch zu msgraph_sharepoint_bootstrap_url() sein.
#' @return invisible(store_path)
#' @export
msgraph_sharepoint_bootstrap <- function(auth, auth_code,
                                         scopes = .msgraph_sp_default_scopes,
                                         redirect_uri = "http://localhost:1410/") {
  # ---- start ---- #
  code <- msgraph_sp_extract_code(auth_code)

  response <- httr::POST(
    paste0("https://login.microsoftonline.com/", auth$tenant_id, "/oauth2/v2.0/token"),
    encode = "form", body = list(
      grant_type    = "authorization_code",
      client_id     = auth$client_id,
      client_secret = auth$client_secret,
      code          = code,
      redirect_uri  = redirect_uri,
      scope         = paste(scopes, collapse = " ")
    ))
  parsed <- httr::content(response, as = "parsed", type = "application/json")
  if (response$status_code != 200) {
    error_code <- if (is.null(parsed$error)) "unbekannt" else parsed$error
    error_desc <- if (is.null(parsed$error_description)) "keine Beschreibung" else parsed$error_description
    stop(sprintf("Bootstrap-Token-Tausch fehlgeschlagen (HTTP %s): %s\n%s",
                 response$status_code, error_code, error_desc), call. = FALSE)
  }
  if (is.null(parsed$refresh_token) || !nzchar(parsed$refresh_token)) {
    stop("Kein Refresh-Token erhalten. Steht 'offline_access' in den Scopes ",
         "und ist es consented?", call. = FALSE)
  }

  now_utc <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  msgraph_sp_store_write(auth$store_path, auth$store_key, list(
    refresh_token = parsed$refresh_token,
    obtained_at = now_utc, last_refreshed_at = now_utc,
    tenant_id = auth$tenant_id, client_id = auth$client_id, scopes = scopes))

  probe <- httr::GET("https://graph.microsoft.com/v1.0/me",
                     httr::add_headers(Authorization = paste("Bearer", parsed$access_token)))
  message("Store geschrieben: ", auth$store_path,
          " | Probe /me HTTP ", probe$status_code)
  invisible(auth$store_path)
}
```

- [ ] **Step 4: Tests laufen lassen — müssen bestehen** (gleicher Befehl).

- [ ] **Step 5: Commit**

```bash
git add R/msgraph_sharepoint_auth.R tests/testthat/test-msgraph_sharepoint_auth.R
git commit -m "feat(msgraph-sharepoint): Headless-Bootstrap fuer den Token-Store"
```

---

### Task 5: Billomatics — Doku generieren, Gesamt-Suite, Push + PR

**Files:**
- Modify: `packages/Billomatics/NAMESPACE`, `packages/Billomatics/man/*` (generiert)

- [ ] **Step 1: roxygen2-Doku generieren**

Run: `Rscript -e "devtools::document()"`
Expected: NAMESPACE enthält neu `export(msgraph_sharepoint_token_provider)`, `export(msgraph_sharepoint_bootstrap)`, `export(msgraph_sharepoint_bootstrap_url)` — und KEINE `msgraph_sp_*`-Exporte.

- [ ] **Step 2: Gesamte Testsuite**

Run: `Rscript -e "devtools::test()"`
Expected: 0 Failures (bestehende Tests unberührt grün).

- [ ] **Step 3: Commit + Push + PR**

```bash
git add NAMESPACE man/
git commit -m "docs(msgraph-sharepoint): NAMESPACE + man-Seiten generieren"
git push -u origin feat/msgraph-sharepoint-delegated
gh pr create --repo PubPackR/Billomatics --title "feat: delegierter MSGraph-SharePoint-Zugriff (msgraph_sharepoint)" --body-file docs/superpowers/specs/2026-08-18-msgraph-sharepoint-delegated-design.md
```

(PR-Body = Spec reicht; Merge macht der User im complete-pr-Flow.)

---

### Task 6: MSGraph-Paket — Drive-Auflösung + `get_DriveItem_Info_delegated`

**Files:**
- Create: `packages/Package-01-MSGraph/R/msgraph_delegated_drive.R`
- Create: `packages/Package-01-MSGraph/R/get_DriveItem_Info_delegated.R`
- Create: `packages/Package-01-MSGraph/tests/testthat.R`, `packages/Package-01-MSGraph/tests/testthat/test-msgraph_delegated_drive.R`
- Modify: `packages/Package-01-MSGraph/DESCRIPTION`

**Interfaces:**
- Consumes: `Billomatics::msgraph_sharepoint_token_provider(auth)` (Task 2).
- Produces:
  - intern: `msgraph_delegated_token(auth) -> character(1)`; `msgraph_site_lookup_url(site_url) -> character(1)`; `msgraph_delegated_drive_id(auth) -> character(1)` (Session-Cache pro site_url); `msgraph_delegated_get(url, auth) -> geparster Body` (wirft bei HTTP != 200 mit Status + error message)
  - exportiert: `get_DriveItem_Info_delegated(absolute_path, auth, info = "id") -> Wert von content[[info]]` (wirft bei Nicht-Fund — bewusst lauter als das alte print+NULL)

- [ ] **Step 0: Branch anlegen**

```bash
cd C:/Users/HEMM036/Github/packages/Package-01-MSGraph
git checkout master
git checkout -b feat/delegated-sharepoint
```

- [ ] **Step 1: Test-Infrastruktur + Failing Test**

`tests/testthat.R`:

```r
library(testthat)
library(MSGraph)
test_check("MSGraph")
```

DESCRIPTION um folgende Zeilen ergänzen (nach `RoxygenNote`):

```
Imports:
    httr,
    utils,
    lubridate,
    Billomatics
Suggests:
    testthat (>= 3.0.0)
Config/testthat/edition: 3
```

`tests/testthat/test-msgraph_delegated_drive.R`:

```r
test_that("msgraph_site_lookup_url baut die Graph-Site-URL aus der Site-URL", {
  expect_equal(
    MSGraph:::msgraph_site_lookup_url("https://studyflix.sharepoint.com/sites/StudyflixCloud"),
    "https://graph.microsoft.com/v1.0/sites/studyflix.sharepoint.com:/sites/StudyflixCloud")
  expect_equal(
    MSGraph:::msgraph_site_lookup_url("https://studyflix.sharepoint.com/sites/StudyflixCloud/"),
    "https://graph.microsoft.com/v1.0/sites/studyflix.sharepoint.com:/sites/StudyflixCloud")
  expect_error(MSGraph:::msgraph_site_lookup_url("https://studyflix.sharepoint.com"),
               "Site-Pfad")
})
```

- [ ] **Step 2: Test laufen lassen — muss fehlschlagen**

Run (im Package-01-MSGraph-Root): `Rscript -e "devtools::load_all(); testthat::test_file('tests/testthat/test-msgraph_delegated_drive.R')"`
Expected: FAIL („msgraph_site_lookup_url not found").

- [ ] **Step 3: Implementierung**

`R/msgraph_delegated_drive.R`:

```r
################################################################################-
# ----- Description -------------------------------------------------------------
#
# Delegierter SharePoint-Zugriff (neuer Tenant): Drive-Aufloesung ueber die
# Site-URL statt der alten hardcodierten Gruppen-GUID. Token kommt aus
# Billomatics::msgraph_sharepoint_token_provider() (rotierender Refresh-Store).
# Spec: Billomatics docs/superpowers/specs/2026-08-18-msgraph-sharepoint-
# delegated-design.md
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/08
#

.msgraph_drive_cache <- new.env(parent = emptyenv())

#' Access-Token aus der Billomatics-Auth-Liste holen (intern)
#' @noRd
msgraph_delegated_token <- function(auth) {
  # ---- start ---- #
  provider <- Billomatics::msgraph_sharepoint_token_provider(auth)
  provider()
}

#' Graph-Lookup-URL fuer eine SharePoint-Site-URL bauen (intern)
#' @noRd
msgraph_site_lookup_url <- function(site_url) {
  # ---- start ---- #
  u <- httr::parse_url(site_url)
  path <- sub("/+$", "", paste(u$path, collapse = "/"))
  if (is.null(u$hostname) || !nzchar(path)) {
    stop("site_url muss Hostname UND Site-Pfad enthalten, z. B. ",
         "https://<host>.sharepoint.com/sites/<name> - erhalten: ", site_url,
         call. = FALSE)
  }
  paste0("https://graph.microsoft.com/v1.0/sites/", u$hostname, ":/", path)
}

#' GET gegen Graph mit delegiertem Token (intern) - wirft bei HTTP != 200
#' @noRd
msgraph_delegated_get <- function(url, auth) {
  # ---- start ---- #
  token <- msgraph_delegated_token(auth)
  response <- httr::GET(url, httr::add_headers(
    authorization = paste("Bearer", token)))
  parsed <- httr::content(response)
  if (response$status_code != 200) {
    msg <- if (!is.null(parsed$error$message)) parsed$error$message else "keine Details"
    stop("Graph-GET fehlgeschlagen (HTTP ", response$status_code, "): ",
         url, "\n", msg, call. = FALSE)
  }
  parsed
}

#' Drive-ID der konfigurierten Site aufloesen (intern, Session-Cache)
#' @noRd
msgraph_delegated_drive_id <- function(auth) {
  # ---- start ---- #
  cached <- .msgraph_drive_cache[[auth$site_url]]
  if (!is.null(cached)) return(cached)
  site <- msgraph_delegated_get(msgraph_site_lookup_url(auth$site_url), auth)
  drive <- msgraph_delegated_get(
    paste0("https://graph.microsoft.com/v1.0/sites/", site$id, "/drive"), auth)
  .msgraph_drive_cache[[auth$site_url]] <- drive$id
  drive$id
}
```

`R/get_DriveItem_Info_delegated.R`:

```r
#' DriveItem-Info ueber den delegierten SharePoint-Zugriff
#'
#' Delegiertes Pendant zu get_DriveItem_Info(): loest einen absoluten Pfad
#' (beginnend mit /General/...) im Drive der konfigurierten Site auf.
#' Wirft bei Nicht-Fund einen Fehler (FlowForce soll Fehlversuche flaggen).
#'
#' @param absolute_path Pfad beginnend mit /General/... (Backslashes vorher zu / aendern).
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @param info Die Info, z. B. "name", "webUrl", "parentReference" - Default "id".
#' @return Der gewaehlte Info-Wert des DriveItems.
#' @export
#' @importFrom utils URLencode
get_DriveItem_Info_delegated <- function(absolute_path, auth, info = "id") {
  # ---- start ---- #
  drive_id <- msgraph_delegated_drive_id(auth)
  clean_path <- utils::URLencode(absolute_path)
  item <- msgraph_delegated_get(
    paste0("https://graph.microsoft.com/v1.0/drives/", drive_id, "/root:", clean_path),
    auth)
  item[[info]]
}
```

- [ ] **Step 4: Test laufen lassen — muss bestehen** (gleicher Befehl wie Step 2).

- [ ] **Step 5: Commit**

```bash
git add DESCRIPTION tests/ R/msgraph_delegated_drive.R R/get_DriveItem_Info_delegated.R
git commit -m "feat(delegated): Drive-Aufloesung ueber Site-URL + get_DriveItem_Info_delegated"
```

---

### Task 7: MSGraph-Paket — Folder-Download + `get_sharepoint_data_delegated`

**Files:**
- Create: `packages/Package-01-MSGraph/R/download_sharepoint_folder_delegated.R`
- Create: `packages/Package-01-MSGraph/R/get_sharepoint_data_delegated.R`

**Interfaces:**
- Consumes: `msgraph_delegated_drive_id`, `msgraph_delegated_get`, `msgraph_delegated_token`, `get_DriveItem_Info_delegated` (Task 6); `Billomatics::read_most_recent_data` (Bestand).
- Produces (exportiert):
  - `download_sharepoint_folder_delegated(folder_path, auth, dest_dir, file_type = "xlsx") -> character()` — lädt alle Dateien des Ordners nach dest_dir, setzt mtime auf lastModifiedDateTime, gibt lokale Pfade zurück.
  - `get_sharepoint_data_delegated(folder_path, file_name, file_type, auth, tmp_folder, sheet = 1)` — Verhalten/Rückgabe identisch zu `get_sharepoint_data()`.

- [ ] **Step 1: Implementierung** (Netz-Funktionen — kein Unit-Test sinnvoll mockbar ohne großen Aufwand; Absicherung über den base-18-Smoke in Task 10. Reihenfolge hier daher: implementieren → `devtools::load_all()` als Parse-Check → Commit.)

`R/download_sharepoint_folder_delegated.R`:

```r
#' Alle Dateien eines SharePoint-Ordners delegiert herunterladen
#'
#' Laedt jede Datei des Ordners nach dest_dir und setzt das lokale
#' Aenderungsdatum auf lastModifiedDateTime der SharePoint-Datei (damit
#' "neueste Datei"-Logik wie Billomatics::read_most_recent_data funktioniert).
#'
#' @param folder_path Ordner-Pfad im Drive, beginnend mit /General/...
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @param dest_dir Lokales Zielverzeichnis (muss existieren).
#' @param file_type Datei-Endung fuer die lokalen Temp-Namen (xlsx, csv, RDS).
#' @return Character-Vektor der lokalen Dateipfade (leer, wenn Ordner leer).
#' @export
download_sharepoint_folder_delegated <- function(folder_path, auth, dest_dir,
                                                 file_type = "xlsx") {
  # ---- start ---- #
  drive_id <- msgraph_delegated_drive_id(auth)
  folder_id <- get_DriveItem_Info_delegated(folder_path, auth)

  children <- msgraph_delegated_get(
    paste0("https://graph.microsoft.com/v1.0/drives/", drive_id,
           "/items/", folder_id, "/children"), auth)

  if (length(children[["value"]]) == 0) {
    message("Ordner ist leer: ", folder_path)
    return(character(0))
  }

  token <- msgraph_delegated_token(auth)
  header <- httr::add_headers(authorization = paste("Bearer", token))

  paths <- character(0)
  for (child in children[["value"]]) {
    response <- httr::GET(
      paste0("https://graph.microsoft.com/v1.0/drives/", drive_id,
             "/items/", child[["id"]], "/content"), header)
    if (response$status_code != 200) {
      stop("Download fehlgeschlagen (HTTP ", response$status_code, "): ",
           child[["name"]], call. = FALSE)
    }
    temp_file_path <- tempfile(pattern = sub("\\..*", "_", child[["name"]]),
                               tmpdir = dest_dir,
                               fileext = paste0(".", file_type))
    writeBin(httr::content(response, as = "raw"), temp_file_path)

    ## so that we keep a date time object, this has to be explicit
    modified_DateTime <- lubridate::as_datetime(
      child[["fileSystemInfo"]][["lastModifiedDateTime"]])
    Sys.setFileTime(temp_file_path, modified_DateTime)
    paths <- c(paths, temp_file_path)
  }
  paths
}
```

`R/get_sharepoint_data_delegated.R`:

```r
#' Aktuellstes File aus einem SharePoint-Ordner delegiert laden
#'
#' Delegiertes Pendant zu get_sharepoint_data(): identisches Verhalten und
#' identische Rueckgabe, nur die Authentifizierung laeuft ueber den
#' Billomatics-Service msgraph_sharepoint (Refresh-Token-Rotation) statt
#' app-only mit hardcodetem Tenant.
#'
#' @param folder_path Ordner-Pfad im Drive, beginnend mit /General/...
#' @param file_name Kompletter Name oder Namensanfang der gesuchten Datei.
#' @param file_type Dateityp (xlsx, csv, RDS), geht an read_most_recent_data.
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @param tmp_folder Lokaler Temp-Ordner (wird bei Bedarf angelegt).
#' @param sheet Sheet-Nummer fuer xlsx.
#' @return Das gefundene File - Message, wenn nichts gefunden wurde.
#' @export
#' @importFrom Billomatics read_most_recent_data
get_sharepoint_data_delegated <- function(folder_path, file_name, file_type,
                                          auth, tmp_folder, sheet = 1) {
  # ---- start ---- #
  if (!dir.exists(tmp_folder)) {
    dir.create(tmp_folder, recursive = TRUE)
  }
  ## eigener Unterordner, damit parallele Aufrufe sich nicht in die Quere kommen
  tmp_tmp_folder <- paste0(tmp_folder, "tmp", as.integer(runif(1, 1, 10000)))
  dir.create(tmp_tmp_folder)

  download_sharepoint_folder_delegated(folder_path, auth,
                                       dest_dir = tmp_tmp_folder,
                                       file_type = file_type)

  file <- Billomatics::read_most_recent_data(tmp_tmp_folder, filetyp = file_type,
                                             name_starts_with = file_name,
                                             sheet = sheet)
  unlink(tmp_tmp_folder, recursive = TRUE)
  file
}
```

- [ ] **Step 2: Parse-Check**

Run: `Rscript -e "devtools::load_all()"`
Expected: lädt ohne Fehler.

- [ ] **Step 3: Commit**

```bash
git add R/download_sharepoint_folder_delegated.R R/get_sharepoint_data_delegated.R
git commit -m "feat(delegated): Folder-Download + get_sharepoint_data_delegated"
```

---

### Task 8: MSGraph-Paket — `upload_to_sharepoint_delegated` + Doku + PR

**Files:**
- Create: `packages/Package-01-MSGraph/R/upload_to_sharepoint_delegated.R`
- Modify: `packages/Package-01-MSGraph/NAMESPACE`, `man/*` (generiert)

**Interfaces:**
- Consumes: `msgraph_delegated_drive_id`, `msgraph_delegated_token`, `get_DriveItem_Info_delegated` (Task 6).
- Produces (exportiert): `upload_to_sharepoint_delegated(local_files, sharepoint_folder, auth) -> invisible(TRUE)` — wirft bei jedem Status außerhalb 200/201.

- [ ] **Step 1: Implementierung**

`R/upload_to_sharepoint_delegated.R`:

```r
#' Dateien delegiert in einen SharePoint-Ordner hochladen
#'
#' Delegiertes Pendant zum Upload-Teil von move_tmpJP5export_to_sharepoint()
#' (base-18): laedt jede Datei per PUT in den Zielordner. Wirft beim ersten
#' fehlgeschlagenen Upload (FlowForce soll den Lauf flaggen).
#'
#' @param local_files Character-Vektor lokaler Dateipfade.
#' @param sharepoint_folder Ziel-Ordner im Drive, beginnend mit /General/...
#' @param auth Liste aus authentication_process()$msgraph_sharepoint.
#' @return invisible(TRUE)
#' @export
upload_to_sharepoint_delegated <- function(local_files, sharepoint_folder, auth) {
  # ---- start ---- #
  drive_id <- msgraph_delegated_drive_id(auth)
  folder_id <- get_DriveItem_Info_delegated(sharepoint_folder, auth)
  token <- msgraph_delegated_token(auth)

  for (file in local_files) {
    filename <- basename(file)
    file_content <- readBin(file, what = "raw", n = file.size(file))
    response <- httr::PUT(
      paste0("https://graph.microsoft.com/v1.0/drives/", drive_id,
             "/items/", folder_id, ":/", utils::URLencode(filename), ":/content"),
      httr::add_headers(authorization = paste("Bearer", token),
                        "content-type" = "octet/stream"),
      body = file_content)
    if (!(response$status_code %in% c(200, 201))) {
      stop("Upload fehlgeschlagen (HTTP ", response$status_code, "): ",
           filename, call. = FALSE)
    }
  }
  message(length(local_files), " Datei(en) nach ", sharepoint_folder, " hochgeladen")
  invisible(TRUE)
}
```

- [ ] **Step 2: Doku generieren + Tests**

Run: `Rscript -e "devtools::document(); devtools::test()"`
Expected: NAMESPACE enthält die 4 neuen Exporte (`get_DriveItem_Info_delegated`, `download_sharepoint_folder_delegated`, `get_sharepoint_data_delegated`, `upload_to_sharepoint_delegated`), Tests PASS. Alte Exporte unverändert vorhanden.

- [ ] **Step 3: Commit + Push + PR**

```bash
git add R/upload_to_sharepoint_delegated.R NAMESPACE man/ DESCRIPTION
git commit -m "feat(delegated): upload_to_sharepoint_delegated + Doku"
git push -u origin feat/delegated-sharepoint
gh pr create --title "feat: delegierte SharePoint-Funktionen (neuer Tenant, Billomatics msgraph_sharepoint)" --body "Delegierte Pendants zu get_sharepoint_data/get_DriveItem_Info/Upload; Drive-Aufloesung ueber site_url statt Gruppen-GUID. Alte app-only-Funktionen bleiben unveraendert (Legacy alter Tenant). Spec: Billomatics docs/superpowers/specs/2026-08-18-msgraph-sharepoint-delegated-design.md"
```

---

### Task 9: base-18 — Migration der Lese-Call-Sites

**Files:**
- Modify: `base-apps/base-18_export_billomat2sap/func/get_data_4_jp5export.R` (Zeilen 23, 72, 87, 119, 132, 144 + neue Funktion neben `get_sharepoint_folder()` Z. 347)
- Modify: `base-apps/base-18_export_billomat2sap/do/main_post_new_debitor.R` (Z. 7, 25)
- Modify: `base-apps/base-18_export_billomat2sap/do/main_Monatsabschluss_erstellen.R` (Z. 36-42, 162, 181)
- Modify: alle weiteren `do/`-Skripte mit `"msgraph"` im `authentication_process()`-Call (Liste in Step 2)

**Interfaces:**
- Consumes: `MSGraph::get_sharepoint_data_delegated`, `MSGraph::download_sharepoint_folder_delegated` (Task 7), Service `msgraph_sharepoint` (Task 3).
- Produces: `get_sharepoint_folder_delegated(folder_path, auth, tmp_folder, file_type = "xlsx")` (app-lokal in `func/get_data_4_jp5export.R`).

- [ ] **Step 0: Repo aktualisieren + Branch**

```bash
cd C:/Users/HEMM036/Github/base-apps/base-18_export_billomat2sap
git checkout main
git pull
git checkout -b feat/msgraph-sharepoint-delegated
```

- [ ] **Step 1: Lokale Pakete installieren** (damit die Skripte lauffähig prüfbar sind)

Run: `Rscript -e "devtools::install('../../packages/Billomatics', upgrade = 'never'); devtools::install('../../packages/Package-01-MSGraph', upgrade = 'never')"`
Expected: beide installieren fehlerfrei.

- [ ] **Step 2: Vollständige Call-Site-Inventur** (Pflicht-Gate vor den Edits)

Run im Repo-Root:

```bash
grep -rn "get_sharepoint_data\|authorize_graph\|get_DriveItem_Info\|groups/{" --include="*.R" do/ func/ analyse/
```

Erwartete Treffer (Stand Planerstellung — bei Abweichung: neue Treffer nach demselben Muster wie unten mitmigrieren):
`func/get_data_4_jp5export.R` (23, 72, 87, 119, 132, 144, 353, 365, 366, 373), `func/move_tmp_to_sharepoint.R` (16, 20, 32), `do/main_post_new_debitor.R` (25), `do/main_Monatsabschluss_erstellen.R` (162, 181), `analyse/abweichung_SAP_Billomat.R` (analyse/ = Wegwerf-Analytik → NICHT migrieren, nur im PR-Text erwähnen).

- [ ] **Step 3: `authentication_process()`-Calls umstellen**

In JEDEM dieser Skripte im Services-Vektor `"msgraph"` durch `"msgraph_sharepoint"` ersetzen (1:1-Ersetzung an gleicher Position — die Key-Datei wird mit demselben Decryption-Key verschlüsselt wie die alte msgraph-Datei, dadurch bleiben die FlowForce-Args positionsgleich gültig, siehe Task 11):

- `do/find_double_booking_jp5.R:33`, `do/main_clear_confirmation_billomat.R:38`, `do/main_clear_payment_document_billomat.R:35`, `do/main_complete_document_billomat.R:36`, `do/main_exportBillomat_2_jp5.R:37`, `do/main_flowForce_create_invoiceBillomat.R:48`, `do/main_Monatsabschluss_erstellen.R:37`, `do/main_post_new_debitor.R:7`, `do/monatlicheAbgrenzung_unfertige_Leistung.R:31`

Beispiel (`do/main_post_new_debitor.R:7`):

```r
# vorher
keys <- authentication_process(c("billomat","msgraph"), args = commandArgs(trailingOnly = TRUE))
# nachher
keys <- authentication_process(c("billomat","msgraph_sharepoint"), args = commandArgs(trailingOnly = TRUE))
```

- [ ] **Step 4: Lese-Call-Sites tauschen**

Muster für alle 8 `get_sharepoint_data(...)`-Aufrufe (6× `func/get_data_4_jp5export.R`, 2× `do/main_Monatsabschluss_erstellen.R`, 1× `do/main_post_new_debitor.R`):

```r
# vorher
get_sharepoint_data(folder_path = ..., file_name = ..., file_type = ...,
                    tmp_folder = ..., msgraph_key = keys$msgraph[1])
# nachher
MSGraph::get_sharepoint_data_delegated(folder_path = ..., file_name = ...,
                    file_type = ..., tmp_folder = ...,
                    auth = keys$msgraph_sharepoint)
```

(`sheet`-Argumente unverändert übernehmen, wo vorhanden.)

- [ ] **Step 5: `get_sharepoint_folder_delegated()` ergänzen**

In `func/get_data_4_jp5export.R` DIREKT NACH der bestehenden `get_sharepoint_folder()` (alte Funktion bleibt!) einfügen, danach per Grep `get_sharepoint_folder(` alle Aufrufer im Repo finden und auf die neue Funktion mit `auth = keys$msgraph_sharepoint` umstellen:

```r
#' Alle Files eines SharePoint-Ordners laden (delegiert, neuer Tenant)
#'
#' @param folder_path Ordner-Pfad im Drive, beginnend mit /General/...
#' @param auth keys$msgraph_sharepoint aus authentication_process().
#' @param tmp_folder Lokaler Temp-Ordner.
#' @param file_type Dateityp (xlsx, csv, RDS).
#' @return Liste eingelesener Dateien wie get_sharepoint_folder().
get_sharepoint_folder_delegated <- function(folder_path, auth, tmp_folder,
                                            file_type = "xlsx") {
  # ---- start ---- #
  if (!dir.exists(tmp_folder)) {
    dir.create(tmp_folder, recursive = TRUE)
  }
  tmp_tmp_folder <- paste0(tmp_folder, "tmp", as.integer(runif(1, 1, 10000)))
  dir.create(tmp_tmp_folder)

  rechnungsfiles <- MSGraph::download_sharepoint_folder_delegated(
    folder_path, auth, dest_dir = tmp_tmp_folder, file_type = file_type)

  if (length(rechnungsfiles) == 0) {
    print("No files found after download")
    return(NULL)
  }

  named_rechnungsfiles <- set_names(rechnungsfiles, nm = basename(rechnungsfiles))
  rechnungen_list <- purrr::map(named_rechnungsfiles, function(x) {
    tryCatch({
      df <- readxl::read_excel(x)
      df
    }, error = function(e) {
      message("Fehler beim Einlesen von ", x, ": ", conditionMessage(e))
      NULL
    })
  })
  unlink(tmp_tmp_folder, recursive = TRUE)
  rechnungen_list
}
```

(Den Einlese-Teil ab `rechnungen_list <- purrr::map(...)` beim Implementieren 1:1 aus der alten `get_sharepoint_folder()` ab Zeile 397 übernehmen — der obige Block zeigt den Anfang; die alte Funktion ist die Quelle der Wahrheit für das Einlese-/Aggregations-Verhalten.)

- [ ] **Step 6: Hardcodete Konstanten entfernen, wo sie funktionslos wurden**

In `do/main_Monatsabschluss_erstellen.R:40-42` (`tenant_id`/`client_id`/`studyflix_cloud_id`): per Grep im File prüfen, ob nach Step 4 noch ein Nutzer existiert — wenn nein, die 3 Zeilen löschen. Gleiches gilt für die Konstanten-Zeilen in umgebauten Funktionen. In NICHT umgebauten Dateien (`analyse/`) nichts anfassen.

- [ ] **Step 7: Parse-Check aller geänderten Dateien**

Run: `Rscript -e "invisible(lapply(c('func/get_data_4_jp5export.R'), function(f) parse(f)))"` — für jede geänderte Datei. Expected: keine Parse-Fehler.

- [ ] **Step 8: Commit**

```bash
git add func/get_data_4_jp5export.R do/
git commit -m "feat: SharePoint-Lesezugriffe auf delegierte Auth (msgraph_sharepoint) umstellen"
```

---

### Task 10: base-18 — Upload-Wrapper + Smoke-Skript + PR

**Files:**
- Modify: `base-apps/base-18_export_billomat2sap/func/move_tmp_to_sharepoint.R` (neue Funktion anhängen, alte bleibt)
- Modify: `base-apps/base-18_export_billomat2sap/do/main_exportBillomat_2_jp5.R:75-79`
- Create: `base-apps/base-18_export_billomat2sap/one-off/smoke_sharepoint_delegated.R`

**Interfaces:**
- Consumes: `MSGraph::upload_to_sharepoint_delegated` (Task 8), `MSGraph::get_sharepoint_data_delegated` (Task 7).
- Produces: `move_tmpJP5export_to_sharepoint_delegated(PATH_ON_SHAREPOINT, PATH_TO_FOLDER, auth)` (app-lokal).

- [ ] **Step 1: Delegierten Upload-Wrapper anhängen**

In `func/move_tmp_to_sharepoint.R` NACH der alten Funktion:

```r
#' move_tmpJP5export_to_sharepoint_delegated
#'
#' Delegiertes Pendant (neuer Tenant): Upload via
#' MSGraph::upload_to_sharepoint_delegated, danach tmp-Ordner leeren.
#'
#' @param PATH_ON_SHAREPOINT Ziel-Ordner im Drive.
#' @param PATH_TO_FOLDER Lokaler Quell-Ordner.
#' @param auth keys$msgraph_sharepoint aus authentication_process().
#' @return invisible(TRUE)
move_tmpJP5export_to_sharepoint_delegated <- function(PATH_ON_SHAREPOINT,
                                                      PATH_TO_FOLDER,
                                                      auth) {
  # ---- start ---- #
  all_files <- list.files(PATH_TO_FOLDER, full.names = TRUE)
  print(paste("Uploading", length(all_files), "files to Sharepoint..."))

  MSGraph::upload_to_sharepoint_delegated(all_files, PATH_ON_SHAREPOINT, auth)

  ## ----- CleanUp -----
  unlink(PATH_TO_FOLDER, recursive = TRUE, force = TRUE)
  dir.create(PATH_TO_FOLDER)
  print("Folder cleaned successfully")
  invisible(TRUE)
}
```

Hinweis: die alte Funktion hängte `FolderNameSharepoint = "/tmp"` an den Pfad an — beim Umstellen des Aufrufers prüfen, ob der Zielordner `paste0(sharepoint_folder_jp5import, "/tmp")` sein muss (Alt-Verhalten mit Default beibehalten!).

- [ ] **Step 2: Aufrufer umstellen**

`do/main_exportBillomat_2_jp5.R:75-79`:

```r
# vorher
move_tmpJP5export_to_sharepoint(
  msgraph_key = keys$msgraph[1],
  PATH_ON_SHAREPOINT = sharepoint_folder_jp5import,
  PATH_TO_FOLDER = server_folder
)
# nachher (Default FolderNameSharepoint="/tmp" der Alt-Funktion explizit uebernommen)
move_tmpJP5export_to_sharepoint_delegated(
  PATH_ON_SHAREPOINT = paste0(sharepoint_folder_jp5import, "/tmp"),
  PATH_TO_FOLDER = server_folder,
  auth = keys$msgraph_sharepoint
)
```

- [ ] **Step 3: Smoke-Skript anlegen**

`one-off/smoke_sharepoint_delegated.R`:

```r
################################################################################-
# ----- Description -------------------------------------------------------------
#
# EINMALIGER Smoke-Test des delegierten SharePoint-Zugriffs (msgraph_sharepoint)
# nach Server-Setup (Runbook in Billomatics docs/). Liest ein bekanntes File und
# laedt eine Wegwerf-Datei hoch. VOR dem ersten FlowForce-Lauf ausfuehren:
#   Rscript one-off/smoke_sharepoint_delegated.R '<decrypt_key_msgraph_sharepoint>'
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/08
#

################################################################################-
# ----- Settings ----------------------------------------------------------------

## ----- libraries -----
library(Billomatics); library(MSGraph)

## ----- constants -----
sharepoint_folder_jp5export <- "/General/Kunden/03 Sales Success Management/PMI/Billomat_2_jp5/JP5 Export"
smoke_upload_folder <- "/General/Kunden/03 Sales Success Management/PMI/Billomat_2_jp5/JP5 Import/tmp"

## ----- data -----
keys <- authentication_process(c("msgraph_sharepoint"), args = commandArgs(trailingOnly = TRUE))

################################################################################-
# ----- Start -------------------------------------------------------------------

## 1) Lesen: bekanntes Export-File
sap_posten <- MSGraph::get_sharepoint_data_delegated(
  folder_path = sharepoint_folder_jp5export,
  file_name = "EXPORT_", file_type = "xlsx",
  auth = keys$msgraph_sharepoint, tmp_folder = "../../base-data/tmp/")
stopifnot(is.data.frame(sap_posten), nrow(sap_posten) > 0)
cat("READ ok:", nrow(sap_posten), "Zeilen\n")

## 2) Schreiben: Wegwerf-Datei hochladen (manuell wieder loeschen)
smoke_file <- file.path(tempdir(), "smoke_test_delete_me.txt")
writeLines(paste("smoke", format(Sys.time(), "%Y-%m-%d %H:%M:%S")), smoke_file)
MSGraph::upload_to_sharepoint_delegated(smoke_file, smoke_upload_folder,
                                        auth = keys$msgraph_sharepoint)
cat("UPLOAD ok - smoke_test_delete_me.txt im SharePoint manuell loeschen.\n")
```

**Achtung:** Die beiden Ordner-Pfade oben stammen aus den heutigen base-18-Konstanten (`do/main_exportBillomat_2_jp5.R:30-31` u. a.) — beim Server-Setup (Task 11) gegen die tatsächliche Struktur im NEUEN SharePoint verifizieren und ggf. hier UND in den `do/`-Konstanten anpassen.

- [ ] **Step 4: Parse-Check + Commit + Push + PR**

```bash
Rscript -e "invisible(parse('func/move_tmp_to_sharepoint.R')); invisible(parse('do/main_exportBillomat_2_jp5.R')); invisible(parse('one-off/smoke_sharepoint_delegated.R'))"
git add func/move_tmp_to_sharepoint.R do/main_exportBillomat_2_jp5.R one-off/smoke_sharepoint_delegated.R
git commit -m "feat: SharePoint-Upload delegiert + Smoke-Skript"
git push -u origin feat/msgraph-sharepoint-delegated
gh pr create --title "feat: SharePoint-Zugriff auf delegierte Auth umstellen (neuer Tenant)" --body "Alle get_sharepoint_data/Upload-Call-Sites auf MSGraph *_delegated + Billomatics-Service msgraph_sharepoint. Voraussetzung: Billomatics-PR + Package-01-MSGraph-PR gemergt + Server-Setup laut Runbook. analyse/abweichung_SAP_Billomat.R bewusst nicht migriert (Wegwerf-Analytik)."
```

---

### Task 11: Runbook Server-Setup & Rollout (Doku, human-only Schritte)

**Files:**
- Create: `packages/Billomatics/docs/runbook_msgraph_sharepoint_server_setup.md`

- [ ] **Step 1: Runbook schreiben** — Inhalt (vollständig ausformulieren, hier die Pflicht-Abschnitte mit den Kern-Kommandos):

1. **Voraussetzungen beschaffen (Moritz/IT):** Tenant-ID, Client-ID, Client-Secret + Secret-Ablaufdatum der n8n-App (aus n8n-Credential bzw. Entra); Site-URL aus dem n8n-Workflow; prüfen, dass `http://localhost:1410/` als *Web*-Redirect an der App steht (sonst bei IT ergänzen). Asana-Reminder-Task für den Secret-Ablauf anlegen (Projekt „Interne Prozesse" 1211291490559148).
2. **Key-Datei erzeugen** (auf dem Server, interaktive R-Session, Working-Dir = beliebige App unter base-apps/):

```r
auth <- list(
  tenant_id = "<TENANT>", client_id = "<CLIENT>", client_secret = "<SECRET>",
  store_key = "<NEUER-ZUFALLS-KEY>",
  store_path = "../../keys/Microsoft365R/msgraph_sharepoint_refresh.txt",
  site_url = "https://<host>.sharepoint.com/sites/<name>")
cipher <- safer::encrypt_string(as.character(jsonlite::toJSON(auth, auto_unbox = TRUE)),
                                key = "<DECRYPT-KEY — DERSELBE wie fuer keys/Microsoft365R/microsoft365r.txt>")
writeLines(gsub("[\r\n]", "", cipher), "../../keys/Microsoft365R/msgraph_sharepoint.txt")
```

**Entscheidung festhalten:** Der Decrypt-Key ist bewusst derselbe wie beim alten `msgraph`-Service — dadurch bleiben die FlowForce-Args nach dem 1:1-Service-Swap positionsgleich gültig und **kein FlowForce-Job muss angefasst werden**. (Falls stattdessen ein neuer Key gewünscht: jeden betroffenen FlowForce-Job um den neuen Arg ergänzen.)
3. **Bootstrap** (interaktive R-Session auf dem Server, gleiches Working-Dir):

```r
keys <- Billomatics::authentication_process(c("msgraph_sharepoint"), args = NA)
cat(Billomatics::msgraph_sharepoint_bootstrap_url(keys$msgraph_sharepoint), "\n")
# -> URL im LOKALEN Browser oeffnen, als n8n-SERVICE-ACCOUNT anmelden,
#    Redirect-URL aus der Adresszeile kopieren (Code ~10 min gueltig), dann:
Billomatics::msgraph_sharepoint_bootstrap(keys$msgraph_sharepoint, "<CODE-ODER-URL>")
# Expected: "Store geschrieben: ... | Probe /me HTTP 200"
```

4. **Rollout-Reihenfolge:** Billomatics-PR mergen → Package-01-MSGraph-PR mergen → Server-Package-Reinstall (Infra-Workflow `04-setup-r-packages-flow-force.yml`) → Smoke `Rscript one-off/smoke_sharepoint_delegated.R '<key>'` in base-18 (vorher Ordner-Pfade gegen neues SharePoint verifiziert) → base-18-PR mergen → Deploy (`gh workflow run "Deploy app"` im base-18-Repo) → ersten FlowForce-Lauf jedes base-18-Jobs beobachten.
5. **Folge-Migrationen:** Restliche Repos (Spec §6) nach dem base-18-Muster aus Task 9/10; `smoke_test_delete_me.txt` im SharePoint löschen.

- [ ] **Step 2: Commit (im Billomatics-Repo, gleicher Branch)**

```bash
cd C:/Users/HEMM036/Github/packages/Billomatics
git add docs/runbook_msgraph_sharepoint_server_setup.md
git commit -m "docs(msgraph-sharepoint): Runbook Server-Setup und Rollout"
git push
```

---

## Plan-Selbstreview (erledigt bei Erstellung)

- **Spec-Abdeckung:** §3.1 → Tasks 1-5; §3.2 → Tasks 6-8; §4 → Tasks 1-2; §5 → Task 11; §6 (base-18) → Tasks 9-10, Rest-Repos bewusst out-of-scope (im Plan-Kopf deklariert); §7 → Tests in Tasks 1-4 + Smoke Task 10; §8 → Task 11 (Reminder, FlowForce-Key-Entscheidung).
- **Offene bewusste Lücke:** Mock-Tests für die Netz-Funktionen des MSGraph-Pakets (Task 7/8) — Absicherung erfolgt über den Smoke in Task 10; reine Logik (`msgraph_site_lookup_url`, Code-Extraktion, Store, Provider) ist unit-getestet.
- **Konsistenz:** Signaturen in „Interfaces"-Blöcken quergeprüft (auth-Liste, Provider-Signatur, `*_delegated`-Namen identisch über Tasks 6-10).
