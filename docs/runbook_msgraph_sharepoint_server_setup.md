# Runbook: MS Graph + SharePoint Scoped Delegated Auth — Server Setup & Rollout

This runbook guides the complete migration from device-code auth to scoped delegated auth for MS Graph (in-server OAuth) and SharePoint access on the production server `shiny.studyflix.info`.

---

## Prerequisites

Before beginning, collect and prepare the following from Entra/n8n:

- **Tenant ID** (Entra directory)
- **Client ID** (n8n service app registration)
- **Client Secret** (n8n service app registration) + **expiration date**
- **SharePoint Site URL** (from n8n workflow or n8n Credential config)
- **Verify Web Redirect URI:** Confirm `http://localhost:1410/` is registered as a Web redirect at the n8n app registration in Entra. If missing, request IT to add it.
- **Create Asana reminder task** in project "Interne Prozesse" (1211291490559148) for the Client Secret expiration date (set reminder for 30 days before).

---

## Step 1: Generate the Encrypted Key File

Execute this on the production server in an **interactive R session**. First, connect to the server:

```bash
ssh application-user@shiny.studyflix.info
cd /srv/shiny-server/base-18  # or another base-apps/ subfolder
R
```

The working directory must be a `base-apps/` subfolder (e.g., `base-18`, `base-41`, etc.). Then run:

```r
# ===== Load Billomatics package and set up auth =====
library(Billomatics)

# Create the auth list with credentials from prerequisites
auth <- list(
  tenant_id = "<TENANT_ID>",
  client_id = "<CLIENT_ID>",
  client_secret = "<CLIENT_SECRET>",
  store_key = "<NEW_RANDOM_KEY>",  # Generate a random string (e.g., random hex or passphrase)
  store_path = "../../keys/Microsoft365R/msgraph_sharepoint_refresh.txt",
  site_url = "https://<SHAREPOINT_HOST>.sharepoint.com/sites/<SITE_NAME>"
)

# Encrypt and write to the shared key file
# IMPORTANT: The decrypt_key MUST be the SAME as the one in keys/Microsoft365R/microsoft365r.txt
# This ensures FlowForce job arguments remain unchanged; no FlowForce jobs need modification.
cipher <- safer::encrypt_string(
  as.character(jsonlite::toJSON(auth, auto_unbox = TRUE)),
  key = "<DECRYPT_KEY_FROM_microsoft365r.txt>"
)
writeLines(gsub("[\r\n]", "", cipher), "../../keys/Microsoft365R/msgraph_sharepoint.txt")
cat("Key file written successfully to ../../keys/Microsoft365R/msgraph_sharepoint.txt\n")
```

**Key Decision (Critical):**  
The decrypt key is intentionally identical to the existing `microsoft365r.txt` key. This preserves FlowForce argument positions across all jobs that use the `msgraph` service; **no FlowForce job modifications are required**. If you prefer a new decrypt key for security isolation, you must:
1. Use a distinct key above (not from `microsoft365r.txt`).
2. Update every FlowForce job that consumes this service to add the new key as an additional argument.
3. Notify the FlowForce operator of the argument position change.

---

## Step 2: Bootstrap (Interactive Auth)

In the **same R session**, execute the bootstrap workflow:

```r
# ===== Load the newly encrypted credentials =====
keys <- Billomatics::authentication_process(c("msgraph_sharepoint"), args = NA)

# ===== Generate the bootstrap consent URL =====
bootstrap_url <- Billomatics::msgraph_sharepoint_bootstrap_url(keys$msgraph_sharepoint)
cat("Open this URL in your LOCAL browser (as n8n SERVICE ACCOUNT):\n")
cat(bootstrap_url, "\n")

# ===== After opening the URL and granting consent =====
# Copy the full redirect URL from the browser address bar (contains authorization code).
# The code is valid for ~10 minutes.
# Then call:
Billomatics::msgraph_sharepoint_bootstrap(keys$msgraph_sharepoint, "<REDIRECT_URL_OR_CODE>")

# Expected output: "Store geschrieben: ... | Probe /me HTTP 200"
# If successful, the refresh token is persisted in the encrypted store.
```

**Troubleshooting:**
- If redirect URL is not recognized, verify `http://localhost:1410/` is registered at the app (Step 1 prerequisites).
- If HTTP 401 or scope errors occur, confirm the n8n app has delegated permissions for `Files.ReadWrite.All`, `User.Read`, `offline_access` in Entra with Admin-Consent. **Critical:** `offline_access` is required to receive a refresh token; without it, Bootstrap errors with "Kein Refresh-Token erhalten".

---

## Step 3: Rollout Order (Coordinated PRs & Deployment)

Execute the following sequence. Do not skip or reorder steps.

### 3.1 Merge Code Changes

1. **Billomatics package (PR #32):** Merge to `main`
   ```bash
   cd C:/Users/HEMM036/Github/packages/Billomatics
   gh pr merge 32 --merge
   ```

2. **Package-01-MSGraph (PR #1):** Merge to `main`
   ```bash
   cd C:/Users/HEMM036/Github/packages/Package-01-MSGraph
   gh pr merge 1 --merge
   ```

### 3.2 Server Package Reinstall

Trigger the infrastructure workflow to reinstall R packages:

```bash
cd C:/Users/HEMM036/Github/shiny-apps/shiny-99-modules  # or any base-app/shiny-app
gh workflow run 04-setup-r-packages-flow-force.yml -R Studyflix/Shiny-0-studyflix-infrastructure
```

(Inputs ggf. in der GitHub-UI prüfen — Workflow liegt im Infra-Repo.)

Wait for the workflow to complete (~10–15 minutes). Verify success in GitHub Actions.

### 3.3 Smoke Test (base-18)

Before running the smoke test, verify that n8n has updated the SharePoint folder paths in the new delegated site. Check the n8n workflow output to confirm the folder structure matches your expectations. This prevents smoke test false passes on stale paths.

Run the smoke test to verify SharePoint delegated auth connectivity:

```bash
# SSH to production server (if not already connected from Steps 1–2)
ssh application-user@shiny.studyflix.info

# Navigate to base-18 directory
cd /srv/shiny-server/base-18

# Run smoke test with the decrypt key
Rscript one-off/smoke_sharepoint_delegated.R '<DECRYPT_KEY_FROM_microsoft365r.txt>'

# Expected output: HTTP 200 confirmations for /me, /drive, and folder listing tests.
# Verify all printed paths match the new SharePoint site structure verified above.
```

### 3.4 Fallback Management (Critical for base-18)

**Current state:** base-18 has a local SharePoint fallback enabled (PR #46, `USE_LOCAL_SHAREPOINT_FALLBACK <- TRUE` in `config.R`). This allows production to continue reading from local files while delegated auth is verified.

**Do NOT disable the fallback yet.** The flag must remain `TRUE` until:
1. **Smoke test passes** (Step 3.3 above: HTTP 200 on all probes).
2. **Deferred script migration is complete** (see Step 4 below).

If you disable the flag prematurely, non-migrated deferred scripts will crash when they attempt to read from SharePoint.

### 3.5 Merge base-18 Code Changes

After smoke test success:

```bash
cd C:/Users/HEMM036/Github/base-apps/base-18
gh pr merge 47 --merge
```

### 3.6 Deploy base-18

Trigger the deployment workflow:

```bash
cd C:/Users/HEMM036/Github/base-apps/base-18
gh workflow run "Deploy app"
```

Monitor the deployment in GitHub Actions. Once complete, verify the Shiny app loads at `https://shiny.studyflix.info/base-18/`.

### 3.7 FlowForce Job Monitoring

Observe the first run of each base-18 FlowForce job:

- `main_create_database_trigger.R`
- `main_complete_document_billomat.R`
- All other configured jobs

Use the FlowForce UI or logs to confirm successful execution (no auth errors or SharePoint access failures).

---

## Step 4: Deferred Migration Plan (base-14 Wave)

The following scripts use the old `msgraph` auth but read from SharePoint. They must be migrated to use `msgraph_sharepoint` before the fallback flag in base-18 is disabled. **This is a separate ticket; coordinate with the sprint planning.**

### Scripts Requiring Migration

**base-14:**
- `do/main_clear_confirmation_billomat.R` — calls `load_sharepoint_data()`
- `do/main_Monatsabschluss_erstellen.R` — calls `load_sharepoint_data()` and `copy_folder_from_server_to_sharepoint()` (only SharePoint write in the codebase)

**Dependent scripts (consuming output from above):**
- `func/get_data_4_jp5export_sqlite.R` — called by:
  - `do/find_double_booking_jp5.R`
  - `do/main_flowForce_create_invoiceBillomat.R`

**Migration approach:**
- Update `load_sharepoint_data()` call signature to use `msgraph_sharepoint` service.
- Update `copy_folder_from_server_to_sharepoint()` to use delegated auth (SharePoint write requires additional scope confirmation).
- Keep `authentication_process()` vector as `c("msgraph_sharepoint")` (or append if other services are used).
- Test locally with the smoke test pattern before deployment.
- Deploy base-14 changes and run FlowForce jobs to confirm.

**Timeline:** This wave is independent of the current rollout. Plan it after base-18 stabilization (2–3 business days post-deployment).

---

## Step 5: Fallback Rollback (Final Cleanup)

**Only after** all of the following conditions are met:
1. **Smoke test passes** (Step 3.3: HTTP 200 on all probes).
2. **Deferred scripts migrated and deployed** (Step 4 complete).
3. **Coordinated with Max Berning** (author of PR #46, the fallback implementation). Notify him via Teams before proceeding.

Then disable the fallback via PR:

```bash
cd C:/Users/HEMM036/Github/base-apps/base-18
git checkout main && git pull
git checkout -b chore/disable-sharepoint-fallback
# Edit config.R: USE_LOCAL_SHAREPOINT_FALLBACK <- FALSE
git add config.R
git commit -m "config: SharePoint-Fallback deaktivieren nach delegierter Migration"
git push -u origin chore/disable-sharepoint-fallback
gh pr create --title "config: SharePoint-Fallback deaktivieren" --body "Voraussetzungen erfuellt: Smoke gruen + deferred Skripte migriert. Abgestimmt mit Max Berning."
# After review and merge:
gh workflow run "Deploy app"
```

**Verify:** Re-run the smoke test to confirm the app reads exclusively from SharePoint (not local fallback).

---

## Step 6: Remaining Repository Migrations

After base-18 and base-14 stabilize, migrate remaining repositories per the pattern established in Tasks 9–10 of the original specification:

- Identify all scripts using `load_sharepoint_data()` or SharePoint operations
- Update auth to `msgraph_sharepoint`
- Smoke test locally
- Merge and deploy
- Monitor FlowForce runs

Upon completion, delete the test marker file in SharePoint:
```bash
# After all repos deployed, remove:
# /sites/<name>/Shared Documents/smoke_test_delete_me.txt
```

---

## Troubleshooting

### HTTP 401 on Smoke Test
- Verify the Client Secret has not expired.
- Confirm the n8n app delegated scopes in Entra include `Files.ReadWrite.All`, `User.Read`, `offline_access` with Admin-Consent.
- Rerun the bootstrap (Step 2) to refresh the stored refresh token.

### FlowForce Job Fails with Auth Error
- Check the FlowForce job argument list—confirm the decrypt key is present and correct.
- Rerun the smoke test to verify server-to-SharePoint connectivity.
- Review the Billomatics error logs on the server for decryption or token issues.

### Deferred Scripts Crash After Disabling Fallback
- A script was not migrated before fallback disable. Identify the script from the error log.
- Migrate it (Step 4 pattern) and redeploy.
- Re-enable the fallback temporarily if needed (revert `USE_LOCAL_SHAREPOINT_FALLBACK <- FALSE` back to `TRUE`, deploy, notify Moritz).

### Known Pre-Existing Bug
- **Script:** `main_complete_document_billomat.R`
- **Issue:** Does not source `func/sharepoint_local_fallback.R`, causing crash on next execution.
- **Fix:** Requires a separate ticket; include sourcing of fallback helper when available.
- **Workaround:** Manual intervention until fixed.

---

## Validation Checklist

- [ ] Prerequisites collected (Tenant ID, Client ID, Secret, Site URL)
- [ ] Asana reminder task created for Secret expiration
- [ ] Key file generated and written to `../../keys/Microsoft365R/msgraph_sharepoint.txt`
- [ ] Bootstrap completed successfully (HTTP 200 from /me probe)
- [ ] Billomatics PR #32 merged
- [ ] Package-01-MSGraph PR #1 merged
- [ ] Package reinstall workflow completed
- [ ] Smoke test passes on base-18 (HTTP 200 on /me, /drive, folder listing)
- [ ] base-18 PR #47 merged
- [ ] base-18 deployment completed
- [ ] base-18 FlowForce jobs execute without auth errors
- [ ] Deferred scripts (base-14) migration planned and scheduled
- [ ] Max Berning notified of fallback rollback timeline
- [ ] Fallback flag disabled (only after Step 4 complete)
- [ ] Remaining repos migrated and deployed
- [ ] `smoke_test_delete_me.txt` deleted from SharePoint

---

## Security Notes

- The encrypt/decrypt key is stored in `../../keys/Microsoft365R/microsoft365r.txt` and must be protected (server-only access, not checked into git).
- The Client Secret in Entra will expire; the Asana reminder ensures timely renewal.
- All refresh tokens are encrypted at rest in `msgraph_sharepoint_refresh.txt`.
- FlowForce job logs are readable by the application user; they should not print plaintext secrets (the Billomatics auth layer handles this).

---

**Runbook version:** 1.0  
**Date:** 2026-08-19  
**Authors:** Implementation team  
