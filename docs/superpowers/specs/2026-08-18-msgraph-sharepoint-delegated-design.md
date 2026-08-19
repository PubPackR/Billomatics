# Spec: Delegierter MS-Graph-SharePoint-Zugriff (`msgraph_sharepoint`)

**Datum:** 2026-08-18 · **Status:** freigegeben (Design-Review mit Moritz)
**Betroffene Repos:** `Billomatics`, `Package-01-MSGraph`, danach ~10 Konsumenten-Repos (siehe §6)

## 1 Kontext & Problem

Mit dem Tenant-Wechsel (Bertelsmann) sind SharePoint/OneDrive umgezogen. Der bisherige
Zugriffsweg ist damit doppelt tot:

- `MSGraph::get_sharepoint_data()` u. a. authentifizieren **app-only**
  (`MSGraph::authorize_graph()` mit Client-Secret) und haben Tenant-ID (`f34ad13c…`,
  alter Studyflix-Tenant), Client-ID und die Gruppen-GUID der „Studyflix Cloud"
  **im Code hardcoded** — teils im Paket, teils in App-Funktionen
  (z. B. `base-18/func/move_tmp_to_sharepoint.R`).
- Im neuen Tenant gibt es **nur eine Zusage für delegierte Rechte** (kein app-only,
  kein Device-Code — vgl. base-62/Bertelsmann-Review).

n8n greift im neuen Tenant bereits erfolgreich delegiert auf die Files zu — **ohne**
periodische Re-Authentifizierung. Der Mechanismus dahinter ist OAuth2-Refresh-Token-
Rotation: Beim Token-Refresh liefert Entra ein neues Refresh-Token mit, das persistiert
wird; die Kette rollt unbegrenzt, solange mindestens alle ~90 Tage ein Refresh
stattfindet. Genau dieses Muster existiert bereits als bewiesener Code in
`dbconnectorR::msgraph_make_delegated_token_provider()` (base-62). Ein turnusmäßiges
Erneuerungs-Ritual (z. B. alle 7 Tage) ist unnötig.

**Entscheidung (Moritz, 2026-08-18):** Der Code-Zugriff läuft über **dieselbe
App-Registrierung und denselben Service-Account wie n8n** — bewiesenermaßen
funktionierend, Admin-Consent für die SharePoint-Scopes existiert dort bereits.
Der R-Code bekommt per einmaligem Bootstrap ein **eigenes** Refresh-Token
(eigener Store, kein Konflikt mit n8n).

## 2 Ziele & Nicht-Ziele

**Ziele**

1. Zentrale delegierte MS-Graph-Auth als Billomatics-Service `msgraph_sharepoint`
   (Billomatics steckt in jedem Repo — keine neue Dependency).
2. Delegierte Pendants der SharePoint-File-Funktionen in `Package-01-MSGraph`.
3. Kein Hardcoding mehr: Tenant/Client/Secret/Site kommen aus **einer**
   verschlüsselten Key-Datei; ein künftiger Tenant-/App-Wechsel = eine Datei tauschen.
4. Migration aller Konsumenten-Repos mit minimalem Diff pro Call-Site
   (Service-Eintrag + Funktionsname).

**Nicht-Ziele**

- `base-62` / `dbconnectorR` werden **nicht** angefasst (läuft produktiv im
  Transkript-Cutover). Das bewusste Code-Duplikat der Provider-Logik wird als
  späteres Konsolidierungs-Ticket geführt.
- Die alten app-only-Funktionen werden **nicht** entfernt (Haus-Regel); sie werden
  nur in der Doku als Legacy (alter Tenant) markiert.
- Delegierte `copy_folder_*`-Varianten entstehen erst, wenn ein migriertes Repo sie
  braucht (YAGNI).

## 3 Architektur

### 3.1 Billomatics (Auth-Ebene)

Neuer Service **`msgraph_sharepoint`** — bewusst getrennt vom bestehenden
`msgraph_delegated` (der gehört der base-62-Transkript-App, anderes Client-Secret,
anderer Store).

- **`authentication_msgraph_sharepoint(args)`** — eingehängt in
  `authentication_process()`. Liest `../../keys/Microsoft365R/msgraph_sharepoint.txt`
  (safer-verschlüsselt), Inhalt ist **ein JSON** mit:
  `tenant_id`, `client_id`, `client_secret`, `store_key`, `store_path`, `site_url`.
  Rückgabe: named list mit genau diesen Feldern.
- **`msgraph_sharepoint_token_provider(auth, scopes = …)`** — baut die
  Provider-Closure `function(force_refresh = FALSE) -> access_token`:
  Store lesen → Refresh am Token-Endpoint → bei Rotation neues Refresh-Token
  **atomar** zurückschreiben. Default-Scopes:
  `https://graph.microsoft.com/Files.ReadWrite.All`,
  `https://graph.microsoft.com/User.Read`, `offline_access`.
  Der Provider wird **pro Session gecacht** (Package-Env, Key = client_id+store_path),
  damit mehrere SharePoint-Calls in einem Skript ein Access-Token teilen.
- **`msgraph_sharepoint_bootstrap(auth, auth_code)`** — einmaliger Headless-Bootstrap
  auf dem Server (Bertelsmann-Vorgabe: Token entsteht **auf** dem Server):
  Authorization-Code-Flow, Confidential Client, Redirect `http://localhost:1410/`
  (Web-Redirect an der App). `auth_code` akzeptiert den puren Code **oder** die ganze
  kopierte Redirect-URL (Vorbild: `base-62/do/bootstrap_delegated_store_headless.R`).
  Schreibt den Store, verifiziert per `/me`-Probe.
- **Interne, nicht exportierte Helfer:** Store read/write (tmp + rename + `.bak`),
  Refresh-POST (Fehler werfen mit AADSTS-Code im Text), Inaktivitätswarnung.
  Nicht-Export vermeidet Namenskonflikte mit dbconnectorR in Sessions, die beide
  Pakete laden (base-62).

### 3.2 Package-01-MSGraph (File-Ebene)

Neue Funktionen, alte bleiben unverändert stehen:

- **`get_sharepoint_data_delegated(folder_path, file_name, file_type, auth, tmp_folder, sheet = 1)`**
  — Verhalten und Rückgabe identisch zu `get_sharepoint_data()`; statt `msgraph_key`
  kommt `auth` (= `keys$msgraph_sharepoint`) herein, intern
  `Billomatics::msgraph_sharepoint_token_provider(auth)`.
- **`get_DriveItem_Info_delegated(path, auth)`** — Pfad→DriveItem-Auflösung.
- **`upload_to_sharepoint_delegated(local_files, sharepoint_folder, auth)`** —
  verallgemeinert base-18s `move_tmpJP5export_to_sharepoint()` (Upload per
  `PUT …:/content`, Fehler → `stop()` mit Status). base-18 behält einen dünnen
  App-Wrapper (tmp-Ordner leeren etc.).
- **Drive-Auflösung über die Site-URL** statt Gruppen-GUID:
  `GET /sites/{hostname}:/{site-path}` (aus `auth$site_url`) → `site_id` →
  `GET /sites/{site_id}/drive` → `drive_id`; Ergebnis pro Session gecacht.
  Alle Graph-Pfade laufen über `/drives/{drive_id}/…`.

## 4 Laufzeitverhalten Token

- **Ein gemeinsamer Store** für alle Repos auf dem Server:
  `keys/Microsoft365R/msgraph_sharepoint_refresh.txt` (Pfad steht als `store_path`
  in der Key-Datei). Einmal Bootstrap, alle Jobs teilen ihn.
- **Nebenläufigkeit ist unkritisch:** Schreiben ist atomar (tmp + rename + `.bak`);
  Entra invalidiert bei Confidential Clients das alte Refresh-Token bei Rotation
  nicht sofort — überlappende FlowForce-Jobs können sich nicht gegenseitig
  aussperren.
- **Kein Erneuerungs-Ritual.** Einziges reguläres Ablaufrisiko ist Inaktivität
  (~90 Tage ohne Refresh). Der Provider **warnt**, wenn der letzte erfolgreiche
  Refresh (`last_refreshed_at`) > 60 Tage her ist. (Bewusste Abweichung von
  dbconnectorR, das ab `obtained_at` misst — maßgeblich ist Inaktivität, nicht das
  Alter des interaktiven Logins.)
- **Fehler crashen laut** (FlowForce-Prinzip, kein blanket `tryCatch`): Refresh-Fehler
  werfen mit AADSTS-Code + Hinweis „Bootstrap erneut ausführen".

## 5 Einmaliges Server-Setup

1. JSON mit den Werten der n8n-App bauen (Tenant-ID, Client-ID, Client-Secret aus dem
   n8n-Credential; neuer zufälliger `store_key`; `store_path`; `site_url` aus dem
   n8n-Workflow ablesen), mit `safer` verschlüsseln →
   `keys/Microsoft365R/msgraph_sharepoint.txt`.
2. Bootstrap auf dem Server: Browser-Login als n8n-Service-Account, Code aus der
   Adresszeile kopieren, `Rscript`-Dreizeiler mit
   `msgraph_sharepoint_bootstrap()` → Store liegt, `/me`-Probe grün.
   Voraussetzung: Redirect `http://localhost:1410/` ist als Web-Redirect an der
   n8n-App eingetragen (sonst bei IT ergänzen lassen).
3. FlowForce: betroffene Jobs bekommen einen zusätzlichen Decrypt-Arg für
   `msgraph_sharepoint` (gleiche Mechanik wie bestehende Services).

## 6 Migrationsplan Konsumenten

**Reihenfolge:** Billomatics-PR → Package-01-MSGraph-PR → Server-Package-Reinstall →
**base-18** (Hauptleidtragender: alle `do/main_*.R`, `func/get_data_4_jp5export.R`,
Upload-Wrapper) → danach Repo für Repo:
base-14, base-15, base-07, base-19, base-48, base-43, shiny-29,
shiny-99-modules (`module_auto_pool_distribution`), base-11
(+ ggf. weitere Treffer aus einem finalen Grep über alle Repos).

**Pro Repo:** (a) `authentication_process()`-Call um `"msgraph_sharepoint"` ergänzen
bzw. `"msgraph"` ersetzen, (b) Funktionsnamen auf `*_delegated` tauschen,
(c) `folder_path`-Strings **prüfen** — ob die Ordnerstruktur im neuen SharePoint
identisch übernommen wurde, zeigt der n8n-Workflow bzw. ein Testlauf,
(d) PR, manueller Deploy (`gh workflow run "Deploy app"`), FlowForce-Arg ergänzen.

## 7 Tests

- **Billomatics (testthat, gemockter Token-Endpoint):** Refresh liefert Access-Token;
  Rotation schreibt Store und `.bak`; Antwort ohne neues Refresh-Token lässt Store
  unverändert; HTTP ≠ 200 wirft mit AADSTS-Code; Inaktivitätswarnung > 60 Tage;
  Provider-Cache liefert dasselbe Access-Token ohne zweiten POST.
  (Vorlage: dbconnectorR `test-msgraph_delegated_auth.R`.)
- **Smoke (one-off in base-18, vor erstem FlowForce-Lauf):** bekanntes File lesen +
  Upload in einen Test-Ordner, Roundtrip verifizieren.

## 8 Risiken & offene Punkte

| Punkt | Umgang |
|---|---|
| Client-Secret der n8n-App läuft ab (hartes Datum) | Reminder-Task anlegen; trifft n8n genauso — gemeinsames Ablaufdatum bei IT erfragen |
| Provider-Logik doppelt (dbconnectorR ↔ Billomatics) | bewusst akzeptiert; Konsolidierung = eigenes späteres Ticket |
| shiny-29 läuft im Shiny-Kontext | verifizieren, dass es als derselbe Server-User den Store lesen/schreiben kann |
| Conditional-Access sign-in frequency könnte künftig gesetzt werden | n8n-Empirie: aktuell nicht aktiv; falls doch, bricht der Refresh laut mit AADSTS → Bootstrap erneut |
| Konfig-Werte (IDs, Secret, Site-URL) | Setup-Input aus n8n-Credential/-Workflow, siehe §5 — vor Implementierungsbeginn beschaffen |
