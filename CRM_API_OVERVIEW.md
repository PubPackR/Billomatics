# CRM API Functions Overview

Übersicht aller verfügbaren Funktionen für die Interaktion mit der Central Station CRM API.

## Standard Pattern

Alle API-Funktionen folgen einem konsistenten Pattern:
- **Input:** `function(headers, df)` - Header für Authentifizierung + Dataframe mit Daten
- **Validierung:** Automatische Validierung mit Helper-Funktionen
- **Error Handling:** Status Code Checks mit aussagekräftigen Warnungen
- **Pflichtfelder:** `action` und `field_type` für Filterung (außer GET-Funktionen)

---

## 1. People API (`crm_api_people.R`)

### GET Functions
- **`get_central_station_contacts(api_key, pages, person_id, includes, methods)`**
  - Lädt alle Personen oder einzelne Person
  - Optional: `includes` Parameter (z.B. "tags custom_fields")
  - Optional: `methods` Parameter

### CREATE Functions
- **`create_person(headers, df)`**
  - Erstellt neue Person
  - **Pflicht:** `action="create"`, `field_type="person"`
  - **Optional:** `salutation`, `first_name`, `last_name`, `background`

### UPDATE Functions
- **`update_crm_person(headers, df)`**
  - Updated Person-Daten
  - **Pflicht:** `action="update"`, `field_type="person"`, `attachable_id`
  - **Optional:** `salutation`, `first_name`, `last_name`, `background`

### Utility Functions
- **`get_responsible_person(persons, employees, o_tag_responsible)`**
  - Ermittelt zuständige Person basierend auf Tags

---

## 2. Companies API (`crm_api_companies.R`)

### GET Functions
- **`get_central_station_companies(api_key, positions, pages)`**
  - Lädt alle Companies
  - Optional: `positions=TRUE` für Position-Daten

- **`search_company_by_name(headers, company_name)`**
  - Sucht Company nach Namen
  - Returns: `list(found, company)`

### CREATE Functions
- **`create_company(headers, df)`**
  - Erstellt neue Company
  - **Pflicht:** `action="create"`, `field_type="company"`, `company_name`

### Utility Functions
- **`prepare_companies_crm(companies_crm)`**
  - Bereitet Companies-Daten auf

---

## 3. Tags API (`crm_api_tags.R`)

### GET Functions
- **`get_central_station_single_tags(api_key, tag_to_export, pages)`**
  - Lädt alle person_ids mit einem bestimmten Tag

### CREATE Functions
- **`add_crm_tag(headers, df)`**
  - Fügt Tag hinzu
  - **Pflicht:** `action="add"`, `field_type="tag"`, `attachable_id`, `attachable_type`, `field_name`

### DELETE Functions
- **`remove_crm_tag(headers, df)`**
  - Entfernt Tag
  - **Pflicht:** `action="remove"`, `field_type="tag"`, `attachable_id`, `attachable_type`, `custom_fields_id`

---

## 4. Custom Fields API (`crm_api_custom_fields.R`)

### GET Functions
- **`get_crm_custom_fields(headers, df)`**
  - Lädt alle Custom Fields
  - **Pflicht:** `attachable_id`, `attachable_type`
  - Returns: `custom_fields_id`, `custom_fields_type_id`, `value`

### CREATE Functions
- **`add_crm_custom_fields(headers, df)`**
  - Fügt Custom Field hinzu
  - **Pflicht:** `action="add"`, `field_type="custom_field"`, `attachable_id`, `attachable_type`, `field_name`, `value`

### UPDATE Functions
- **`update_crm_custom_fields(headers, df)`**
  - Updated Custom Field
  - **Pflicht:** `action="update"`, `field_type="custom_field"`, `attachable_id`, `attachable_type`, `custom_fields_id`, `field_name`, `value`

### DELETE Functions
- **`remove_crm_custom_fields(headers, df)`**
  - Entfernt Custom Field
  - **Pflicht:** `action="remove"`, `field_type="custom_field"`, `attachable_id`, `attachable_type`, `custom_fields_id`

### Smart CRUD Functions
- **`manage_crm_custom_fields(headers, df)`**
  - Automatische CREATE/UPDATE/DELETE basierend auf `old_value` und `new_value`
  - **Pflicht:** `attachable_id`, `attachable_type`, `field_name`, `old_value`, `new_value`, `custom_fields_id`
  - Logik:
    - `old=NA, new=X` → CREATE
    - `old=X, new=Y` → UPDATE
    - `old=X, new=NA` → DELETE
    - `old=NA, new=NA` → SKIP

---

## 5. Contact Details API (`crm_api_contact_details.R`)

### GET Functions
- **`get_crm_contact_details(headers, df)`**
  - Lädt alle Contact Details (email, tel, homepage, sm)
  - **Pflicht:** `attachable_id`, `attachable_type`
  - Returns: Contact Details mit `contact_type` Spalte

### CREATE Functions
- **`add_contact_details(headers, df)`**
  - Fügt Contact Detail hinzu
  - **Pflicht:** `action="add"`, `field_type="contact_details"`, `attachable_id`, `attachable_type`, `field_name`, `atype`, `contact_detail_type`
  - `contact_detail_type`: "email", "tel", "homepage", "sm"

### DELETE Functions
- **`remove_contact_details(headers, df)`**
  - Entfernt Contact Detail
  - **Pflicht:** `action="remove"`, `field_type="contact_details"`, `attachable_id`, `attachable_type`, `custom_fields_id`

---

## 6. Addresses API (`crm_api_addresses.R`)

### GET Functions
- **`get_crm_addresses(headers, df)`**
  - Lädt alle Adressen
  - **Pflicht:** `attachable_id`, `attachable_type`
  - Returns: `address_id`, `atype`, `street`, `additional`, `zip`, `city`, `state`, `country`

### CREATE Functions
- **`add_crm_addresses(headers, df)`**
  - Fügt Adresse hinzu
  - **Pflicht:** `action="add"`, `field_type="address"`, `attachable_id`, `attachable_type`, `atype`, `street`, `city`, `zip`, `country`
  - **Optional:** `additional`, `state`

### DELETE Functions
- **`remove_crm_addresses(headers, df)`**
  - Entfernt Adresse
  - **Pflicht:** `action="remove"`, `field_type="address"`, `attachable_id`, `attachable_type`, `address_id`

---

## 7. Positions API (`crm_api_positions.R`)

### GET Functions
- **`get_crm_positions(headers, df)`**
  - Lädt alle Positions für Person(en)
  - **Pflicht:** `attachable_id`
  - Returns: `position_id`, `company_id`, `name`, `department`, `primary_function`, `former`

### CREATE Functions
- **`create_crm_position(headers, df)`**
  - Erstellt Position (Person-Company Beziehung)
  - **Pflicht:** `action="create"`, `field_type="position"`, `attachable_id`, `company_id`
  - **Optional:** `position_name`, `department`, `primary_function`, `former`

### UPDATE Functions
- **`update_crm_position(headers, df)`**
  - Updated Position
  - **Pflicht:** `action="update"`, `field_type="position"`, `attachable_id`, `position_id`
  - **Optional:** `position_name`, `department`, `company_id`, `company_name`, `primary_function`, `former`

---

## 8. Events API (`crm_api_events.R`)

### GET Functions
- **`get_central_station_cal_events(api_key, pages)`**
  - Lädt alle Calendar Events
  - Returns: Event-Daten

---

## 9. Protocols API (`crm_api_protocols.R`)

### GET Functions
- **`get_central_station_protocols(api_key, pages)`**
  - Lädt alle Protocols
  - Returns: Protocol-Daten

- **`get_central_station_attachments(api_key, protocol_ids)`**
  - Lädt Attachments für Protocols
  - Input: `protocol_ids` Vector
  - Returns: Attachment-Daten

---

## 10. Users API (`crm_api_users.R`)

### GET Functions
- **`get_crm_users(api_key, active_only = FALSE)`**
  - Lädt alle CRM Users
  - `active_only=TRUE`: Nur aktive User (1 API Call)
  - `active_only=FALSE`: Alle User mit `is_active` Flag (2 API Calls)
  - Returns: `crm_user_id`, `user_first_name`, `user_name`, `user_login`, `is_active`, `is_deleted`

---

## 11. Helper Functions (`crm_helpers.R`)

### Validation Functions
- `validate_required_columns(df, required_cols)`
- `validate_attachable_id(df)`
- `validate_attachable_type(df)`
- `validate_custom_fields_id(df)`
- `validate_field_name(df)`
- `validate_value(df)`
- `validate_not_empty(df, column_name)`
- `validate_atype(df)`
- `validate_positive_id(df, id_column_name, id_label)`

### Filter Functions
- `filter_by_field_and_action(df, field_type_value, action_value)`

### Mapping Functions
- **`map_country_to_code(country_name)`** - Exported
  - Deutsche Ländernamen → ISO Codes
  - Unterstützt: DACH, Europa, Weltweit (25+ Länder)

- **`map_state_to_code(state_name, country_code)`** - Exported
  - Bundesland/Kanton → Code (basierend auf Land)
  - DE: Bundesländer → BY, NW, etc.
  - AT: Bundesländer → 1-9
  - CH: Kantone → ZH, BE, etc.

- `map_state_to_code_de(state_name)` - Internal
- `map_state_to_code_at(state_name)` - Internal
- `map_canton_to_code_ch(canton_name)` - Internal

### Value Helper Functions
- `has_valid_value(df, field_name, row_index)` - Prüft ob Feld gültig
- `get_optional_value(df, field_name, row_index, convert_func)` - Holt optionalen Wert

### URL & Transform Functions
- `build_crm_delete_url(attachable_type, attachable_id, resource_type, resource_id)`
- `transform_attachable_type(attachable_type)` - "people" → "Person"

---

## 12. Data Processing (`crm_data_processing.R`)

### Utility Functions
- **`unnesting_lists(df_with_lists)`**
  - Unnested Listen in Dataframes

- **`create_single_table(get_central_station_contacts_result, list_to_unnest)`**
  - Erstellt Single Table aus Contacts

- **`create_full_table(get_central_station_contacts_result, list_to_unnest)`**
  - Erstellt Full Table aus Contacts

---

## 13. Attachments (`crm_process_attachments.R`)

### Functions
- **`get_attachment_information(api_key, attachment_id)`**
  - Lädt Attachment-Info

- **`download_attachment(headers, attachment_information)`**
  - Downloaded Attachment

- **`save_attachment(attachment_information, attachment_content, folder_path)`**
  - Speichert Attachment

---

## Beispiel Workflows

### Person mit Tags und Custom Fields erstellen
```r
# 1. Person erstellen
df_person <- data.frame(
  action = "create",
  field_type = "person",
  first_name = "Max",
  last_name = "Mustermann"
)
create_person(headers, df_person)

# 2. Tag hinzufügen
df_tag <- data.frame(
  action = "add",
  field_type = "tag",
  attachable_id = 123456,
  attachable_type = "people",
  field_name = "VIP"
)
add_crm_tag(headers, df_tag)

# 3. Custom Field hinzufügen
df_cf <- data.frame(
  action = "add",
  field_type = "custom_field",
  attachable_id = 123456,
  attachable_type = "people",
  field_name = 144729,  # USt-ID field type
  value = "DE123456789"
)
add_crm_custom_fields(headers, df_cf)
```

### Adresse mit Mapping hinzufügen
```r
df_addr <- data.frame(
  action = "add",
  field_type = "address",
  attachable_id = 123456,
  attachable_type = "people",
  atype = "work",
  street = "Musterstraße 1",
  city = "München",
  zip = "80331",
  country = map_country_to_code("Deutschland"),  # "DE"
  state = map_state_to_code("Bayern", "DE")      # "BY"
)
add_crm_addresses(headers, df_addr)
```

### Position erstellen
```r
df_pos <- data.frame(
  action = "create",
  field_type = "position",
  attachable_id = 123456,  # person_id
  company_id = 789012,
  position_name = "Geschäftsführer",
  department = "Management",
  primary_function = TRUE
)
create_crm_position(headers, df_pos)
```

---

## Hinweise

- Alle Funktionen verwenden konsistente Validierung
- Error Handling mit Status Code Checks
- `action` und `field_type` für automatisches Filtering
- Helper-Funktionen für DRY-Prinzip
- Mapping-Funktionen für Geo-Daten (DE, AT, CH, LI)
