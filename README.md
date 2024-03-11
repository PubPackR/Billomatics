# Billomatics
This package contains functions to get data and process it stemming from the contracts (Billomat) and the CRM (CentralStation).
# Installation

To install the package in your local R run: 

```
devtools::install_github("PubPackR/Billomatics",
                          ref = "main",
                          force = TRUE)
```
# Package Description

## get billomat data:

Billomatics allows to download, post, and extract data from/ to the Billomat software. 

### download the billomat data

The package allows to first call the api and drop information into a DBI database that is at a designated path. 
After the data is pulled, using the get_db_2_wide_df() function will turn this data into a rowwise df, each row being one complete item from the Billomat.

The data pulled from the API will be encrypted when stored in the DB, the encryption key has to be provided when storing and when loading the data.

### getting Information from the free text fields
Information of fields entered in the billomat in free text is stored using keys and text. The keys and text are separated by ":". With the get_extracted_information() function the respective field holding the information is turned into its own df, that contains only information from a specific field. 

To turn the field into the key and the text the function read_keys_FromDescription() is used. This function separates the text and key and also if EntryFormatCheck is false replaces known errors. If EntryFormatCheck is set to true, all errors will result in NAs.

#### Running times
To extract the laufzeit information from the keys and description df the function get_laufzeiten_information() is used. This function creates the variable Laufzeit_Start and Laufzeit_Ende and the id of the respective entry. 

The resulting dataframe can then be joined with the id to the billomat entry in the rowwise df.

```
## I want to keep the following keys 
 keep_keys <-
    c("Laufzeit",
      "Leistungsdatum",
      "Leistungszeitraum",
      "Leistungsbeginn",
      "Leistungsende",
      "Date of performance"
    ) %>% 
  paste(.,collapse = "|")

confirmation_items_current_laufzeit <- confirmation_items %>%  
  # extract the description, this means put it in a table consisting of key - value
  Billomatics::get_extracted_information(., "description") %>% 
  # get the keys
  Billomatics::read_KeysFromDescription(., sep = ":", EntryFormatCheck = FALSE) %>%
  # only keep keys that are relevant
  Billomatics::get_laufzeiten_information(., keep_keys = keep_keys,is_invoice = FALSE) %>% 
  # add the confirmation id
  left_join(confirmation_items, by = "id")

confirmations_complete <- confirmations %>%
   left_join(confirmation_items_current_laufzeit, by = c("id" = "confirmation_id")
```


#### Impressions ordered
To get the number of ordered impressions, distinguishing bonus and standard impressions, can be extracted using the function get_impressions_and_bonus(). The function needs to get the wide Billomat table conataining item specific informatino and then will return the impressions contracted. 

The resulting dataframe can then be joined with the id to the billomat entry in the rowwise df.

#### Tags on the client level
client Tags are not a standard information we use. For this reason tags are only in the long table in the billomat export. To get the table and filter it for the relevant fields:

```
Billomatics::get_tables_billomat(db_table_name = "clients", billomatDB = billomatDB,encryption_key_db = encryption_db)

```

- client-property-values.client-property-value.name
- client-property-values.client-property-value.value

This will then give you the tags. IF there are multiple tags, then you need to group them and give them each an index, as values and names are always grouped but do not have a unique unififying id.

## get comments function
Use this function to retrieve all confirmation comments for a given id.

## clear confirmations function 
Use this function to set statut to cleared a set of given confirmation ids.

### Pre processing functions to create a monthly dataset


### Functions to create revenue types and KPIs

### using the billomat data
First set the path to the DB

```billomatDB_path <- "../../base-data/Billomat/billomat.db"```

this code chunk loads the tables in the db

```billomatDB <- DBI::dbConnect(RSQLite::SQLite(), billomatDB_path)```

Now you have to pass the encryption key for the DB

```encryption_db <- getPass::getPass("Enter the password: ")```

to see all table in the DB use:

```DBI::dbListTables(billomatDB)```

to get a specific db into a wide table you should use get_db_2_wide_df()
```
offers <- Billomatics::get_db_2_wide_df(db_table_name = "offers",
                                               billomatDB = billomatDB,
                                               encryption_key_db = encryption_db)
```

## Getting CRM Data

The package is used to get and wrangle the data from the crm api.

### download data

Use the functions crm_api_call to get the data. The two arguments are the api key and whether you want a specific or all pages.

enter api key via the call:
```api_key <- getPass::getPass("Please enter your API-Key: ")```

The packages (httr)  and  (jsonlite) allow the call to the api of the CRM.

The script get_central_station_contacts() downloads all entries with the endpoint persons. If there are several entries in columns, they are mapped in lists of values in the json. The lists contain an attachable_id that links them to the entry. The following information is stored in lists:

- emails
- tels
- tags
- positions
- tasks
- pending tasks

The final file can then be saved as an RDS -- or processed directly.

### Preprocessing 
For further processing, the tags etc. must be transferred from the lists into a normal datatable.

The information from the lists is compiled into a final dataset using the functions_to_create_dataset() function. The key for linking the person and the respective entries is the variable "id".
The field name is stored in name and the field value in value. Then you can filter the names of the field that you are interested in and then make the table wide via pivot_wider(). Your name column should determine the names and value the values. This will result in a table with all your fields as column names and their respective values in rows.



