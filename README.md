# Billomatics
Billomatics allows to download and extract the data from the Billomat software. 

The package allows to first call the api and drop information into a DBI database that is at a designated path. 
After the data is pulled, using the get_db_2_wide_df() function will turn this data into a rowwise df, each row being one complete item from the Billomat.

## getting Information from the free text fields
Information of fields entered in the billomat in free text is stored using keys and text. The keys and text are separated by ":". With the get_extracted_information() function the respective field holding the information is turned into its own df, that contains only information from a specific field. 

To turn the field into the key and the text the function read_keys_FromDescription() is used. This function separates the text and key and also if EntryFormatCheck is false replaces known errors. If EntryFormatCheck is set to true, all errors will result in NAs.

To extract the laufzeit information from the keys and description df the function get_laufzeiten_information() is used. This function creates the variable Laufzeit_Start and Laufzeit_Ende and the id of the respective entry. 

The resulting dataframe can then be joined with the id to the billomat entry in the rowwise df.
