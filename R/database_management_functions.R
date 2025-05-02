#' Setup Standard Triggers in a PostgreSQL Table
#'
#' Creates and attaches standard triggers to a PostgreSQL table using a DBI connection.
#' This includes:
#' \itemize{
#'   \item An `update_timestamp` trigger that updates the `updated_at` column when `first_name` or `name` changes.
#'   \item A `soft_delete` trigger that prevents actual deletions and instead sets `is_deleted = TRUE` and updates `updated_at`.
#' }
#'
#' @param con A `DBI` database connection.
#' @param schema A string specifying the schema name where the table resides.
#' @param table A string specifying the name of the table to attach triggers to.
#' @param soft_delete Logical. Whether to create a soft delete trigger. Defaults to `FALSE`.
#' @param update_timestamp Logical. Whether to create an updated_at trigger. Defaults to `FALSE`.
#'
#' @return No return value. Executes SQL statements on the database to define and attach the requested triggers.
#' @examples
#' \dontrun{
#' postgres_setup_standard_triggers(
#'   con = con,
#'   schema = "raw",
#'   table = "msgraph_users",
#'   soft_delete = TRUE,
#'   update_timestamp = TRUE
#' )
#' }
#' @export
postgres_setup_standard_triggers <- function(con, schema, table, soft_delete = FALSE, update_timestamp = TRUE) {
  full_table <- DBI::dbQuoteIdentifier(con, DBI::Id(schema = schema, table = table))

  if(update_timestamp){

    # Create or replace update_timestamp function
    DBI::dbExecute(con, "
      CREATE OR REPLACE FUNCTION update_timestamp() RETURNS trigger AS $$
      BEGIN
          -- Überprüfen, ob sich der Wert von 'first_name' oder 'name' geändert hat
          IF NEW.first_name IS DISTINCT FROM OLD.first_name OR NEW.name IS DISTINCT FROM OLD.name THEN
              -- Logging der Trigger-Aktivierung (optional)
              RAISE NOTICE 'Trigger update_timestamp fired for row: %', NEW.id;

              -- Setze das 'updated_at'-Feld nur, wenn eine Änderung stattgefunden hat
              NEW.updated_at = CURRENT_TIMESTAMP;
          END IF;

          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      ")

    # Drop and create updated_at trigger
    DBI::dbExecute(con, glue::glue("
      DROP TRIGGER IF EXISTS trigger_set_updated_at ON {full_table};"))

    DBI::dbExecute(con, glue::glue("
      CREATE TRIGGER trigger_set_updated_at
      BEFORE UPDATE ON {full_table}
      FOR EACH ROW
      EXECUTE FUNCTION update_timestamp();
    "))

  }

  if(soft_delete){

    # Create or replace soft_delete function
    DBI::dbExecute(con, glue::glue("
      CREATE OR REPLACE FUNCTION soft_delete()
      RETURNS trigger AS $$
      BEGIN
        RAISE NOTICE 'Trigger soft_delete fired for row: %', OLD.id;

        UPDATE {full_table}
        SET is_deleted = TRUE,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.id;

        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql;
    "))

    # Drop and create soft_delete trigger
    DBI::dbExecute(con, glue::glue("
      DROP TRIGGER IF EXISTS trigger_soft_delete ON {full_table};"))

    DBI::dbExecute(con, glue::glue("
      CREATE TRIGGER trigger_soft_delete
      BEFORE DELETE ON {full_table}
      FOR EACH ROW
      EXECUTE FUNCTION soft_delete();
    "))

  }

  message("✅ Trigger setup complete for ", schema, ".", table)
}


#' postgres_rename_table
#'
#' Renames an existing table in a PostgreSQL database, and optionally moves it to a different schema.
#' If the old and new table names are in different schemas, the table will be moved to the new schema and renamed.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the old table is located. If not provided in `old_name`, this argument is required.
#' @param old_name The current name of the table (can be in the format `schema.table`).
#' @param new_name The new name for the table (can be in the format `schema.table`).
#' @return A logical value indicating success (`TRUE`) or failure (`FALSE`). Additionally, a message will be printed
#' indicating whether the renaming was successful or if there was an error.
#' @details
#' - If the `old_name` or `new_name` includes both schema and table (i.e., `schema.table` format), the schema is extracted automatically.
#' - The function checks if the old table exists and ensures that the new table name is not already in use.
#' - If the old and new table names are in different schemas, the function will rename and move the table across schemas.
#' - If the old and new table names are in the same schema, the function will only rename the table.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_rename_table(connection, "public", "old_table_name", "new_table_name")
#' }
#'
#' @export
postgres_rename_table <- function(con, schema, old_name, new_name) {

  # Extract schema and table if provided in old_name
  if (grepl("\\.", old_name)) {
    parts <- strsplit(old_name, "\\.")[[1]]
    if (length(parts) == 2) {
      schema_old <- parts[1]
      old_name <- parts[2]
    } else {
      stop("Invalid format for `old_name`. Use 'schema.table' or provide `schema` separately.")
    }
  } else if (!is.null(schema)) {
    schema_old <- schema
  } else {
    stop("Schema must be provided either in `old_name` or via the `schema` argument.")
  }

  # Extract schema and table if provided in new_name
  if (grepl("\\.", new_name)) {
    parts <- strsplit(new_name, "\\.")[[1]]
    if (length(parts) == 2) {
      schema_new <- parts[1]
      new_name <- parts[2]
    } else {
      stop("Invalid format for `new_name`. Use 'schema.table' or only table name.")
    }
  } else {
    schema_new <- schema_old
  }

  # Ensure the old table exists
  exists_query <- sprintf("SELECT to_regclass('%s.%s');", schema_old, old_name)
  exists_result <- DBI::dbGetQuery(con, exists_query)

  if (is.null(exists_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' does not exist.", schema_old, old_name))
  }

  # Ensure the new table name doesn't already exist
  exists_new_query <- sprintf("SELECT to_regclass('%s.%s');", schema_new, new_name)
  exists_new_result <- DBI::dbGetQuery(con, exists_new_query)

  if (!is.null(exists_new_result[[1]])) {
    stop(sprintf("Error: Table '%s.%s' already exists.", schema_new, new_name))
  }

  # Perform rename or move+rename if schemas differ
  tryCatch({
    if (schema_old == schema_new) {
      # Just rename within same schema
      query <- sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_old, old_name, new_name)
    } else {
      # Rename and move across schemas
      temp_rename <- paste0("temp_", as.integer(Sys.time()))
      queries <- c(
        sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_old, old_name, temp_rename),
        sprintf("ALTER TABLE %s.%s SET SCHEMA %s;", schema_old, temp_rename, schema_new),
        sprintf("ALTER TABLE %s.%s RENAME TO %s;", schema_new, temp_rename, new_name)
      )
      query <- paste(queries, collapse = " ")
    }

    DBI::dbExecute(con, query)
    message(sprintf("✅ Table '%s.%s' renamed to '%s.%s'", schema_old, old_name, schema_new, new_name))
    return(TRUE)
  }, error = function(e) {
    message("❌ Error renaming table: ", e$message)
    return(FALSE)
  })
}

#' postgres_add_column
#'
#' Adds a new column to an existing table in PostgreSQL.
#' This function checks if the column already exists, and if not, it adds the specified column to the table with the given type.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table to which the column will be added.
#' @param column_name The name of the new column to add.
#' @param column_type The data type of the new column (e.g., "text", "integer", "boolean").
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the specified column already exists in the given table and schema.
#' - If the column does not exist, it constructs and executes an `ALTER TABLE` query to add the new column.
#' - If the column already exists, the function raises an error and halts the operation.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_add_column(connection, "raw", "my_table", "new_column", "text")
#' }
#'
#' @export
postgres_add_column <- function(con, schema, table, column_name, column_type) {

  # Extract schema and table if combined in `table`
  if (grepl("\\.", table)) {
    parts <- strsplit(table, "\\.")[[1]]
    if (length(parts) == 2) {
      schema <- parts[1]
      table <- parts[2]
    } else {
      stop("Invalid table format. Use 'schema.table' or specify schema separately.")
    }
  } else if (is.null(schema)) {
    stop("Schema must be provided either via `schema` argument or in `table` as 'schema.table'.")
  }

  # Check if the column already exists in the table
  check_query <- sprintf("
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';
  ", schema, table, column_name)

  column_exists <- DBI::dbGetQuery(con, check_query)

  if (nrow(column_exists) > 0) {
    stop(sprintf("Error: Column '%s' already exists in table '%s.%s'.", column_name, schema, table))
  }

  # Construct the query to add the new column
  query <- sprintf("
    ALTER TABLE %s.%s ADD COLUMN %s %s;
  ", schema, table, column_name, column_type)

  # Try to execute the query
  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Column '%s' of type '%s' has been successfully added to '%s.%s'.", column_name, column_type, schema, table))
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    message("Error adding column: ", e$message)
    return(FALSE)  # Return FALSE on failure
  })
}


#' postgres_change_column_type
#'
#' Changes the data type of an existing column in a PostgreSQL table.
#' This function allows altering the data type of a specific column in an existing table.
#' If the column doesn't exist, an error is raised.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table containing the column.
#' @param column_name The name of the column whose data type will be changed.
#' @param new_column_type The new data type for the column (e.g., "text", "integer", "boolean").
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the specified column exists in the table and schema.
#' - If the column exists, it constructs and executes an `ALTER TABLE` query to change the column's data type.
#' - If the column does not exist, the function raises an error and halts the operation.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_change_column_type(connection, "raw", "my_table", "my_column", "text")
#' }
#'
#' @export
postgres_change_column_type <- function(con, schema, table, column_name, new_column_type) {

  # Extract schema and table if combined in `table`
  if (grepl("\\.", table)) {
    parts <- strsplit(table, "\\.")[[1]]
    if (length(parts) == 2) {
      schema <- parts[1]
      table <- parts[2]
    } else {
      stop("Invalid table format. Use 'schema.table' or specify schema separately.")
    }
  } else if (is.null(schema)) {
    stop("Schema must be provided either via `schema` argument or in `table` as 'schema.table'.")
  }

  # Check if the column exists in the table
  check_query <- sprintf("
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';
  ", schema, table, column_name)

  column_exists <- DBI::dbGetQuery(con, check_query)

  if (nrow(column_exists) == 0) {
    stop(sprintf("Error: Column '%s' does not exist in table '%s.%s'.", column_name, schema, table))
  }

  # Construct the query to change the column type
  query <- sprintf("
    ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s;
  ", schema, table, column_name, new_column_type)

  # Try to execute the query
  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Column '%s' in table '%s.%s' has been successfully changed to type '%s'.", column_name, schema, table, new_column_type))
    return(TRUE)  # Return TRUE on success
  }, error = function(e) {
    message("Error changing column type: ", e$message)
    return(FALSE)  # Return FALSE on failure
  })
}

#' postgres_create_table
#'
#' Creates a new table in a PostgreSQL database.
#' This function allows creating a new table with specified column names and data types in a given schema.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table will be created.
#' @param table The name of the table to be created.
#' @param columns A named vector where names are column names and values are data types (e.g., c("column1" = "integer", "column2" = "text")).
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the schema is provided either via the `schema` argument or embedded in the `table` argument as 'schema.table'.
#' - The table creation SQL query is dynamically constructed from the provided column names and data types.
#' - If the table creation is successful, a success message is shown, and the function returns `TRUE`.
#' - If any error occurs during table creation, an error message is shown, and the function returns `FALSE`.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_create_table(connection, "raw", "new_table", c("id" = "integer", "name" = "text", "created_at" = "timestamp"))
#' }
#'
#' @export
postgres_create_table <- function(con, schema, table, columns) {
  column_definitions <- paste(names(columns), columns, collapse = ", ")

  # Extract schema and table if combined in `table`
  if (grepl("\\.", table)) {
    parts <- strsplit(table, "\\.")[[1]]
    if (length(parts) == 2) {
      schema <- parts[1]
      table <- parts[2]
    } else {
      stop("Invalid table format. Use 'schema.table' or specify schema separately.")
    }
  } else if (is.null(schema)) {
    stop("Schema must be provided either via `schema` argument or in `table` as 'schema.table'.")
  }

  query <- sprintf("CREATE TABLE %s.%s (%s);", schema, table, column_definitions)

  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Table '%s.%s' has been successfully created.", schema, table))
    return(TRUE)
  }, error = function(e) {
    message("Error creating table: ", e$message)
    return(FALSE)
  })
}

#' postgres_drop_table
#'
#' Drops a table from a PostgreSQL database.
#' This function allows dropping an existing table from the specified schema in the database.
#'
#' @param con The database connection object, created using DBI.
#' @param schema The schema where the table is located.
#' @param table The name of the table to be dropped.
#' @return A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
#' @details
#' - The function first checks if the schema is provided either via the `schema` argument or embedded in the `table` argument as 'schema.table'.
#' - The SQL query to drop the table is constructed and executed.
#' - If the table exists, it will be dropped. If it doesn't exist, no error will occur due to the `IF EXISTS` clause in the SQL statement.
#' - If the operation is successful, a success message is shown, and the function returns `TRUE`.
#' - If any error occurs during table deletion, an error message is shown, and the function returns `FALSE`.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
#' postgres_drop_table(connection, "raw", "old_table")
#' }
#'
#' @export
postgres_drop_table <- function(con, schema, table) {

  # Extract schema and table if combined in `table`
  if (grepl("\\.", table)) {
    parts <- strsplit(table, "\\.")[[1]]
    if (length(parts) == 2) {
      schema <- parts[1]
      table <- parts[2]
    } else {
      stop("Invalid table format. Use 'schema.table' or specify schema separately.")
    }
  } else if (is.null(schema)) {
    stop("Schema must be provided either via `schema` argument or in `table` as 'schema.table'.")
  }

  query <- sprintf("DROP TABLE IF EXISTS %s.%s;", schema, table)

  tryCatch({
    DBI::dbExecute(con, query)
    message(sprintf("Table '%s.%s' has been successfully dropped.", schema, table))
    return(TRUE)
  }, error = function(e) {
    message("Error dropping table: ", e$message)
    return(FALSE)
  })
}
