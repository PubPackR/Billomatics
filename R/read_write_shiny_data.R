################################################################################-
# ----- Description -------------------------------------------------------------
#
# These functions are for reading and writing data in shiny apps
#
# ------------------------------------------------------------------ #
# Authors@R: Tobias Aufenanger
# Date: 2024/07
#

################################################################################-
# ----- Start -------------------------------------------------------------------

#' name_of_app
#'
#' Returns the name of the currently running shiny app,
#' based on the current working directory
#'

#' @return the name of the app (last folder of the current working directory)

#' @export
name_of_app <- function(){
  #Returns the name of the current app
  name_of_app <- gsub(".*/([^/]+)$","\\1", getwd())
  return(name_of_app)
}


#' save_shiny_data
#'
#' This function saves an R object to an RDS file in the folder base-data/shiny_files/name_of_the_app
#' If the directory does not exist, the function will create it.
#' If the file already exists, it will be overwritten

#' @param object The R object to be saved
#' @param file The file path relative to path_to_shiny_files/name_of_the_app
#' @return NULL invisibly

#' @export
save_shiny_data <- function(object, file, path_to_shiny_files = "../../base-data/shiny_files") {

  # consistency checks
  if(!is.character(file)){
    stop("In Function saveShinyData: Variable 'file' has to be in character format.")
  }
  if(tools::file_ext(file) != "RDS"){
    stop("In Function saveShinyData: Variable 'file' has to end on .RDS")
  }

  #determine paths froms parameters
  name_of_file <- gsub(".*/([^/]+)$","\\1", file)
  file_path <- gsub(name_of_file, "", file)
  file_path <- gsub("/$", "", file_path)
  path <- paste0(path_to_shiny_files,"/",name_of_app(), "/", file_path)

  #create directory if not exists
  if(!dir.exists(path)){
    dir.create(path, recursive = TRUE)
  }

  # Save Object to path
  saveRDS(object = object, file = paste0(path,"/",name_of_file))
}


#' read_shiny_data
#'
#' This function reads an RDS file in the folder base-data/shiny_files/name_of_the_app into an R object

#' @param file The file path relative to path_to_shiny_files/name_of_the_app
#' @return The R object that was stored in the file

#' @export
read_shiny_data <- function(file, path_to_shiny_files = "../../base-data/shiny_files") {

  # consistency checks
  if(!is.character(file)){
    stop("In Function readShinyData: Variable file has to be in character format.")
  }
  path <- paste0(path_to_shiny_files,"/",name_of_app(), "/", file)
  if(!file.exists(path)){
    stop(paste("In Function readShinyData: File",path,"not found"))
  }
  if (tools::file_ext(file) != "RDS") {
    stop(paste("In Function readShinyData: File",path,"is not an .RDS file"))
  }

  #save object to path
  object <- readRDS(file = path)
  return(object)
}


#' write_data_to_console
#'
#' This function writes the structure of a data frame to the console.
#' In you want to hardcode the data into a module, you can copy the output of this function.

#' @param df The R object (normally a data frame) to be processed
#' @return The structure of the input is printed to the console

#' @export
write_data_to_console <- function(df){
  df <- lapply(df, function(x) {
    if (is.factor(x)) {
      as.character(x)
    } else if (lubridate::is.Date(x)) {
      as.character(x)
    } else {
      x
    }
  })

  # Verwende dput, um die Struktur auszugeben
  out <- paste0(capture.output(dput(df, control = c("keepNA","keepInteger","niceNames", "showAttributes"))), collapse = "\n")
  out <- gsub(", ([^ ]+ =)", ",\n\\1",out)
  cat(out)

}


#' get_shiny_data_path
#'
#' This function returns the filepath of a particular shiny data file
#' If file is left empty it returns the default location where data files of this app are stored

#' @param file (optional) The path to the file inside the default location
#' @param path_to_shiny_files (optional) the path to the folder where all shiny files are stored. The files of this app are stored in path_to_shiny_files/name_of_the_app
#' @return The path to the file

#' @export
get_shiny_data_path <- function(file = NULL, path_to_shiny_files = "../../base-data/shiny_files"){
  return(paste0(path_to_shiny_files,"/",name_of_app(), "/", file))
}
