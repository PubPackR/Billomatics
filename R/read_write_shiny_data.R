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


#' saveShinyData
#'
#' This function saves an R object to an RDS file in the folder base-data/shiny_files/name_of_the_app
#' If the directory does not exist, the function will create it.
#' If the file already exists, it will be overwritten

#' @param object The R object to be saved
#' @param file The file path relative to path_to_shiny_files/name_of_the_app
#' @return NULL invisibly

#' @export
saveShinyData <- function(object, file, path_to_shiny_files = "../../base-data/shiny_files") {

  # consistency checks
  if(!is.character(file)){
    stop("In Function saveShinyData: Variable 'file' has to be in character format.")
  }
  if(gsub(".*\\.([^\\.]+)$","\\1",file)!="rds"){
    stop("In Function saveShinyData: Variable 'file' has to end on .rds")
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

#' readShinyData
#'
#' This function reads an RDS file in the folder base-data/shiny_files/name_of_the_app into an R object

#' @param file The file path relative to path_to_shiny_files/name_of_the_app
#' @return The R object that was stored in the file

#' @export
readShinyData <- function(file, path_to_shiny_files = "../../base-data/shiny_files") {

  # consistency checks
  if(!is.character(file)){
    stop("In Function readShinyData: Variable file has to be in character format.")
  }
  path <- paste0(path_to_shiny_files,"/",name_of_app(), "/", file)
  if(!file.exists(path)){
    stop(paste("In Function readShinyData: File",path,"not found"))
  }
  if (tools::file_ext(file_path) != "rds") {
    stop(paste("In Function readShinyData: File",path,"is not an .rds file"))
  }

  #save object to path
  object <- readRDS(file = path)
  return(object)
}


#' writeDataToConsole
#'
#' This function writes the structure of a data frame to the console.
#' In you want to hardcode the data into a module, you can copy the output of this function.

#' @param df The R object (normally a data frame) to be processed
#' @return The structure of the input is printed to the console

#' @export
writeDataToConsole <- function(df){
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
