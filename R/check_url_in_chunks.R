# Install packages the first time
# install.packages("tidyverse")
# install.packages("tictoc")
# install.packages("furrr")
# install.packages("glue")
# install.packages("fs")

# Load packages ----
library(tidyverse)
library(furrr) # for running loops in parallel

# Define custom functions ----

#' Check the status of a URL
#' 
#' From [wikipedia](https://en.wikipedia.org/wiki/List_of_HTTP_status_codes), 
#' the response codes are as follows:
#' 
#' - 1xx informational response: the request was received, continuing process
#' - 2xx successful:  the request was successfully received, understood, and accepted
#' - 3xx redirection: further action needs to be taken in order to complete the request
#' - 4xx client error: the request contains bad syntax or cannot be fulfilled
#' - 5xx server error: the server failed to fulfil an apparently valid request
#'
#' @param x Input URL
#'
#' @return The status code of the URL. If the URL did not work at all,
#' "no response" is returned.
#'
#' @examples
#' # Inspired by https://stackoverflow.com/questions/52911812/check-if-url-exists-in-r
#' some_urls <- c(
#'   "http://content.thief/",
#'   "doh",
#'   NA,
#'   "http://rud.is/this/path/does/not_exist",
#'   "https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords=content+theft", 
#'   "https://rud.is/b/2018/10/10/geojson-version-of-cbc-quebec-ridings-hex-cartograms-with-example-usage-in-r/")
#' purrr::map_chr(some_urls, url_status)
#' 
url_status <- function (x) {
  
  # safe version of httr::HEAD
  sHEAD <- purrr::safely(httr::HEAD)
  
  # safe version of httr::GET
  sGET <- purrr::safely(httr::GET)
  
  # Return NA if input is NA
  if(is.na(x)) return (NA)
  
  # Check URL using HEAD
  # see httr::HEAD()
  # "This method is often used for testing hypertext links for validity, 
  # accessibility, and recent modification"
  res <- sHEAD(x)
  
  # If that returned an error or a non-200 range status (meaning the URL is broken)
  # try GET next
  if (is.null(res$result) || ((httr::status_code(res$result) %/% 200) != 1)) {
    
    res <- sGET(x)
    
    # If neither HEAD nor GET work, it's hard error
    if (is.null(res$result)) return("no response") # or whatever you want to return on "hard" errors
    
    return(httr::status_code(res$result))
    
  } else {
    
    return(httr::status_code(res$result))
    
  }
  
}

#' Write out a dataframe in chunks
#' 
#' The dataframe will be split in to `n_chunks` number of chunks,
#' and each chunk written to the current working directory.
#'
#' @param data Input dataframe
#' @param n_chunks Number of chunks to split the data into
#' @param base_name Initial part of file name for chunked data
#' @param file_ext File extension for chunked data
#'
#' @return Nothing; externally, the dataframe will be written in chunks named
#' like base_name_01.csv, base_name_02.csv, etc to the working directory
#' 
write_chunked_data <- function (data, n_chunks = 11, base_name = "chunked_data", file_ext = ".csv") { 
  
  chunked_data <-
    data %>%
    mutate(row_num = 1:nrow(.)) %>%
    mutate(chunk_num = cut_number(row_num, n_chunks)) %>%
    select(-row_num) %>%
    group_by(chunk_num) %>%
    group_split %>%
    map(~select(., -chunk_num))
  
  n_digits <- stringr::str_count(n_chunks)
  file_counter <- sprintf(glue::glue("%0{n_digits}d"), 1:n_chunks)
  
  walk2(chunked_data, glue::glue("{base_name}_{file_counter}{file_ext}"), ~write_csv(.x, .y))
  
}

# Set parallel backend ----
plan(multisession)

# Split data ----

# Read in the full data file
full_data <- read_csv("data/Sample_500.csv")

# Run this line to delete existing CSV files in "data_chunks" folder
# (for example, if some are leftover from a previous run and you don't want to
# mix them up)
# fs::file_delete(list.files("data_chunks", pattern = ".*csv", full.names = TRUE))

# Split up the data file into chunks, save each to the "data_chunks" folder
# (make sure this folder is empty first!)
write_chunked_data(full_data, base_name = "data_chunks/chunk", n_chunks = 20)

# Check URLs in chunks ----

# You may need repeat this step, adjusting the data chunks you choose each time
# until all are finished.

# First make a vector of all the CSV files in the "data_chunks" folder
data_files <- list.files("data_chunks", pattern = "chunk_.*csv", full.names = TRUE) %>%
  sort()

# Select which data chunks (CSV files) to load and check URLs by 
# changing the numbers in [].
# e.g., 
# if you want to load the first 10: data_files_to_load <- data_files[1:10]
# if you want to load all at once: data_files_to_load <- data_files
data_files_to_load <- data_files[11:12]

# Load selected chunks, check URLs, write out results
data_files_to_load %>%
  # - Load the selected data file chunks
  map(suppressMessages(read_csv)) %>%
  set_names(fs::path_file(data_files_to_load)) %>%
  # - Check the URL for each data chunk, in parallel
  map(
    ~mutate(.,
            dc.publisher.uri.status = future_map_chr(dc.publisher.uri, url_status),
            dc.relation.uri.status = future_map_chr(dc.relation.uri, url_status)
    )) %>%
  # - Write out the results in chunks
  walk2(., names(.), ~write_csv(.x, glue::glue("results_chunks/{.y}")))

# Combine the results and write out as a single CSV ----
# This can also be split up into chunks if it takes too much memory
list.files("results_chunks", pattern = "chunk_.*csv", full.names = TRUE) %>%
  map(suppressMessages(read_csv)) %>%
  map(~mutate(., across(everything(), as.character))) %>%
  bind_rows() %>%
  write_csv("results/url_check_results.csv")
