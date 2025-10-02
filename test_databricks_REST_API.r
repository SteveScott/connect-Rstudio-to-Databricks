# This script tests the basic Databricks REST API connection by listing available clusters.
# It uses the 'httr' package directly to make a raw API call, which is the most
# fundamental test of network and authentication settings.
# This test HTTPS REST. It does not test databricks-connect.
# databricks-connect instead uses gRPC which is more complex and can fail for other reasons.
# gRPC is high-performance, binary traffic over a persistant connection, which may be blocked by some proxies/firewalls.
# if this test passes, but connect_R_to_Dtabricks.r fails, then the problem is likely with gRPC traffic being blocked.

# --- Install and Load Libraries ---
if (!require("httr")) install.packages("httr")
if (!require("jsonlite")) install.packages("jsonlite") # For parsing the response
if (!require("dplyr")) install.packages("dplyr")
library(httr)
library(jsonlite)
library(dplyr)

# --- STEP 1: Configure network settings for the proxy ---
# This is critical because we are using httr directly.
cat("Forcing HTTP/1.1 and disabling SSL peer verification for proxy compatibility...\n")
httr::set_config(httr::config(http_version = 2, ssl_verifypeer = 0L))


# --- STEP 2: Test the API connection by listing clusters ---
tryCatch({
  cat("\nAttempting a direct API call to list Databricks clusters...\n")
  
  # Retrieve host and token from environment variables
  db_host <- Sys.getenv("DATABRICKS_HOST")
  db_token <- Sys.getenv("DATABRICKS_TOKEN")
  
  if (db_host == "" || db_token == "") {
    stop("DATABRICKS_HOST or DATABRICKS_TOKEN environment variables are not set.")
  }
  
  # Construct the API endpoint URL
  api_url <- paste0(db_host, "/api/2.0/clusters/list")
  
  # Make the GET request with the authorization header
  response <- httr::GET(
    url = api_url,
    httr::add_headers(Authorization = paste("Bearer", db_token))
  )
  
  # Check for HTTP errors (like 401 Unauthorized or 403 Forbidden)
  httr::stop_for_status(response, "list clusters. Check your token and host URL.")
  
  # Parse the JSON response
  response_content <- httr::content(response, as = "text", encoding = "UTF-8")
  clusters_list <- jsonlite::fromJSON(response_content)
  
  cat("\nSUCCESS: Direct REST API connection is working.\n")
  cat("Available clusters:\n")
  
  # Print the cluster information from the parsed list
  print(clusters_list$clusters %>% select(cluster_id, cluster_name, state, spark_version))
  
}, error = function(e) {
  cat("\n-----------------------------------\n")
  cat("ERROR: Direct REST API connection test failed.\n")
  cat("This confirms a problem with your network proxy, firewall, or authentication token.\n")
  cat("The error message was:\n")
  cat(e$message, "\n")
  cat("-----------------------------------\n")
  stop("API test failed.")
})
