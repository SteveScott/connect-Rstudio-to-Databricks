##################################################
####        connect_R_to_Databricks.r         ####
#### A tutorial on connecting R to Databricks ####
####        Vibe coded by Steve Scott         ####
##################################################

#######################################################################################################################
##### This script connects RStudio to Databricks using sparklyr and pysparklyr.                                   #####
##### It demonstrates how to connect to Databricks, list the tables, and load a table into a dplyr tibble.        #####
##### It installs the Databricks Connect client and establishes a connection to a specified cluster.              #####
#####                                                                                                             #####
##### Setting up the environment                                                                                  #####
##### Make sure to set your environment variables in your .Rprofile file as shown below.                          #####
##### You don't need to set the cluster version in the environment; the client will auto-detect it.               #####
#####                                                                                                             #####
##### The environment variables are set in the .Rprofile file                                                     #####
##### to open the Rprofile file, run:                                                                             #####
##### useits::edit_r_profile()                                                                                    #####
#####                                                                                                             #####
##### Variables you will need to set:                                                                             #####  
##### RETICULATE_PYTHON_ENV_METHOD = "conda": reticulate uses UV, a Rust library to download PyPi packages quickly#####                             
#####     this requires the uv.exe binary                                                                         ##### 
#####     I had to download uv.exe                                                                                #####
#####     download UV using the following powershell command:                                                     ##### 
#####          powershell -ExecutionPolicy ByPass -File "\\chgoldfs\operations\DEV_Team\R\install_uv.ps1"         #####
#####          add the directory containing uv.exe to your PATH environment variable                              #####
#####     if you go the UV route instead of conda,                                                                #####       
#####       make sure to remove the RETICULATE_PYTHON_ENV_METHOD line from your .Rprofile                         #####
#####                                                                                                             #####
##### DATABRICKS_HOST: Your Databricks workspace URL (e.g., "https://dbc-xxxx.cloud.databricks.com")              #####
##### DATABRICKS_TOKEN: Your Databricks personal access token (keep this secret!)                                 #####
##### HTTP_PROXY and HTTPS_PROXY: Your proxy settings (if applicable)                                             #####
##### CLUSTER_ID: The ID of the Databricks cluster you want to connect to                                         #####
#####                                                                                                             #####
##### This script assumes you have already set up your .Rprofile with the necessary environment variables.        #####
##### After changing the .Rprofile, restart your R session to load the new settings.                              #####
#####                                                                                                             #####                       
##### Also make sure that you have started the specified Databricks cluster.                                      #####
#####                                                                                                             ##### 
##### If you are unsure of your CLUSTER_ID, you may list it in the terminal                                       #####
##### to list available clusters, run in your R console:                                                          #####
##### databricks_clusters <- pysparklyr::databricks_clusters()                                                    #####
##### print(databricks_clusters)                                                                                  #####
##### Then choose the appropriate CLUSTER_ID for your connection.                                                 #####
#######################################################################################################################

# --- Load Libraries ---
suppressPackageStartupMessages({
  library(sparklyr)
  library(pysparklyr)
  library(dplyr)
  library(dbplyr)
  library(httr)
})

# --- STEP 1: Find and use the existing Python environment ---
# This logic mirrors the setup script to find the correct conda installation.

# First, check for the user's existing miniconda3
user_conda_path <- file.path(Sys.getenv("LOCALAPPDATA"), "miniconda3")
cat("Looking for your existing miniconda3 installation at:", user_conda_path, "\n")

if (dir.exists(user_conda_path)) {
  # If found, the base path is the miniconda3 directory
  cat("Existing miniconda3 found. Using it as the base path.\n")
  conda_base_path <- user_conda_path
} else {
  # If not found, the base path is the reticulate-managed r-miniconda directory
  cat("Existing miniconda3 not found. Using r-miniconda as the base path.\n")
  conda_base_path <- reticulate::miniconda_path()
}

# Define the environment name
env_name <- "r-sparklyr-databricks-14.3"

# Construct the full, direct path to the python.exe using the correct base path
python_path <- file.path(conda_base_path, "envs", env_name, "python.exe")

cat("Attempting to use Python executable at:", python_path, "\n")

# Check if the python.exe actually exists. If not, the setup script needs to be run.
if (!file.exists(python_path)) {
  stop(
    "FATAL: python.exe not found at the expected path.\n",
    "Please run the 'databricks-setup-script.r' script once to create the environment."
  )
}

# Force reticulate to use this specific Python executable
reticulate::use_python(python_path, required = TRUE)
cat("Successfully configured reticulate to use the correct Python environment.\n")


# --- STEP 2: Configure network settings for the proxy ---
cat("Forcing HTTP/1.1 and disabling SSL peer verification for proxy compatibility...\n")
httr::set_config(httr::config(http_version = 2, ssl_verifypeer = 0L))



# --- STEP 3: Test the basic REST API connection ---
# This uses the httr package directly to ensure basic network connectivity and
# authentication are working before attempting the more complex gRPC connection.
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
    httr::add_headers(Authorization = paste("Bearer", db_token)),
    httr::timeout(20) # Add a 20-second timeout to the API call
  )
  
  # Check for HTTP errors (like 401 Unauthorized or 403 Forbidden)
  httr::stop_for_status(response, "list clusters. Check your token and host URL.")
  
  # Parse the JSON response
  response_content <- httr::content(response, as = "text", encoding = "UTF-8")
  if (!require("jsonlite")) install.packages("jsonlite")
  clusters_list <- jsonlite::fromJSON(response_content)
  
  cat("\nSUCCESS: Direct REST API connection is working.\n")
  cat("Available clusters:\n")
  
  # Print the cluster information from the parsed list
  print(clusters_list$clusters %>% dplyr::select(cluster_id, cluster_name, state, spark_version))
  
}, error = function(e) {
  cat("\n-----------------------------------\n")
  cat("ERROR: Direct REST API connection test failed.\n")
  cat("This confirms a problem with your network proxy, firewall, or authentication token.\n")
  cat("The error message was:\n")
  cat(e$message, "\n")
  cat("-----------------------------------\n")
  stop("API test failed.")
})

# --- STEP 4: Connect to Databricks ---
tryCatch({
  cat("Attempting to connect to Databricks using 'databricks_connect'...\n")
  
  # Create a config object with a 30-second read timeout
  # This will prevent the script from hanging indefinitely if the proxy blocks the connection.
  conf <- sparklyr::spark_config()
  conf$spark.databricks.service.channel.read.timeout <- "30s"
  
  sc <- spark_connect(
    method = "databricks_connect",
    cluster_id = Sys.getenv("CLUSTER_ID"),
    config = conf  # Pass the configuration to the connection function
  )
  
  cat("Connection successful!\n")
}, error = function(e) {
  cat("Connection failed:", e$message, "\n")
  stop("Could not connect to Databricks")
})

# --- Use Connection ---
cat("Listing tables...\n")
DBI::dbListTables(sc)
