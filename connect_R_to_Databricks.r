#### connect_R_to_Databricks.r ####
### Vibe Coded by Steve Scott ####
# This script connects RStudio to Databricks using sparklyr and pysparklyr.
# It demonstrates how to connect to Databricks, list the tables, and load a table
# It installs the Databricks Connect client and establishes a connection to a specified cluster.

## Setting up the environment
# Make sure to set your environment variables in your .Rprofile file as shown below.
# You don't need to set the cluster version in the environment; the client will auto-detect it.

# Variables:
# DATABRICKS_HOST: Your Databricks workspace URL (e.g., "https://dbc-xxxx.cloud.databricks.com")
# DATABRICKS_TOKEN: Your Databricks personal access token
# HTTP_PROXY and HTTPS_PROXY: Your proxy settings (if applicable)
# CLUSTER_ID: The ID of the Databricks cluster you want to connect to

# This script assumes you have already set up your .Rprofile with the necessary environment variables.
# After changing the .Rprofile, restart your R session to load the new settings.
# Also make sure that you have started the specified Databricks cluster.


# If you are unsure of your CLUSTER_ID,
# to list available clusters, run in your R console:
# databricks_clusters <- pysparklyr::databricks_clusters()
# print(databricks_clusters)
# Then choose the appropriate CLUSTER_ID for your connection.


# --- Load Libraries ---
suppressPackageStartupMessages({
  library(sparklyr)
  library(pysparklyr)
  library(dplyr)
  library(dbplyr)
})


# The environment variables are set in the .Rprofile file
#to open the Rprofile file, run:
#useits::edit_r_profile()



# --- STEP 1: Install a specific, stable Databricks Connect Client ---
# We specify version '14.3.1' to bypass the version parsing bug.
# This only needs to be run once successfully.
tryCatch({
  cat("Installing Databricks Connect client version 14.3.1...\n")
  pysparklyr::install_databricks(version = "14.3.1")
  cat("Installation successful!\n")
}, error = function(e) {
  cat("Installation failed:", e$message, "\n")
  stop("Could not install the Databricks client.")
})


# --- STEP 2: Connect using the modern, simplified method ---
tryCatch({
  cat("Attempting to connect to Databricks using 'databricks_connect'...\n")
  
  # This method is simple. It only needs the cluster_id.
  # It automatically finds host, token, and proxy settings from the environment.
  sc <- spark_connect(
    method = "databricks_connect",
    cluster_id = Sys.getenv("CLUSTER_ID")
  )
  
  cat("Connection successful!\n")
}, error = function(e) {
  cat("Connection failed:", e$message, "\n")
  stop("Could not connect to Databricks")
})

# --- Use Connection ---
cat("Listing tables...\n")
DBI::dbListTables(sc)

# Example: Reference a specific table
cat("Referencing 'scorecard_fulcrum.scorecard_fulcrum_records.scorecard_gold_table_blockface'...\n")
scorecard_blockface <- tbl(sc, "scorecard_fulcrum.scorecard_fulcrum_records.scorecard_gold_table_blockface")
cat("Table referenced successfully!\n")
