# cloudfunction-bigtable-to-bigquery
A Cloud Function, writted in Python, to perform the following steps:

  1) get data from a request;
  2) read a row from BigTable;
  3) transform the BigTable row to a BigQuery row;
  4) steraming insert the BigQuery row into BigQuery. 