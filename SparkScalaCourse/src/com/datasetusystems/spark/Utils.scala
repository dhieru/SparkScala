package com.datasetusystems.spark

object Utils {
  // a regular expression which matches a comma but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
}