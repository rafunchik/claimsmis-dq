package com.gdp.ports

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

trait SecretManager {
  def getSecret(scope: String, key: String): String
}

object DatabricksSecretManager extends SecretManager {
  def getSecret(scope: String, key: String): String = {
    dbutils.secrets.get(scope = scope, key = key)
  }
}
