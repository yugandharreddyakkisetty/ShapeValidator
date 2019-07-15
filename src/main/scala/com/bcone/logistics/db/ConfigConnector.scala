package com.bcone.logistics.db

import java.util.HashMap

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, QuerySpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document._
import com.typesafe.scalalogging.{LazyLogging, Logger}


object ConfigConnector {

  def fetchAppConfig(dynamoDB: DynamoDB, key : String,logger: Logger) : String = {

    val spec:GetItemSpec= new GetItemSpec().withPrimaryKey("CONFIG_KEY",key).withConsistentRead(true)
    logger.info("Connecting to APPS_CONFIGS table")
    val item:Item = DynamoDBUtil.getItem(dynamoDB, "CTMS_APP_CONFIGS", spec)
    logger.info("Fetched table prefix")
    item.getString("CONFIG_VALUE")

  }

  def fetchConfig(dynamoDB: DynamoDB, key : String, table_prefix:String) : String = {


    val spec:GetItemSpec= new GetItemSpec().withPrimaryKey("KEY",key).withConsistentRead(true)
    val item:Item = DynamoDBUtil.getItem(dynamoDB, table_prefix+"ENV_CONFIG", spec)
    item.getString("VALUE")
  }

}