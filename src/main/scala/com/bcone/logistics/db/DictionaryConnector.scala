package com.bcone.logistics.db

import java.util.Map

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, ItemCollection, QueryOutcome}
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.bcone.logistics.vo.DataDictionaryVo
import scala.collection.JavaConversions._

object DictionaryConnector {
  def fetchDataDictionary(dynamoDB: DynamoDB, fileConfig : String, table_prefix:String) : List[DataDictionaryVo] = {

    val spec = new QuerySpec().withKeyConditionExpression("FILE_CONFIG_NAME = :v_partitionKey")
      .withValueMap(new ValueMap().withString(":v_partitionKey", fileConfig)
      )

    val items = DynamoDBUtil.queryItems(dynamoDB, table_prefix+"DATA_DICTIONARY", spec)

    generateDataDictionaryMap(items)
  }

  def generateDataDictionaryMap(items: ItemCollection[QueryOutcome]) : List[DataDictionaryVo] ={
    val dictionary=items.toList.map{
      item => {
        DataDictionaryVo(fileConfigName = item.getString("FILE_CONFIG_NAME"),
          columnName = item.getString("COLUMN_NAME"),
          comments = item.getString("COMMENTS"),
          dataType = item.getString("DATA_TYPE"),
          dataFormat = item.getString("DATA_FORMAT"),
          isNull = item.getBoolean("NULLABLE"),
          isUnique = item.getBoolean("UNIQUE"),
          allowedValues=if(item.getString("ALLOWED_VALUES")==null) "na" else item.getString("ALLOWED_VALUES"),
          minimumValue=if(item.getString("MIN_VALUE")==null) "na" else item.getString("MIN_VALUE"),
          maximumValue=if(item.getString("MAX_VALUE")==null) "na" else item.getString("MAX_VALUE"),
          length=if(item.getString("LENGTH")==null) "na" else item.getString("LENGTH")
        )
      }

    }
    dictionary
  }
}
