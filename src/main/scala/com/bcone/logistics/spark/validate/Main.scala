package com.bcone.logistics.spark.validate

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.bcone.logistics.db.{ConfigConnector, DictionaryConnector, RDSConnector, TransformationConnector}
import com.bcone.logistics.util.{DataTypes, ExceptionUtil, ExecutionStatus, PathProcessor}
import com.bcone.logistics.vo.DataDictionaryVo
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


import scala.util.matching.Regex

object Main  {
  def main(args: Array[String]): Unit = {


    val client: AmazonDynamoDBClient = new AmazonDynamoDBClient()
    val dynamoDB: DynamoDB = new DynamoDB(client)

    val logger=Logger("Root")


    // uncomment next six lines when running locally
    val spark = SparkSession.builder().master("local").appName("FileValidation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", new ProfileCredentialsProvider().getCredentials().getAWSSecretKey)
    val inputPath:String="D://bucket/LEVEL1/LEVEL2/LEVEL3/LEVEL4/sample.csv"
    val executionId:String=""
    val pipelineID:String=""
    val dictionaryName=""



    // uncomment if expression and spark session creation statement if you are running on cloud
   /* if(args.length < 2) {
      logger.error("Minimum arguments needed")
      throw new SparkException("Incorrect Input Arguments")
      return;
    }
      val inputPath:String=args(0).trim
      val executionId:String=args(1).trim
      val pipelineID=args(2).trim

    val spark = SparkSession.builder().appName("FileValidator").getOrCreate()
    logger.info("Spark session created")
    logger.info("Fetching Table prefix")*/

    // table prefixes : (Production:PROD_),(QA:QA_),(Devlopment:DEV_)
    val table_prefix=ConfigConnector.fetchAppConfig(dynamoDB,"TABLE_PREFIX",logger)

    // validating the file

    val validationStatus=dictionaryValidation(dynamoDB: DynamoDB,spark: SparkSession,inputPath:String,table_prefix:String,executionId:String,dictionaryName:String)
    if(validationStatus==ExecutionStatus.success) {
      println(ExecutionStatus.codes(validationStatus))
    }
    else {
      println(ExecutionStatus.codes(validationStatus))
    /*
    * Throwing a custom exception in case of an invalid file.
    * This is done to fail the step in EMR to discard all the succeeding steps in EMR.
    * */
      ExceptionUtil.throwCustomException(ExecutionStatus.codes(validationStatus))
    }
  }
  def dictionaryValidation(dynamoDB: DynamoDB, spark: SparkSession, path: String, table_prefix: String, executionId: String,dictionaryName:String):Int  = {

    // Validating file path
    val p=PathProcessor.generateFileNameAndFileConfigName(path)
    if(p.isEmpty) {
      println(ExecutionStatus.codes(ExecutionStatus.invalidPath))
      return ExecutionStatus.invalidPath
    }

    val fileConfigName :: fileName :: fileFormat :: other = p
   // val fileFormat=path.split('.')(1)

  /*  // file format validation
    val expectedFormat=ConfigConnector.fetchConfig(dynamoDB,fileConfigName+"_FORMAT",table_prefix)
    if(!fileFormat.equalsIgnoreCase(expectedFormat)) {
      println(ExecutionStatus.codes(ExecutionStatus.invalidFormat))
      println("Expected format: "+expectedFormat)
      println("Found format: "+fileFormat)
    }*/

    // Checking for the dictionary existence
    val dataDictionary=DictionaryConnector.fetchDataDictionary(dynamoDB,fileConfigName,table_prefix)
    if(dataDictionary.isEmpty){
      println(ExecutionStatus.codes(ExecutionStatus.dictionaryNotDefined))
      return ExecutionStatus.dictionaryNotDefined
    }



    val columnsInDictionary=dataDictionary.map(_.columnName)
    // loading input file into spark data frame
    val inputDf=spark.read.format(fileFormat.toLowerCase)
      .option("header","true").option("inferSchema","true").load(path)

  /*  // checking for the empty data frame
    if(inputDf.isEmpty)
      {
        println(ExecutionStatus.codes(ExecutionStatus.emptyFile))
        return ExecutionStatus.emptyFile
      }*/

    // Columns in the input file
    val columnsInInput=inputDf.columns

  /* // Checking for unnamed columns
    val pattern=new Regex("_c[0-9]*")
    val unNamedColumns=for(item<-columnsInInput) yield {
      pattern.findFirstIn(item) match
      {
        case Some(_) => item
        case None => null
      }
    }

    if(!unNamedColumns.filterNot( _== null).isEmpty) {
      println(ExecutionStatus.codes(ExecutionStatus.blankColumns))
      return ExecutionStatus.blankColumns
    }
    // checking for the duplicate columns
    val duplicateColumns=columnsInInput.groupBy(i=>i).mapValues(_.size).filter(p=>p._2>1)
    if(!duplicateColumns.isEmpty) {
      duplicateColumns.foreach(i=>println("Column "+i._1+" occurred "+i._2+" time(s)"))
      return ExecutionStatus.duplicateColumns
    }*/

    // Checking for the extra columns in the input file
    val extraColumns=columnsInInput.diff(columnsInDictionary)
    if(extraColumns.nonEmpty)
      {
        println(ExecutionStatus.codes(ExecutionStatus.extraColumns)+":"+extraColumns.mkString(","))
        return ExecutionStatus.extraColumns
      }
    // checking for the missing columns in the input file
    val missingColumns=columnsInDictionary.diff(columnsInInput)
    if(missingColumns.nonEmpty)
      {
        println(ExecutionStatus.codes(ExecutionStatus.missingColumn)+":"+missingColumns.mkString(","))
        return ExecutionStatus.missingColumn
      }
    /*// checking for the empty columns
    if(columnsInInput.contains("")||columnsInInput.contains(" ")) {
      println(ExecutionStatus.codes(ExecutionStatus.blankColumns))
      return ExecutionStatus.blankColumns
    }*/



    // validating the input files schema against data schema
    val inputSchema:StructType=inputDf.schema
    val schemaValidationStatus=validateSchema(dataDictionary,inputSchema)
    if(schemaValidationStatus!=ExecutionStatus.success) return schemaValidationStatus
    // Validate the input files for allowed values
    val allowedValuesValidationStatus=allowedValuesValidation(dataDictionary,inputDf)
    if(allowedValuesValidationStatus!=ExecutionStatus.success) return allowedValuesValidationStatus


    // Validate the input file for min max values
    val minMaxValidationStatus=minMaxValidation(dataDictionary,inputDf)
    if(minMaxValidationStatus!=ExecutionStatus.success) return  minMaxValidationStatus

    // Validate in the input file for maximum allowed number of characters in a column
    val lengthValidationStatus=lengthValidation(dataDictionary,inputDf)
    if(lengthValidationStatus!=ExecutionStatus.success) return  lengthValidationStatus

   ExecutionStatus.success

  }

  def validateSchema(dictionary:List[DataDictionaryVo],schema:StructType) :Int={

    /*
    * This method validate the schema against the data dictionary
    */
    val undefined=List(""," ","na","null")
    val typeDefinedColumns=dictionary.filterNot(d=>undefined.contains(d.dataType.toLowerCase))
    // Columns defined in the dictionary
    val dataDictionaryMap=typeDefinedColumns.map(d=>d.columnName -> d).toMap
     for(field <- schema.toList
       if !DataTypes.typeMapper(field.dataType.sql.toLowerCase).equalsIgnoreCase(dataDictionaryMap(field.name).dataType)) {
      println(field.name+" column's expected type is "+dataDictionaryMap(field.name).dataType+" but found "+DataTypes.typeMapper(field.dataType.sql.toLowerCase).toUpperCase)
      return ExecutionStatus.invalidSchema
   }
    ExecutionStatus.success
  }
  def allowedValuesValidation(dictionary:List[DataDictionaryVo],df:DataFrame) :Int = {
    val undefined=List(""," ","na","null")
    val allowedDefindColumns=dictionary.filterNot(d=>undefined.contains(d.allowedValues.toLowerCase))
    allowedDefindColumns.foreach{
      d=> {
        val l=d.allowedValues.split("\\|").toList
        if(!df.filter(!col(d.columnName).isin(l:_*)).isEmpty)
          {
            println("Allowed values in the column "+d.columnName+" "+l.mkString(","))
            return ExecutionStatus.allowedValues
          }
      }
    }

    ExecutionStatus.success
  }
  def minMaxValidation(dictionary:List[DataDictionaryVo],df:DataFrame):Int = {
    val undefined=List(""," ","na","null")
    val minDefinedColumns=dictionary.filterNot(d=>undefined.contains(d.minimumValue.toLowerCase))
    val maxDefinedColumns=dictionary.filterNot(d=>undefined.contains(d.maximumValue.toLowerCase))
    val ll=List("min","max")
    val l=df.describe().filter(col("summary").isin(ll:_*)).collect().toList.map(r=>r.getString(0)->r).toMap
    val minRow=l("min")
    val maxRow=l("max")

    minDefinedColumns.foreach{
      d => {
        val columnIndex=minRow.fieldIndex(d.columnName)
        val minValue=minRow.getString(columnIndex).toDouble
        if(d.minimumValue.toDouble > minValue) {
          println("Minimum value of the column "+d.columnName+" is "+minRow.getString(columnIndex))
          return ExecutionStatus.minimumValue
        }


      }
    }

    maxDefinedColumns.foreach{
      d => {
        val columnIndex=maxRow.fieldIndex(d.columnName)
        val maxValue=maxRow.getString(columnIndex).toDouble
        if(d.maximumValue.toDouble < maxValue) {
          println("Maximum value of the column "+d.columnName+" is "+maxRow.getString(columnIndex))
          return ExecutionStatus.maximumValue
        }


      }
    }

    ExecutionStatus.success

  }
  def lengthValidation(dictionary:List[DataDictionaryVo],df:DataFrame):Int ={

    val undefined=List(""," ","na","null")
    val lengthDefinedColumns=dictionary.filterNot(d=>undefined.contains(d.length.toString.toLowerCase))
    lengthDefinedColumns.foreach{
      d=> {
        if(!df.withColumn("Temp_Length_C!",length(col(d.columnName))).filter(col("Temp_Length_C!") > d.length).isEmpty) {
          println("Maximum  length (number of characters) of the column "+d.columnName+" is "+d.length)
          return ExecutionStatus.lengthCheck
        }
      }
    }


    ExecutionStatus.success
  }
}
