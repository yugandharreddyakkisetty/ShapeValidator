package com.bcone.logistics.util


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sha1, when, concat}
import org.apache.spark.sql.types.IntegerType

object SCDType2 {
  def handleType2Changes(currentDFWithoutRecordHash: DataFrame,
                         newDFWithoutRecordHash: DataFrame,
                         effectiveFromDate: String,
                         effectiveUptoDate: String,
                         finalTableName: String,
                         dropColsInHash: Array[String] = Array()): Unit = {
    //Record Hash Computation
   /* val newDFConcatColumns = newDFWithoutRecordHash.columns
      .filterNot(column => dropColsInHash.contains(column))
      .map(col)
      .reduce((column1, column2) => concat(column1, column2))

    val currentDFConcatColumns = currentDFWithoutRecordHash.columns
      .filterNot(column => dropColsInHash.contains(column))
      .map(col)
      .reduce((column1, column2) => concat(column1, column2))

    val newDF =newDFWithoutRecordHash.withColumn("record_hash", sha1(newDFConcatColumns))
    val currentDF=currentDFWithoutRecordHash.withColumn("record_hash",sha1(currentDFConcatColumns))

    */

    val newDF=getRecordHashedDF(newDFWithoutRecordHash,dropColsInHash)
    val currentDF=getRecordHashedDF(currentDFWithoutRecordHash,dropColsInHash)
    //Delete Records
    val deleteRecords = currentDF
      .as("left")
      .join(newDF.as("right"),
        col("left.record_hash") === col("right.record_hash"),
        "leftanti")
      .select("left.*")
      .withColumn("EFFECTIVE_UPTO",
        when(col("EFFECTIVE_UPTO").isNull, lit(effectiveUptoDate))
          .otherwise(col("EFFECTIVE_UPTO")))
      .cache()

    //Insert Records
    val insertRecords = newDF
      .as("left")
      .join(currentDF.filter(col("EFFECTIVE_UPTO").isNull).as("right"),
        col("left.record_hash") === col("right.record_hash"),
        "leftanti")
      .select("left.*")
      .withColumn("EFFECTIVE_FROM", lit(effectiveFromDate))
      .withColumn("EFFECTIVE_UPTO", lit(null).cast(IntegerType))
      .cache()

    //Unchanged Records
    val unchangedRecords =
      currentDF
        .as("left")
        .join(newDF.as("right"),
          col("left.record_hash") === col("right.record_hash"),
          "inner")
        .select("left.*")
        .cache()
    // Union of unchanged records, new records and changed records
    val result = unchangedRecords
      .select(currentDF.columns.map(x => col(x)): _*)
      .union(
        insertRecords
          .select(currentDF.columns.map(x => col(x)): _*)
      )
      .union(deleteRecords
        .select(currentDF.columns.map(x => col(x)): _*))

    result.write
      .option("mergeSchema", "true")
      .mode(SaveMode.Overwrite)
      .format("delta")
      .saveAsTable(finalTableName)
  }

  //Record Hash Computation

  def getRecordHashedDF(df:DataFrame,metaDataColumns:Array[String]=Array()) = {

    //concat columns
    val concatColumns = df.columns
      .filterNot(column => metaDataColumns.contains(column))
      .map(col)
      .reduce((column1, column2) => concat(column1, column2))
    // compute record hash
    df.withColumn("record_hash", sha1(concatColumns))
  }
}
