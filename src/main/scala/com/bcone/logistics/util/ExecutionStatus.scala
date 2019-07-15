package com.bcone.logistics.util

object ExecutionStatus {
  val invalidPath:Int=1001
  val extraColumns:Int=1002
  val missingColumn:Int=1003
  val blankColumns:Int=1004
  val dictionaryNotDefined:Int=1005
  val emptyFile:Int=1006
  val invalidSchema:Int=1007
  val duplicateColumns:Int=1008
  val invalidFormat:Int=1009
  val allowedValues:Int=1010
  val notAllowedValues:Int=1011
  val minimumValue:Int=1012
  val maximumValue:Int=1013
  val lengthCheck:Int=1014
  val success:Int=0
  val codes:Map[Int,String]=Map(
    0->"Validation Success",
    1001->"Invalid Path",
    1002->"Extra column(s) found",
    1003->"Missing column(s)",
    1004->"Blanks columns found",
    1005->"Dictionary Not Defined",
    1006->"Empty File",
    1007->"Invalid Schema",
    1008->"Duplicate columns found",
    1009->"Invalid file format",
    1010->"Other than allowed values found in the data set, refer the log for allowed values",
    1011->"Not allowed values found in the data set, refer the log for not allowed values",
    1012 ->"Minimum value condition not met",
    1013 ->"Maximum value condition not met",
    1014 ->"Maximum length of the column is exceeded")

}
