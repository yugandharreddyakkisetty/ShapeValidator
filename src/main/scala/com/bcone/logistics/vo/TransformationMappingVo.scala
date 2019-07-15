package com.bcone.logistics.vo
// used to hold the values of transformation mapping table of dynamodb

case class TransformationMappingVo (
                                   transformation:String,
                                   command:String,
                                   existingFormat:String,
                                   logic:String,
                                   order:Int=1,
                                   requiredFormat:String,
                                   outputColumn:String,
                                   dependentColumns:String,
                                   )

