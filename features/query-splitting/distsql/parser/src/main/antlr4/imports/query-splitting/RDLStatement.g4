/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar RDLStatement;

import BaseRule;

createQuerySplittingRule
    : CREATE QUERY_SPLITTING RULE ifNotExists? querySplittingRuleDefinition (COMMA_ querySplittingRuleDefinition)*
    ;

alterQuerySplittingRule
    : ALTER QUERY_SPLITTING RULE querySplittingRuleDefinition (COMMA_ querySplittingRuleDefinition)*
    ;

dropQuerySplittingRule
    : DROP QUERY_SPLITTING RULE ifExists? ruleName (COMMA_ ruleName)*
    ;

querySplittingRuleDefinition
    : ruleName LP_ (staticQuerySplittingRuleDefinition | dynamicQuerySplittingRuleDefinition) (COMMA_ algorithmDefinition)? RP_
    ;

staticQuerySplittingRuleDefinition
    : WRITE_STORAGE_UNIT EQ_ writeStorageUnitName COMMA_ READ_STORAGE_UNITS LP_ readStorageUnitsNames RP_
    ;

dynamicQuerySplittingRuleDefinition
    : AUTO_AWARE_RESOURCE EQ_ resourceName (COMMA_ WRITE_DATA_SOURCE_QUERY_ENABLED EQ_ writeDataSourceQueryEnabled)?
    ;

ruleName
    : IDENTIFIER_
    ;

writeStorageUnitName
    : storageUnitName
    ;

readStorageUnitsNames
    : storageUnitName (COMMA_ storageUnitName)*
    ;

ifExists
    : IF EXISTS
    ;

writeDataSourceQueryEnabled
    : TRUE | FALSE
    ;

ifNotExists
    : IF NOT EXISTS
    ;
