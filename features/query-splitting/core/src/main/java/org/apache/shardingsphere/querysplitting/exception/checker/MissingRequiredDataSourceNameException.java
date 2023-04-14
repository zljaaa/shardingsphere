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

package org.apache.shardingsphere.querysplitting.exception.checker;

import org.apache.shardingsphere.infra.util.exception.external.sql.sqlstate.XOpenSQLState;
import org.apache.shardingsphere.querysplitting.exception.QuerySplittingSQLException;

/**
 * Missing required data source name exception.
 */
public final class MissingRequiredDataSourceNameException extends QuerySplittingSQLException {
    
    private static final long serialVersionUID = 8006957930250488016L;
    
    public MissingRequiredDataSourceNameException(final String databaseName) {
        super(XOpenSQLState.CHECK_OPTION_VIOLATION, 90, "Data source name is required in database `%s`.", databaseName);
    }
}