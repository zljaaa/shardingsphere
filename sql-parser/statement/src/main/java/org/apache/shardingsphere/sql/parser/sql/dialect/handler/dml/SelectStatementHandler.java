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

package org.apache.shardingsphere.sql.parser.sql.dialect.handler.dml;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.order.GroupBySegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.limit.LimitSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.predicate.LockSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.ModelSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.WindowSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.WithSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.handler.SQLStatementHandler;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.MySQLStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dml.MySQLSelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.OpenGaussStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.opengauss.dml.OpenGaussSelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.oracle.OracleStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.oracle.dml.OracleSelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.postgresql.PostgreSQLStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.postgresql.dml.PostgreSQLSelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sql92.SQL92Statement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sql92.dml.SQL92SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sqlserver.SQLServerStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.sqlserver.dml.SQLServerSelectStatement;

import java.util.Optional;

/**
 * Select statement helper class for different dialect SQL statements.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SelectStatementHandler implements SQLStatementHandler {
    
    /**
     * Get limit segment.
     *
     * @param selectStatement select statement
     * @return limit segment
     */
    public static Optional<LimitSegment> getLimitSegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof MySQLStatement) {
            return ((MySQLSelectStatement) selectStatement).getLimit();
        }
        if (selectStatement instanceof PostgreSQLStatement) {
            return ((PostgreSQLSelectStatement) selectStatement).getLimit();
        }
        if (selectStatement instanceof SQL92Statement) {
            return ((SQL92SelectStatement) selectStatement).getLimit();
        }
        if (selectStatement instanceof SQLServerStatement) {
            return ((SQLServerSelectStatement) selectStatement).getLimit();
        }
        if (selectStatement instanceof OpenGaussStatement) {
            return ((OpenGaussSelectStatement) selectStatement).getLimit();
        }
        return Optional.empty();
    }
    
    /**
     * Get lock segment.
     *
     * @param selectStatement select statement
     * @return lock segment
     */
    public static Optional<LockSegment> getLockSegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof MySQLStatement) {
            return ((MySQLSelectStatement) selectStatement).getLock();
        }
        if (selectStatement instanceof OracleStatement) {
            return ((OracleSelectStatement) selectStatement).getLock();
        }
        if (selectStatement instanceof PostgreSQLStatement) {
            return ((PostgreSQLSelectStatement) selectStatement).getLock();
        }
        if (selectStatement instanceof OpenGaussStatement) {
            return ((OpenGaussSelectStatement) selectStatement).getLock();
        }
        return Optional.empty();
    }
    
    /**
     * Get window segment.
     *
     * @param selectStatement select statement
     * @return window segment
     */
    public static Optional<WindowSegment> getWindowSegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof MySQLStatement) {
            return ((MySQLSelectStatement) selectStatement).getWindow();
        }
        if (selectStatement instanceof PostgreSQLStatement) {
            return ((PostgreSQLSelectStatement) selectStatement).getWindow();
        }
        if (selectStatement instanceof OpenGaussStatement) {
            return ((OpenGaussSelectStatement) selectStatement).getWindow();
        }
        return Optional.empty();
    }
    
    /**
     * Get with segment.
     *
     * @param selectStatement select statement
     * @return with segment
     */
    public static Optional<WithSegment> getWithSegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof OracleStatement) {
            return ((OracleSelectStatement) selectStatement).getWithSegment();
        }
        if (selectStatement instanceof SQLServerStatement) {
            return ((SQLServerSelectStatement) selectStatement).getWithSegment();
        }
        return Optional.empty();
    }
    
    /**
     * Get model segment.
     *
     * @param selectStatement select statement
     * @return model segment
     */
    public static Optional<ModelSegment> getModelSegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof OracleStatement) {
            return ((OracleSelectStatement) selectStatement).getModelSegment();
        }
        return Optional.empty();
    }
    
    public static Optional<GroupBySegment> getGroupBySegment(final SelectStatement selectStatement) {
        if (selectStatement instanceof MySQLStatement) {
            return ((MySQLSelectStatement) selectStatement).getGroupBy();
        }
        if (selectStatement instanceof PostgreSQLStatement) {
            return ((PostgreSQLSelectStatement) selectStatement).getGroupBy();
        }
        if (selectStatement instanceof SQL92Statement) {
            return ((SQL92SelectStatement) selectStatement).getGroupBy();
        }
        if (selectStatement instanceof SQLServerStatement) {
            return ((SQLServerSelectStatement) selectStatement).getGroupBy();
        }
        if (selectStatement instanceof OpenGaussStatement) {
            return ((OpenGaussSelectStatement) selectStatement).getGroupBy();
        }
        return Optional.empty();
    }
}
