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

package org.apache.shardingsphere.querysplitting.distsql.handler.query;

import org.apache.shardingsphere.distsql.handler.resultset.DatabaseDistSQLResultSet;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.CountQuerySplittingRuleStatement;
import org.apache.shardingsphere.querysplitting.rule.QuerySplittingRule;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Supplier;

/**
 * Result set for count readwrite splitting rule.
 */
public final class CountQuerySplittingRuleResultSet implements DatabaseDistSQLResultSet {
    
    private static final String QUERY_SPLITTING = "query_splitting";
    
    private Iterator<Entry<String, LinkedList<Object>>> data = Collections.emptyIterator();
    
    @Override
    public Collection<String> getColumnNames() {
        return Arrays.asList("rule_name", "database", "count");
    }
    
    @Override
    public void init(final ShardingSphereDatabase database, final SQLStatement sqlStatement) {
        Optional<QuerySplittingRule> rule = database.getRuleMetaData().findSingleRule(QuerySplittingRule.class);
        Map<String, LinkedList<Object>> result = new LinkedHashMap<>();
        rule.ifPresent(optional -> addQuerySplittingData(result, database.getName(), rule.get()));
        data = result.entrySet().iterator();
    }
    
    private void addQuerySplittingData(final Map<String, LinkedList<Object>> rowMap, final String databaseName, final QuerySplittingRule rule) {
        addData(rowMap, QUERY_SPLITTING, databaseName, () -> rule.getDataSourceMapper().size());
    }
    
    private void addData(final Map<String, LinkedList<Object>> rowMap, final String dataKey, final String databaseName, final Supplier<Integer> apply) {
        rowMap.compute(dataKey, (key, value) -> buildRow(value, databaseName, apply.get()));
    }
    
    private LinkedList<Object> buildRow(final LinkedList<Object> value, final String databaseName, final int count) {
        if (null == value) {
            return new LinkedList<>(Arrays.asList(databaseName, count));
        }
        value.set(1, (Integer) value.get(1) + count);
        return value;
    }
    
    @Override
    public boolean next() {
        return data.hasNext();
    }
    
    @Override
    public Collection<Object> getRowData() {
        Entry<String, LinkedList<Object>> entry = data.next();
        entry.getValue().addFirst(entry.getKey());
        return entry.getValue();
    }
    
    @Override
    public String getType() {
        return CountQuerySplittingRuleStatement.class.getName();
    }
}
