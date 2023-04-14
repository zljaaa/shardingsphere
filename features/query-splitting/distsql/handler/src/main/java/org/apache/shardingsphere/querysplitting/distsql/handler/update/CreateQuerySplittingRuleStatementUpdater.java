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

package org.apache.shardingsphere.querysplitting.distsql.handler.update;

import org.apache.shardingsphere.distsql.handler.update.RuleDefinitionCreateUpdater;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.distsql.handler.checker.QuerySplittingRuleStatementChecker;
import org.apache.shardingsphere.querysplitting.distsql.handler.converter.QuerySplittingRuleStatementConverter;
import org.apache.shardingsphere.querysplitting.distsql.parser.segment.QuerySplittingRuleSegment;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.CreateQuerySplittingRuleStatement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * Create readwrite-splitting rule statement updater.
 */
public final class CreateQuerySplittingRuleStatementUpdater implements RuleDefinitionCreateUpdater<CreateQuerySplittingRuleStatement, QuerySplittingRuleConfiguration> {
    
    @Override
    public void checkSQLStatement(final ShardingSphereDatabase database, final CreateQuerySplittingRuleStatement sqlStatement, final QuerySplittingRuleConfiguration currentRuleConfig) {
        QuerySplittingRuleStatementChecker.checkCreation(database, sqlStatement.getRules(), currentRuleConfig, sqlStatement.isIfNotExists());
    }
    
    @Override
    public QuerySplittingRuleConfiguration buildToBeCreatedRuleConfiguration(final QuerySplittingRuleConfiguration currentRuleConfig,
                                                                             final CreateQuerySplittingRuleStatement sqlStatement) {
        Collection<QuerySplittingRuleSegment> segments = sqlStatement.getRules();
        if (sqlStatement.isIfNotExists()) {
            Collection<String> duplicatedRuleNames = getDuplicatedRuleNames(currentRuleConfig, sqlStatement.getRules());
            segments.removeIf(each -> duplicatedRuleNames.contains(each.getName()));
        }
        return QuerySplittingRuleStatementConverter.convert(segments);
    }
    
    @Override
    public void updateCurrentRuleConfiguration(final QuerySplittingRuleConfiguration currentRuleConfig, final QuerySplittingRuleConfiguration toBeCreatedRuleConfig) {
        currentRuleConfig.getDataSources().addAll(toBeCreatedRuleConfig.getDataSources());
        currentRuleConfig.getLoadBalancers().putAll(toBeCreatedRuleConfig.getLoadBalancers());
    }
    
    private Collection<String> getDuplicatedRuleNames(final QuerySplittingRuleConfiguration currentRuleConfig, final Collection<QuerySplittingRuleSegment> segments) {
        Collection<String> currentRuleNames = new LinkedList<>();
        if (null != currentRuleConfig) {
            currentRuleNames.addAll(currentRuleConfig.getDataSources().stream().map(QuerySplittingDataSourceRuleConfiguration::getName).collect(Collectors.toList()));
        }
        return segments.stream().map(QuerySplittingRuleSegment::getName).filter(currentRuleNames::contains).collect(Collectors.toList());
    }
    
    @Override
    public Class<QuerySplittingRuleConfiguration> getRuleConfigurationClass() {
        return QuerySplittingRuleConfiguration.class;
    }
    
    @Override
    public String getType() {
        return CreateQuerySplittingRuleStatement.class.getName();
    }
}
