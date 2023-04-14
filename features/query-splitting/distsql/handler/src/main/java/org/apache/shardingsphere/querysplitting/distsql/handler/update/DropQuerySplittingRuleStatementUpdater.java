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

import org.apache.shardingsphere.distsql.handler.exception.rule.MissingRequiredRuleException;
import org.apache.shardingsphere.distsql.handler.exception.rule.RuleDefinitionViolationException;
import org.apache.shardingsphere.distsql.handler.exception.rule.RuleInUsedException;
import org.apache.shardingsphere.distsql.handler.update.RuleDefinitionDropUpdater;
import org.apache.shardingsphere.infra.datanode.DataNode;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rule.identifier.type.DataNodeContainedRule;
import org.apache.shardingsphere.infra.rule.identifier.type.DataSourceContainedRule;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.distsql.handler.checker.QuerySplittingRuleStatementChecker;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.DropQuerySplittingRuleStatement;
import org.apache.shardingsphere.querysplitting.rule.QuerySplittingRule;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Drop readwrite-splitting rule statement updater.
 */
public final class DropQuerySplittingRuleStatementUpdater implements RuleDefinitionDropUpdater<DropQuerySplittingRuleStatement, QuerySplittingRuleConfiguration> {
    
    @Override
    public void checkSQLStatement(final ShardingSphereDatabase database, final DropQuerySplittingRuleStatement sqlStatement,
                                  final QuerySplittingRuleConfiguration currentRuleConfig) throws RuleDefinitionViolationException {
        if (!isExistRuleConfig(currentRuleConfig) && sqlStatement.isIfExists()) {
            return;
        }
        String databaseName = database.getName();
        QuerySplittingRuleStatementChecker.checkRuleConfigurationExist(database, currentRuleConfig);
        checkToBeDroppedRuleNames(databaseName, sqlStatement, currentRuleConfig);
        checkToBeDroppedInUsed(database, sqlStatement);
    }
    
    private void checkToBeDroppedRuleNames(final String databaseName, final DropQuerySplittingRuleStatement sqlStatement,
                                           final QuerySplittingRuleConfiguration currentRuleConfig) throws MissingRequiredRuleException {
        if (sqlStatement.isIfExists()) {
            return;
        }
        Collection<String> currentRuleNames = currentRuleConfig.getDataSources().stream().map(QuerySplittingDataSourceRuleConfiguration::getName).collect(Collectors.toList());
        Collection<String> notExistedRuleNames = sqlStatement.getNames().stream().filter(each -> !currentRuleNames.contains(each)).collect(Collectors.toList());
        ShardingSpherePreconditions.checkState(notExistedRuleNames.isEmpty(), () -> new MissingRequiredRuleException("Readwrite splitting", databaseName, sqlStatement.getNames()));
    }
    
    private void checkToBeDroppedInUsed(final ShardingSphereDatabase database, final DropQuerySplittingRuleStatement sqlStatement) throws RuleInUsedException {
        Collection<String> resourceBeUsed = getInUsedResources(database);
        Collection<String> ruleInUsed = sqlStatement.getNames().stream().filter(resourceBeUsed::contains).collect(Collectors.toSet());
        ShardingSpherePreconditions.checkState(ruleInUsed.isEmpty(), () -> new RuleInUsedException("Readwrite splitting", database.getName(), ruleInUsed));
    }
    
    private Collection<String> getInUsedResources(final ShardingSphereDatabase database) {
        Collection<String> result = new HashSet<>();
        for (DataSourceContainedRule each : database.getRuleMetaData().findRules(DataSourceContainedRule.class)) {
            if (each instanceof QuerySplittingRule) {
                continue;
            }
            Collection<String> actualDataSources = new HashSet<>();
            each.getDataSourceMapper().values().forEach(actualDataSources::addAll);
            result.addAll(actualDataSources);
        }
        for (DataNodeContainedRule each : database.getRuleMetaData().findRules(DataNodeContainedRule.class)) {
            Collection<DataNode> actualDataNodes = new HashSet<>();
            each.getAllDataNodes().values().forEach(actualDataNodes::addAll);
            result.addAll(actualDataNodes.stream().map(DataNode::getDataSourceName).collect(Collectors.toSet()));
        }
        return result;
    }
    
    @Override
    public boolean updateCurrentRuleConfiguration(final DropQuerySplittingRuleStatement sqlStatement, final QuerySplittingRuleConfiguration currentRuleConfig) {
        for (String each : sqlStatement.getNames()) {
            dropRule(currentRuleConfig, each);
        }
        return currentRuleConfig.getDataSources().isEmpty();
    }
    
    private void dropRule(final QuerySplittingRuleConfiguration currentRuleConfig, final String ruleName) {
        Optional<QuerySplittingDataSourceRuleConfiguration> dataSourceRuleConfig = currentRuleConfig.getDataSources().stream().filter(each -> ruleName.equals(each.getName())).findAny();
        dataSourceRuleConfig.ifPresent(optional -> {
            currentRuleConfig.getDataSources().remove(optional);
            if (null != optional.getLoadBalancerName() && isLoadBalancerNotInUse(currentRuleConfig, optional.getLoadBalancerName())) {
                currentRuleConfig.getLoadBalancers().remove(optional.getLoadBalancerName());
            }
        });
    }
    
    private boolean isLoadBalancerNotInUse(final QuerySplittingRuleConfiguration currentRuleConfig, final String toBeDroppedLoadBalancerName) {
        return currentRuleConfig.getDataSources().stream().map(QuerySplittingDataSourceRuleConfiguration::getLoadBalancerName)
                .filter(Objects::nonNull).noneMatch(toBeDroppedLoadBalancerName::equals);
    }
    
    @Override
    public boolean hasAnyOneToBeDropped(final DropQuerySplittingRuleStatement sqlStatement, final QuerySplittingRuleConfiguration currentRuleConfig) {
        return null != currentRuleConfig && !getIdenticalData(currentRuleConfig.getDataSources().stream()
                .map(QuerySplittingDataSourceRuleConfiguration::getName).collect(Collectors.toSet()), sqlStatement.getNames()).isEmpty();
    }
    
    @Override
    public Class<QuerySplittingRuleConfiguration> getRuleConfigurationClass() {
        return QuerySplittingRuleConfiguration.class;
    }
    
    @Override
    public String getType() {
        return DropQuerySplittingRuleStatement.class.getName();
    }
}
