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

package org.apache.shardingsphere.querysplitting.checker;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.shardingsphere.infra.algorithm.ShardingSphereAlgorithmFactory;
import org.apache.shardingsphere.infra.config.rule.checker.RuleConfigurationChecker;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.identifier.type.DataSourceContainedRule;
import org.apache.shardingsphere.infra.rule.identifier.type.DynamicDataSourceContainedRule;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.expr.InlineExpressionParser;
import org.apache.shardingsphere.querysplitting.algorithm.loadbalance.TransactionWeightReadQueryLoadBalanceAlgorithm;
import org.apache.shardingsphere.querysplitting.algorithm.loadbalance.WeightReadQueryLoadBalanceAlgorithm;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.DynamicQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.StaticQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.constant.QuerySplittingOrder;
import org.apache.shardingsphere.querysplitting.exception.algorithm.MissingRequiredReadDatabaseWeightException;
import org.apache.shardingsphere.querysplitting.exception.checker.*;
import org.apache.shardingsphere.querysplitting.spi.ReadQueryLoadBalanceAlgorithm;
import javax.sql.DataSource;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.ArrayList;

/**
 * Readwrite-splitting rule configuration checker.
 */
public final class QuerySplittingRuleConfigurationChecker implements RuleConfigurationChecker<QuerySplittingRuleConfiguration> {
    
    @Override
    public void check(final String databaseName, final QuerySplittingRuleConfiguration config, final Map<String, DataSource> dataSourceMap, final Collection<ShardingSphereRule> rules) {
        Collection<QuerySplittingDataSourceRuleConfiguration> configs = config.getDataSources();
        Preconditions.checkArgument(!configs.isEmpty(), "Readwrite-splitting data source rules can not be empty.");
        checkDataSources(databaseName, configs, dataSourceMap, rules);
        checkLoadBalancerDataSourceName(databaseName, configs, getLoadBalancer(config), rules);
    }
    
    private void checkDataSources(final String databaseName,
                                  final Collection<QuerySplittingDataSourceRuleConfiguration> configs, final Map<String, DataSource> dataSourceMap, final Collection<ShardingSphereRule> rules) {
        Collection<String> addedWriteDataSourceNames = new HashSet<>();
        Collection<String> addedReadDataSourceNames = new HashSet<>();
        for (QuerySplittingDataSourceRuleConfiguration each : configs) {
            ShardingSpherePreconditions.checkState(!Strings.isNullOrEmpty(each.getName()), () -> new MissingRequiredDataSourceNameException(databaseName));
            Preconditions.checkState(null != each.getStaticStrategy() || null != each.getDynamicStrategy(), "No available readwrite-splitting rule configuration in database `%s`.", databaseName);
            Optional.ofNullable(each.getStaticStrategy()).ifPresent(optional -> checkStaticStrategy(databaseName, dataSourceMap, addedWriteDataSourceNames, addedReadDataSourceNames, optional, rules));
            Optional.ofNullable(each.getDynamicStrategy()).ifPresent(optional -> checkDynamicStrategy(databaseName, rules, optional));
        }
    }
    
    private void checkStaticStrategy(final String databaseName, final Map<String, DataSource> dataSourceMap, final Collection<String> addedWriteDataSourceNames,
                                     final Collection<String> readDataSourceNames, final StaticQuerySplittingStrategyConfiguration strategyConfig, final Collection<ShardingSphereRule> rules) {
        ShardingSpherePreconditions.checkState(!Strings.isNullOrEmpty(strategyConfig.getMysqlDataSourceName()), () -> new MissingRequiredWriteDataSourceNameException(databaseName));
        ShardingSpherePreconditions.checkState(!strategyConfig.getSnowDataSourceNames().isEmpty(), () -> new MissingRequiredReadDataSourceNamesException(databaseName));
        checkWriteDataSourceNames(databaseName, dataSourceMap, addedWriteDataSourceNames, strategyConfig, rules);
        for (String each : readDataSourceNames) {
            checkReadeDataSourceNames(databaseName, dataSourceMap, readDataSourceNames, each);
        }
    }
    
    private void checkWriteDataSourceNames(final String databaseName, final Map<String, DataSource> dataSourceMap, final Collection<String> addedWriteDataSourceNames,
                                           final StaticQuerySplittingStrategyConfiguration strategyConfig, final Collection<ShardingSphereRule> rules) {
        for (String each : new InlineExpressionParser(strategyConfig.getMysqlDataSourceName()).splitAndEvaluate()) {
            ShardingSpherePreconditions.checkState(dataSourceMap.containsKey(each) || containsInOtherRules(each, rules),
                    () -> new DataSourceNameExistedException(String.format("Write data source name `%s` not in database `%s`.", each, databaseName)));
            ShardingSpherePreconditions.checkState(addedWriteDataSourceNames.add(each),
                    () -> new DuplicateDataSourceException(String.format("Can not config duplicate write data source `%s` in database `%s`.", each, databaseName)));
        }
    }
    
    private boolean containsInOtherRules(final String datasourceName, final Collection<ShardingSphereRule> rules) {
        for (ShardingSphereRule each : rules) {
            if (each instanceof DataSourceContainedRule && ((DataSourceContainedRule) each).getDataSourceMapper().containsKey(datasourceName)) {
                return true;
            }
        }
        return false;
    }
    
    private void checkReadeDataSourceNames(final String databaseName,
                                           final Map<String, DataSource> dataSourceMap, final Collection<String> addedReadDataSourceNames, final String readDataSourceName) {
        for (String each : new InlineExpressionParser(readDataSourceName).splitAndEvaluate()) {
            ShardingSpherePreconditions.checkState(dataSourceMap.containsKey(each),
                    () -> new DataSourceNameExistedException(String.format("Read data source name `%s` not in database `%s`.", each, databaseName)));
            ShardingSpherePreconditions.checkState(addedReadDataSourceNames.add(each),
                    () -> new DuplicateDataSourceException(String.format("Can not config duplicate read data source `%s` in database `%s`.", each, databaseName)));
        }
    }
    
    private void checkDynamicStrategy(final String databaseName, final Collection<ShardingSphereRule> rules, final DynamicQuerySplittingStrategyConfiguration dynamicStrategy) {
        ShardingSpherePreconditions.checkState(!Strings.isNullOrEmpty(dynamicStrategy.getAutoAwareDataSourceName()), () -> new MissingRequiredAutoAwareDataSourceNameException(databaseName));
        Optional<ShardingSphereRule> dynamicDataSourceStrategy = rules.stream().filter(each -> each instanceof DynamicDataSourceContainedRule).findFirst();
        Preconditions.checkArgument(dynamicDataSourceStrategy.isPresent(), "Dynamic data source strategy is required");
    }
    
    private void checkLoadBalancerDataSourceName(final String databaseName, final Collection<QuerySplittingDataSourceRuleConfiguration> configs,
                                                 final Map<String, ReadQueryLoadBalanceAlgorithm> loadBalancers, final Collection<ShardingSphereRule> rules) {
        for (QuerySplittingDataSourceRuleConfiguration each : configs) {
            if (Strings.isNullOrEmpty(each.getLoadBalancerName())) {
                continue;
            }
            ReadQueryLoadBalanceAlgorithm loadBalancer = loadBalancers.get(each.getLoadBalancerName());
            ShardingSpherePreconditions.checkNotNull(loadBalancer, () -> new LoadBalancerAlgorithmNotFoundException(databaseName));
            if (loadBalancer instanceof WeightReadQueryLoadBalanceAlgorithm || loadBalancer instanceof TransactionWeightReadQueryLoadBalanceAlgorithm) {
                ShardingSpherePreconditions.checkState(!loadBalancer.getProps().isEmpty(), () -> new MissingRequiredReadDatabaseWeightException(loadBalancer.getType(),
                        String.format("Read data source weight config are required in database `%s`", databaseName)));
                Collection<String> dataSourceNames = getDataSourceNames(each, rules);
                loadBalancer.getProps().stringPropertyNames().forEach(dataSourceName -> ShardingSpherePreconditions.checkState(dataSourceNames.contains(dataSourceName),
                        () -> new InvalidWeightLoadBalancerConfigurationException(databaseName)));
            }
        }
    }
    
    private List<String> getDataSourceNames(final QuerySplittingDataSourceRuleConfiguration config, final Collection<ShardingSphereRule> rules) {
        if (null != config.getStaticStrategy()) {
            return config.getStaticStrategy().getSnowDataSourceNames();
        }
        Optional<ShardingSphereRule> dynamicDataSourceStrategy = rules.stream().filter(each -> each instanceof DynamicDataSourceContainedRule).findFirst();
        if (!dynamicDataSourceStrategy.isPresent()) {
            return Collections.emptyList();
        }
        DynamicDataSourceContainedRule dynamicDataSourceRule = (DynamicDataSourceContainedRule) dynamicDataSourceStrategy.get();
        List<String> result = new ArrayList<>(dynamicDataSourceRule.getReplicaDataSourceNames(config.getDynamicStrategy().getAutoAwareDataSourceName()));
        result.add(dynamicDataSourceRule.getPrimaryDataSourceName(config.getDynamicStrategy().getAutoAwareDataSourceName()));
        return result;
    }
    
    private Map<String, ReadQueryLoadBalanceAlgorithm> getLoadBalancer(final QuerySplittingRuleConfiguration config) {
        Map<String, ReadQueryLoadBalanceAlgorithm> result = new LinkedHashMap<>(config.getLoadBalancers().size(), 1);
        config.getLoadBalancers().forEach((key, value) -> result.put(key, ShardingSphereAlgorithmFactory.createAlgorithm(value, ReadQueryLoadBalanceAlgorithm.class)));
        return result;
    }
    
    @Override
    public int getOrder() {
        return QuerySplittingOrder.ORDER;
    }
    
    @Override
    public Class<QuerySplittingRuleConfiguration> getTypeClass() {
        return QuerySplittingRuleConfiguration.class;
    }
}