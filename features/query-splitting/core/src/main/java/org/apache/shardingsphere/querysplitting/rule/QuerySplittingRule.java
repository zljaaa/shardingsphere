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

package org.apache.shardingsphere.querysplitting.rule;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.shardingsphere.infra.algorithm.ShardingSphereAlgorithmFactory;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.metadata.database.schema.QualifiedDatabase;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.event.DataSourceStatusChangedEvent;
import org.apache.shardingsphere.infra.rule.identifier.scope.DatabaseRule;
import org.apache.shardingsphere.infra.rule.identifier.type.DataSourceContainedRule;
import org.apache.shardingsphere.infra.rule.identifier.type.StaticDataSourceContainedRule;
import org.apache.shardingsphere.infra.rule.identifier.type.StorageConnectorReusableRule;
import org.apache.shardingsphere.infra.rule.identifier.type.exportable.ExportableRule;
import org.apache.shardingsphere.infra.rule.identifier.type.exportable.constant.ExportableConstants;
import org.apache.shardingsphere.infra.rule.identifier.type.exportable.constant.ExportableItemConstants;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.expr.InlineExpressionParser;
import org.apache.shardingsphere.infra.util.spi.type.required.RequiredSPIRegistry;
import org.apache.shardingsphere.mode.metadata.storage.StorageNodeStatus;
import org.apache.shardingsphere.mode.metadata.storage.event.StorageNodeDataSourceChangedEvent;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.DynamicQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.StaticQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.exception.rule.InvalidInlineExpressionDataSourceNameException;
import org.apache.shardingsphere.querysplitting.spi.ReadQueryLoadBalanceAlgorithm;
import org.apache.shardingsphere.querysplitting.strategy.type.DynamicQuerySplittingStrategy;
import org.apache.shardingsphere.querysplitting.strategy.type.StaticQuerySplittingStrategy;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Readwrite-splitting rule.
 */
public final class QuerySplittingRule implements DatabaseRule, DataSourceContainedRule, StaticDataSourceContainedRule, ExportableRule, StorageConnectorReusableRule {
    
    @Getter
    private final RuleConfiguration configuration;
    
    private final Map<String, ReadQueryLoadBalanceAlgorithm> loadBalancers = new LinkedHashMap<>();
    
    private final Map<String, QuerySplittingDataSourceRule> dataSourceRules;
    
    public QuerySplittingRule(final QuerySplittingRuleConfiguration ruleConfig, final Collection<ShardingSphereRule> builtRules) {
        configuration = ruleConfig;
        ruleConfig.getDataSources().stream().filter(each -> null != ruleConfig.getLoadBalancers().get(each.getLoadBalancerName()))
                .forEach(each -> loadBalancers.put(each.getName() + "." + each.getLoadBalancerName(),
                        ShardingSphereAlgorithmFactory.createAlgorithm(ruleConfig.getLoadBalancers().get(each.getLoadBalancerName()), ReadQueryLoadBalanceAlgorithm.class)));
        dataSourceRules = new HashMap<>(ruleConfig.getDataSources().size(), 1);
        for (QuerySplittingDataSourceRuleConfiguration each : ruleConfig.getDataSources()) {
            dataSourceRules.putAll(createQuerySplittingDataSourceRules(each, builtRules));
        }
    }
    
    private Map<String, QuerySplittingDataSourceRule> createQuerySplittingDataSourceRules(final QuerySplittingDataSourceRuleConfiguration config,
                                                                                          final Collection<ShardingSphereRule> builtRules) {
        ReadQueryLoadBalanceAlgorithm loadBalanceAlgorithm = loadBalancers.getOrDefault(
                config.getName() + "." + config.getLoadBalancerName(), RequiredSPIRegistry.getRegisteredService(ReadQueryLoadBalanceAlgorithm.class));
        return null == config.getStaticStrategy()
                ? createDynamicQuerySplittingDataSourceRules(config, builtRules, loadBalanceAlgorithm)
                : createStaticQuerySplittingDataSourceRules(config, builtRules, loadBalanceAlgorithm);
    }
    
    private Map<String, QuerySplittingDataSourceRule> createStaticQuerySplittingDataSourceRules(final QuerySplittingDataSourceRuleConfiguration config,
                                                                                                final Collection<ShardingSphereRule> builtRules,
                                                                                                final ReadQueryLoadBalanceAlgorithm loadBalanceAlgorithm) {
        Map<String, QuerySplittingDataSourceRule> result = new LinkedHashMap<>();
        List<String> inlineReadwriteDataSourceNames = new InlineExpressionParser(config.getName()).splitAndEvaluate();
        List<String> inlineWriteDatasourceNames = new InlineExpressionParser(config.getStaticStrategy().getMysqlDataSourceName()).splitAndEvaluate();
        List<List<String>> inlineReadDatasourceNames = config.getStaticStrategy().getSnowDataSourceNames().stream()
                .map(each -> new InlineExpressionParser(each).splitAndEvaluate()).collect(Collectors.toList());
        ShardingSpherePreconditions.checkState(inlineWriteDatasourceNames.size() == inlineReadwriteDataSourceNames.size(),
                () -> new InvalidInlineExpressionDataSourceNameException("Inline expression write data source names size error"));
        inlineReadDatasourceNames.forEach(each -> ShardingSpherePreconditions.checkState(each.size() == inlineReadwriteDataSourceNames.size(),
                () -> new InvalidInlineExpressionDataSourceNameException("Inline expression read data source names size error")));
        for (int i = 0; i < inlineReadwriteDataSourceNames.size(); i++) {
            QuerySplittingDataSourceRuleConfiguration staticConfig = createStaticDataSourceRuleConfiguration(
                    config, i, inlineReadwriteDataSourceNames, inlineWriteDatasourceNames, inlineReadDatasourceNames);
            result.put(inlineReadwriteDataSourceNames.get(i), new QuerySplittingDataSourceRule(staticConfig, loadBalanceAlgorithm, builtRules));
        }
        return result;
    }
    
    private QuerySplittingDataSourceRuleConfiguration createStaticDataSourceRuleConfiguration(final QuerySplittingDataSourceRuleConfiguration config, final int index,
                                                                                              final List<String> readwriteDataSourceNames, final List<String> writeDatasourceNames,
                                                                                              final List<List<String>> readDatasourceNames) {
        List<String> readDataSourceNames = readDatasourceNames.stream().map(each -> each.get(index)).collect(Collectors.toList());
        return new QuerySplittingDataSourceRuleConfiguration(readwriteDataSourceNames.get(index),
                new StaticQuerySplittingStrategyConfiguration(writeDatasourceNames.get(index), readDataSourceNames), null, config.getLoadBalancerName());
    }
    
    private Map<String, QuerySplittingDataSourceRule> createDynamicQuerySplittingDataSourceRules(final QuerySplittingDataSourceRuleConfiguration config,
                                                                                                 final Collection<ShardingSphereRule> builtRules,
                                                                                                 final ReadQueryLoadBalanceAlgorithm loadBalanceAlgorithm) {
        Map<String, QuerySplittingDataSourceRule> result = new LinkedHashMap<>();
        List<String> inlineQueryDataSourceNames = new InlineExpressionParser(config.getName()).splitAndEvaluate();
        List<String> inlineAutoAwareDataSourceNames = new InlineExpressionParser(config.getDynamicStrategy().getAutoAwareDataSourceName()).splitAndEvaluate();
        ShardingSpherePreconditions.checkState(inlineAutoAwareDataSourceNames.size() == inlineQueryDataSourceNames.size(),
                () -> new InvalidInlineExpressionDataSourceNameException("Inline expression auto aware data source names size error"));
        for (int i = 0; i < inlineQueryDataSourceNames.size(); i++) {
            QuerySplittingDataSourceRuleConfiguration dynamicConfig = createDynamicDataSourceRuleConfiguration(config, i, inlineQueryDataSourceNames, inlineAutoAwareDataSourceNames);
            result.put(inlineQueryDataSourceNames.get(i), new QuerySplittingDataSourceRule(dynamicConfig, loadBalanceAlgorithm, builtRules));
        }
        return result;
    }
    
    private QuerySplittingDataSourceRuleConfiguration createDynamicDataSourceRuleConfiguration(final QuerySplittingDataSourceRuleConfiguration config, final int index,
                                                                                               final List<String> readwriteDataSourceNames, final List<String> autoAwareDataSourceNames) {
        return new QuerySplittingDataSourceRuleConfiguration(readwriteDataSourceNames.get(index), null,
                new DynamicQuerySplittingStrategyConfiguration(autoAwareDataSourceNames.get(index), config.getDynamicStrategy().getWriteDataSourceQueryEnabled()), config.getLoadBalancerName());
    }
    
    /**
     * Get single data source rule.
     *
     * @return readwrite-splitting data source rule
     */
    public QuerySplittingDataSourceRule getSingleDataSourceRule() {
        return dataSourceRules.values().iterator().next();
    }
    
    /**
     * Find data source rule.
     *
     * @param dataSourceName data source name
     * @return readwrite-splitting data source rule
     */
    public Optional<QuerySplittingDataSourceRule> findDataSourceRule(final String dataSourceName) {
        return Optional.ofNullable(dataSourceRules.get(dataSourceName));
    }
    
    @Override
    public Map<String, Collection<String>> getDataSourceMapper() {
        Map<String, Collection<String>> result = new HashMap<>();
        for (Entry<String, QuerySplittingDataSourceRule> entry : dataSourceRules.entrySet()) {
            result.put(entry.getValue().getName(), entry.getValue().getQuerySplittingStrategy().getAllDataSources());
        }
        return result;
    }
    
    @Override
    public void updateStatus(final DataSourceStatusChangedEvent event) {
        StorageNodeDataSourceChangedEvent dataSourceEvent = (StorageNodeDataSourceChangedEvent) event;
        QualifiedDatabase qualifiedDatabase = dataSourceEvent.getQualifiedDatabase();
        QuerySplittingDataSourceRule dataSourceRule = dataSourceRules.get(qualifiedDatabase.getGroupName());
        Preconditions.checkNotNull(dataSourceRule, "Can not find readwrite-splitting data source rule in database `%s`", qualifiedDatabase.getDatabaseName());
        dataSourceRule.updateDisabledDataSourceNames(dataSourceEvent.getQualifiedDatabase().getDataSourceName(), StorageNodeStatus.isDisable(dataSourceEvent.getDataSource().getStatus()));
    }
    
    @Override
    public Map<String, Object> getExportData() {
        Map<String, Object> result = new HashMap<>(2, 1);
        result.put(ExportableConstants.EXPORT_DYNAMIC_READWRITE_SPLITTING_RULE, exportDynamicDataSources());
        result.put(ExportableConstants.EXPORT_STATIC_READWRITE_SPLITTING_RULE, exportStaticDataSources());
        return result;
    }
    
    private Map<String, Map<String, String>> exportDynamicDataSources() {
        Map<String, Map<String, String>> result = new LinkedHashMap<>(dataSourceRules.size(), 1);
        for (QuerySplittingDataSourceRule each : dataSourceRules.values()) {
            if (each.getQuerySplittingStrategy() instanceof DynamicQuerySplittingStrategy) {
                Map<String, String> exportedDataSources = new LinkedHashMap<>(2, 1);
                exportedDataSources.put(ExportableItemConstants.AUTO_AWARE_DATA_SOURCE_NAME, ((DynamicQuerySplittingStrategy) each.getQuerySplittingStrategy()).getAutoAwareDataSourceName());
                exportedDataSources.put(ExportableItemConstants.PRIMARY_DATA_SOURCE_NAME, each.getWriteDataSource());
                exportedDataSources.put(ExportableItemConstants.REPLICA_DATA_SOURCE_NAMES, String.join(",", each.getQuerySplittingStrategy().getReadDataSources()));
                result.put(each.getName(), exportedDataSources);
            }
        }
        return result;
    }
    
    private Map<String, Map<String, String>> exportStaticDataSources() {
        Map<String, Map<String, String>> result = new LinkedHashMap<>(dataSourceRules.size(), 1);
        for (QuerySplittingDataSourceRule each : dataSourceRules.values()) {
            if (each.getQuerySplittingStrategy() instanceof StaticQuerySplittingStrategy) {
                Map<String, String> exportedDataSources = new LinkedHashMap<>(2, 1);
                exportedDataSources.put(ExportableItemConstants.PRIMARY_DATA_SOURCE_NAME, each.getWriteDataSource());
                exportedDataSources.put(ExportableItemConstants.REPLICA_DATA_SOURCE_NAMES, String.join(",", each.getQuerySplittingStrategy().getReadDataSources()));
                result.put(each.getName(), exportedDataSources);
            }
        }
        return result;
    }
    
    @Override
    public String getType() {
        return QuerySplittingRule.class.getSimpleName();
    }
}
