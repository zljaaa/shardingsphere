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

package org.apache.shardingsphere.querysplitting.yaml.swapper;

import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.yaml.config.swapper.algorithm.YamlAlgorithmConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.rule.YamlRuleConfigurationSwapper;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.constant.QuerySplittingOrder;
import org.apache.shardingsphere.querysplitting.yaml.config.YamlQuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.yaml.config.rule.YamlQuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.yaml.swapper.strategy.YamlDynamicQuerySplittingStrategyConfigurationSwapper;
import org.apache.shardingsphere.querysplitting.yaml.swapper.strategy.YamlStaticQuerySplittingStrategyConfigurationSwapper;

import javax.management.Query;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * YAML readwrite-splitting rule configuration swapper.
 */
public final class YamlQuerySplittingRuleConfigurationSwapper implements YamlRuleConfigurationSwapper<YamlQuerySplittingRuleConfiguration, QuerySplittingRuleConfiguration> {
    
    private final YamlStaticQuerySplittingStrategyConfigurationSwapper staticConfigSwapper = new YamlStaticQuerySplittingStrategyConfigurationSwapper();
    
    private final YamlDynamicQuerySplittingStrategyConfigurationSwapper dynamicConfigSwapper = new YamlDynamicQuerySplittingStrategyConfigurationSwapper();
    
    private final YamlAlgorithmConfigurationSwapper algorithmSwapper = new YamlAlgorithmConfigurationSwapper();
    
    @Override
    public YamlQuerySplittingRuleConfiguration swapToYamlConfiguration(final QuerySplittingRuleConfiguration data) {
        YamlQuerySplittingRuleConfiguration result = new YamlQuerySplittingRuleConfiguration();
        result.setDataSources(data.getDataSources().stream().collect(
                Collectors.toMap(QuerySplittingDataSourceRuleConfiguration::getName, this::swapToYamlConfiguration, (oldValue, currentValue) -> oldValue, LinkedHashMap::new)));
        if (null != data.getLoadBalancers()) {
            data.getLoadBalancers().forEach((key, value) -> result.getLoadBalancers().put(key, algorithmSwapper.swapToYamlConfiguration(value)));
        }
        return result;
    }
    
    private YamlQuerySplittingDataSourceRuleConfiguration swapToYamlConfiguration(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig) {
        YamlQuerySplittingDataSourceRuleConfiguration result = new YamlQuerySplittingDataSourceRuleConfiguration();
        if (null != dataSourceRuleConfig.getStaticStrategy()) {
            result.setStaticStrategy(staticConfigSwapper.swapToYamlConfiguration(dataSourceRuleConfig.getStaticStrategy()));
        }
        if (null != dataSourceRuleConfig.getDynamicStrategy()) {
            result.setDynamicStrategy(dynamicConfigSwapper.swapToYamlConfiguration(dataSourceRuleConfig.getDynamicStrategy()));
        }
        result.setLoadBalancerName(dataSourceRuleConfig.getLoadBalancerName());
        return result;
    }
    
    @Override
    public QuerySplittingRuleConfiguration swapToObject(final YamlQuerySplittingRuleConfiguration yamlConfig) {
        Collection<QuerySplittingDataSourceRuleConfiguration> dataSources = new LinkedList<>();
        for (Entry<String, YamlQuerySplittingDataSourceRuleConfiguration> entry : yamlConfig.getDataSources().entrySet()) {
            dataSources.add(swapToObject(entry.getKey(), entry.getValue()));
        }
        Map<String, AlgorithmConfiguration> loadBalancerMap = new LinkedHashMap<>(yamlConfig.getLoadBalancers().entrySet().size(), 1);
        if (null != yamlConfig.getLoadBalancers()) {
            yamlConfig.getLoadBalancers().forEach((key, value) -> loadBalancerMap.put(key, algorithmSwapper.swapToObject(value)));
        }
        return new QuerySplittingRuleConfiguration(dataSources, loadBalancerMap);
    }
    
    private QuerySplittingDataSourceRuleConfiguration swapToObject(final String name, final YamlQuerySplittingDataSourceRuleConfiguration yamlDataSourceRuleConfig) {
        return new QuerySplittingDataSourceRuleConfiguration(name, staticConfigSwapper.swapToObject(yamlDataSourceRuleConfig.getStaticStrategy()),
                dynamicConfigSwapper.swapToObject(yamlDataSourceRuleConfig.getDynamicStrategy()), yamlDataSourceRuleConfig.getLoadBalancerName());
    }
    
    @Override
    public Class<QuerySplittingRuleConfiguration> getTypeClass() {
        return QuerySplittingRuleConfiguration.class;
    }
    
    @Override
    public String getRuleTagName() {
        return "QUERY_SPLITTING";
    }
    
    @Override
    public int getOrder() {
        return QuerySplittingOrder.ORDER;
    }
}
