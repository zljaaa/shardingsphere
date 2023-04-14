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

import com.google.common.base.Joiner;
import org.apache.shardingsphere.distsql.handler.resultset.DatabaseDistSQLResultSet;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.rule.identifier.type.exportable.constant.ExportableConstants;
import org.apache.shardingsphere.infra.rule.identifier.type.exportable.constant.ExportableItemConstants;
import org.apache.shardingsphere.infra.util.props.PropertiesConverter;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.ShowQuerySplittingRulesStatement;
import org.apache.shardingsphere.querysplitting.rule.QuerySplittingRule;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;

import java.util.*;

/**
 * Result set for show readwrite splitting rule.
 */
public final class QuerySplittingRuleResultSet implements DatabaseDistSQLResultSet {
    
    private Iterator<Collection<Object>> data = Collections.emptyIterator();
    
    private Map<String, Map<String, String>> exportableAutoAwareDataSource = Collections.emptyMap();
    
    private Map<String, Map<String, String>> exportableDataSourceMap = Collections.emptyMap();
    
    @Override
    public void init(final ShardingSphereDatabase database, final SQLStatement sqlStatement) {
        Optional<QuerySplittingRule> rule = database.getRuleMetaData().findSingleRule(QuerySplittingRule.class);
        rule.ifPresent(optional -> {
            buildExportableMap(optional);
            data = buildData(optional).iterator();
        });
    }
    
    @SuppressWarnings("unchecked")
    private void buildExportableMap(final QuerySplittingRule rule) {
        Map<String, Object> exportedData = rule.getExportData();
        exportableAutoAwareDataSource = (Map<String, Map<String, String>>) exportedData.get(ExportableConstants.EXPORT_DYNAMIC_READWRITE_SPLITTING_RULE);
        exportableDataSourceMap = (Map<String, Map<String, String>>) exportedData.get(ExportableConstants.EXPORT_STATIC_READWRITE_SPLITTING_RULE);
    }
    
    private Collection<Collection<Object>> buildData(final QuerySplittingRule rule) {
        Collection<Collection<Object>> result = new LinkedList<>();
        ((QuerySplittingRuleConfiguration) rule.getConfiguration()).getDataSources().forEach(each -> {
            Collection<Object> dataItem = buildDataItem(each, getLoadBalancers((QuerySplittingRuleConfiguration) rule.getConfiguration()));
            result.add(dataItem);
        });
        return result;
    }
    
    private Collection<Object> buildDataItem(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig, final Map<String, AlgorithmConfiguration> loadBalancers) {
        String name = dataSourceRuleConfig.getName();
        Map<String, String> exportDataSources = null == dataSourceRuleConfig.getDynamicStrategy() ? exportableDataSourceMap.get(name) : exportableAutoAwareDataSource.get(name);
        Optional<AlgorithmConfiguration> loadBalancer = Optional.ofNullable(loadBalancers.get(dataSourceRuleConfig.getLoadBalancerName()));
        return Arrays.asList(name,
                getAutoAwareDataSourceName(dataSourceRuleConfig),
                getWriteDataSourceQueryEnabled(dataSourceRuleConfig),
                getWriteDataSourceName(dataSourceRuleConfig, exportDataSources),
                getReadDataSourceNames(dataSourceRuleConfig, exportDataSources),
                loadBalancer.map(AlgorithmConfiguration::getType).orElse(""),
                loadBalancer.map(each -> PropertiesConverter.convert(each.getProps())).orElse(""));
    }
    
    private Map<String, AlgorithmConfiguration> getLoadBalancers(final QuerySplittingRuleConfiguration ruleConfig) {
        Map<String, AlgorithmConfiguration> loadBalancers = ruleConfig.getLoadBalancers();
        return null == loadBalancers ? Collections.emptyMap() : loadBalancers;
    }
    
    private String getAutoAwareDataSourceName(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig) {
        return null == dataSourceRuleConfig.getDynamicStrategy() ? "" : dataSourceRuleConfig.getDynamicStrategy().getAutoAwareDataSourceName();
    }
    
    private String getWriteDataSourceQueryEnabled(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig) {
        return null == dataSourceRuleConfig.getDynamicStrategy() ? "" : dataSourceRuleConfig.getDynamicStrategy().getWriteDataSourceQueryEnabled();
    }
    
    private String getWriteDataSourceName(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig, final Map<String, String> exportDataSources) {
        return null == exportDataSources ? dataSourceRuleConfig.getStaticStrategy().getMysqlDataSourceName() : exportDataSources.get(ExportableItemConstants.PRIMARY_DATA_SOURCE_NAME);
    }
    
    private String getReadDataSourceNames(final QuerySplittingDataSourceRuleConfiguration dataSourceRuleConfig, final Map<String, String> exportDataSources) {
        return null == exportDataSources
                ? Joiner.on(",").join(dataSourceRuleConfig.getStaticStrategy().getSnowDataSourceNames())
                : exportDataSources.get(ExportableItemConstants.REPLICA_DATA_SOURCE_NAMES);
    }
    
    @Override
    public Collection<String> getColumnNames() {
        return Arrays.asList("name", "auto_aware_data_source_name", "write_data_source_query_enabled",
                "write_storage_unit_name", "read_storage_unit_names", "load_balancer_type", "load_balancer_props");
    }
    
    @Override
    public boolean next() {
        return data.hasNext();
    }
    
    @Override
    public Collection<Object> getRowData() {
        return data.next();
    }
    
    @Override
    public String getType() {
        return ShowQuerySplittingRulesStatement.class.getName();
    }
}
