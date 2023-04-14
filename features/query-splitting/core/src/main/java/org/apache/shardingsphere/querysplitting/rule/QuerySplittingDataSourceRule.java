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

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.spi.ReadQueryLoadBalanceAlgorithm;
import org.apache.shardingsphere.querysplitting.strategy.QuerySplittingStrategy;
import org.apache.shardingsphere.querysplitting.strategy.QuerySplittingStrategyFactory;
import org.apache.shardingsphere.querysplitting.strategy.type.DynamicQuerySplittingStrategy;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Readwrite-splitting data source rule.
 */
@Getter
public final class QuerySplittingDataSourceRule {
    
    private final String name;
    
    private final ReadQueryLoadBalanceAlgorithm loadBalancer;
    
    private final QuerySplittingStrategy querySplittingStrategy;
    
    @Getter(AccessLevel.NONE)
    private final Collection<String> disabledDataSourceNames = new HashSet<>();
    
    public QuerySplittingDataSourceRule(final QuerySplittingDataSourceRuleConfiguration config, final ReadQueryLoadBalanceAlgorithm loadBalancer,
                                        final Collection<ShardingSphereRule> builtRules) {
        name = config.getName();
        this.loadBalancer = loadBalancer;
        querySplittingStrategy = QuerySplittingStrategyFactory.newInstance(config, builtRules);
    }
    
    /**
     * Get write data source name.
     *
     * @return write data source name
     */
    public String getWriteDataSource() {
        return querySplittingStrategy.getWriteDataSource();
    }
    
    /**
     * Update disabled data source names.
     *
     * @param dataSourceName data source name
     * @param isDisabled is disabled
     */
    public void updateDisabledDataSourceNames(final String dataSourceName, final boolean isDisabled) {
        if (isDisabled) {
            disabledDataSourceNames.add(dataSourceName);
        } else {
            disabledDataSourceNames.remove(dataSourceName);
        }
    }
    
    /**
     * Get enabled replica data sources.
     *
     * @return enabled replica data sources
     */
    public List<String> getEnabledReplicaDataSources() {
        List<String> result = querySplittingStrategy.getReadDataSources();
        if (querySplittingStrategy instanceof DynamicQuerySplittingStrategy) {
            return result;
        }
        if (!disabledDataSourceNames.isEmpty()) {
            result = new LinkedList<>(result);
            result.removeIf(disabledDataSourceNames::contains);
        }
        return result;
    }
}
