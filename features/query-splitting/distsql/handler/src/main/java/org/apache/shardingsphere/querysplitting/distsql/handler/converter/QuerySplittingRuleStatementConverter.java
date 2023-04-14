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

package org.apache.shardingsphere.querysplitting.distsql.handler.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.DynamicQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.StaticQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.distsql.parser.segment.QuerySplittingRuleSegment;

import java.util.*;

/**
 * Readwrite splitting rule statement converter.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class QuerySplittingRuleStatementConverter {
    
    /**
     * Convert readwrite splitting rule segments to readwrite splitting rule configuration.
     *
     * @param ruleSegments readwrite splitting rule segments
     * @return readwrite splitting rule configuration
     */
    public static QuerySplittingRuleConfiguration convert(final Collection<QuerySplittingRuleSegment> ruleSegments) {
        Collection<QuerySplittingDataSourceRuleConfiguration> dataSources = new LinkedList<>();
        Map<String, AlgorithmConfiguration> loadBalancers = new HashMap<>(ruleSegments.size(), 1);
        for (QuerySplittingRuleSegment each : ruleSegments) {
            if (null == each.getLoadBalancer()) {
                dataSources.add(createDataSourceRuleConfiguration(each, null, each.isAutoAware()));
            } else {
                String loadBalancerName = getLoadBalancerName(each.getName(), each.getLoadBalancer());
                loadBalancers.put(loadBalancerName, createLoadBalancer(each));
                dataSources.add(createDataSourceRuleConfiguration(each, loadBalancerName, each.isAutoAware()));
            }
        }
        return new QuerySplittingRuleConfiguration(dataSources, loadBalancers);
    }
    
    private static QuerySplittingDataSourceRuleConfiguration createDataSourceRuleConfiguration(final QuerySplittingRuleSegment segment,
                                                                                               final String loadBalancerName, final boolean isAutoAware) {
        return isAutoAware ? new QuerySplittingDataSourceRuleConfiguration(segment.getName(), null,
                new DynamicQuerySplittingStrategyConfiguration(segment.getAutoAwareResource(), segment.getWriteDataSourceQueryEnabled()), loadBalancerName)
                : new QuerySplittingDataSourceRuleConfiguration(segment.getName(),
                        new StaticQuerySplittingStrategyConfiguration(segment.getWriteDataSource(), new ArrayList<>(segment.getReadDataSources())), null, loadBalancerName);
    }
    
    private static AlgorithmConfiguration createLoadBalancer(final QuerySplittingRuleSegment ruleSegment) {
        return new AlgorithmConfiguration(ruleSegment.getLoadBalancer(), ruleSegment.getProps());
    }
    
    private static String getLoadBalancerName(final String ruleName, final String type) {
        return String.format("%s_%s", ruleName, type);
    }
}
