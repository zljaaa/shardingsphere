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

package org.apache.shardingsphere.querysplitting.strategy;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.identifier.type.DynamicDataSourceContainedRule;
import org.apache.shardingsphere.querysplitting.api.rule.QuerySplittingDataSourceRuleConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.DynamicQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.api.strategy.StaticQuerySplittingStrategyConfiguration;
import org.apache.shardingsphere.querysplitting.strategy.type.DynamicQuerySplittingStrategy;
import org.apache.shardingsphere.querysplitting.strategy.type.StaticQuerySplittingStrategy;

import java.util.Collection;
import java.util.Optional;

/**
 * Readwrite splitting strategy factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class QuerySplittingStrategyFactory {
    
    /**
     * Create new instance of readwrite splitting strategy.
     * <p>
     -Dmaven.javadoc.skip=true -Drat.skip=true     * @param readwriteSplittingConfig readwrite-splitting rule config
     * @param builtRules built rules
     * @return created instance
     */
    public static QuerySplittingStrategy newInstance(final QuerySplittingDataSourceRuleConfiguration querySplittingConfig, final Collection<ShardingSphereRule> builtRules) {
        return null == querySplittingConfig.getStaticStrategy()
                ? createDynamicQuerySplittingStrategy(querySplittingConfig.getDynamicStrategy(), builtRules)
                : createStaticQuerySplittingStrategy(querySplittingConfig.getStaticStrategy());
    }
    
    private static StaticQuerySplittingStrategy createStaticQuerySplittingStrategy(final StaticQuerySplittingStrategyConfiguration staticConfig) {
        return new StaticQuerySplittingStrategy(staticConfig.getMysqlDataSourceName(), staticConfig.getSnowDataSourceNames());
    }
    
    private static DynamicQuerySplittingStrategy createDynamicQuerySplittingStrategy(final DynamicQuerySplittingStrategyConfiguration dynamicConfig,
                                                                                     final Collection<ShardingSphereRule> builtRules) {
        Optional<ShardingSphereRule> dynamicDataSourceStrategy = builtRules.stream().filter(each -> each instanceof DynamicDataSourceContainedRule).findFirst();
        boolean allowWriteDataSourceQuery = Strings.isNullOrEmpty(dynamicConfig.getWriteDataSourceQueryEnabled()) ? Boolean.TRUE : Boolean.parseBoolean(dynamicConfig.getWriteDataSourceQueryEnabled());
        return new DynamicQuerySplittingStrategy(dynamicConfig.getAutoAwareDataSourceName(), allowWriteDataSourceQuery, (DynamicDataSourceContainedRule) dynamicDataSourceStrategy.get());
    }
}
