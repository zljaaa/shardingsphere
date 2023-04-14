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

package org.apache.shardingsphere.querysplitting.rule.builder;

import org.apache.shardingsphere.infra.instance.InstanceContext;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.database.DatabaseRuleBuilder;
import org.apache.shardingsphere.querysplitting.api.QuerySplittingRuleConfiguration;
import org.apache.shardingsphere.querysplitting.constant.QuerySplittingOrder;
import org.apache.shardingsphere.querysplitting.rule.QuerySplittingRule;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Map;

/**
 * Readwrite-splitting rule builder.
 */
public final class QuerySplittingRuleBuilder implements DatabaseRuleBuilder<QuerySplittingRuleConfiguration> {
    
    @Override
    public QuerySplittingRule build(final QuerySplittingRuleConfiguration config, final String databaseName,
                                    final Map<String, DataSource> dataSources, final Collection<ShardingSphereRule> builtRules, final InstanceContext instanceContext) {
        return new QuerySplittingRule(config, builtRules);
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
