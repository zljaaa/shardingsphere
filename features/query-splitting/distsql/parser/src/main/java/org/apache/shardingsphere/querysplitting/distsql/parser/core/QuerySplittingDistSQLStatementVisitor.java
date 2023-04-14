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

package org.apache.shardingsphere.querysplitting.distsql.parser.core;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.shardingsphere.distsql.parser.autogen.QuerySplittingDistSQLStatementBaseVisitor;
import org.apache.shardingsphere.distsql.parser.autogen.QuerySplittingDistSQLStatementParser;
import org.apache.shardingsphere.distsql.parser.segment.AlgorithmSegment;

import org.apache.shardingsphere.querysplitting.distsql.parser.segment.QuerySplittingRuleSegment;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.*;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.hint.ClearQuerySplittingHintStatement;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.hint.SetQuerySplittingHintStatement;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.hint.ShowQuerySplittingHintStatusStatement;
import org.apache.shardingsphere.querysplitting.distsql.parser.statement.status.AlterQuerySplittingStorageUnitStatusStatement;
import org.apache.shardingsphere.sql.parser.api.visitor.ASTNode;
import org.apache.shardingsphere.sql.parser.api.visitor.SQLVisitor;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.DatabaseSegment;
import org.apache.shardingsphere.sql.parser.sql.common.value.identifier.IdentifierValue;

import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * SQL statement visitor for readwrite-splitting DistSQL.
 */
public final class QuerySplittingDistSQLStatementVisitor extends QuerySplittingDistSQLStatementBaseVisitor<ASTNode> implements SQLVisitor {
    
    @Override
    public ASTNode visitCreateQuerySplittingRule(final QuerySplittingDistSQLStatementParser.CreateQuerySplittingRuleContext ctx) {
        return new CreateQuerySplittingRuleStatement(null != ctx.ifNotExists(),
                ctx.querySplittingRuleDefinition().stream().map(each -> (QuerySplittingRuleSegment) visit(each)).collect(Collectors.toList()));
    }
    
    @Override
    public ASTNode visitAlterQuerySplittingRule(final QuerySplittingDistSQLStatementParser.AlterQuerySplittingRuleContext ctx) {
        return new AlterQuerySplittingRuleStatement(ctx.querySplittingRuleDefinition().stream().map(each -> (QuerySplittingRuleSegment) visit(each)).collect(Collectors.toList()));
    }
    
    @Override
    public ASTNode visitDropQuerySplittingRule(final QuerySplittingDistSQLStatementParser.DropQuerySplittingRuleContext ctx) {
        return new DropQuerySplittingRuleStatement(ctx.ifExists() != null, ctx.ruleName().stream().map(this::getIdentifierValue).collect(Collectors.toList()));
    }
    
    @Override
    public ASTNode visitAlterQuerySplittingStorageUnitStatus(final QuerySplittingDistSQLStatementParser.AlterQuerySplittingStorageUnitStatusContext ctx) {
        DatabaseSegment databaseSegment = Objects.nonNull(ctx.databaseName()) ? (DatabaseSegment) visit(ctx.databaseName()) : null;
        String groupName = getIdentifierValue(ctx.groupName());
        String status = null == ctx.ENABLE() ? ctx.DISABLE().getText().toUpperCase() : ctx.ENABLE().getText().toUpperCase();
        String storageUnitName = getIdentifierValue(ctx.storageUnitName());
        return new AlterQuerySplittingStorageUnitStatusStatement(databaseSegment, groupName, storageUnitName, status);
    }
    
    @Override
    public ASTNode visitShowQuerySplittingRules(final QuerySplittingDistSQLStatementParser.ShowQuerySplittingRulesContext ctx) {
        return new ShowQuerySplittingRulesStatement(Objects.nonNull(ctx.databaseName()) ? (DatabaseSegment) visit(ctx.databaseName()) : null);
    }
    
    @Override
    public ASTNode visitQuerySplittingRuleDefinition(final QuerySplittingDistSQLStatementParser.QuerySplittingRuleDefinitionContext ctx) {
        Properties props = new Properties();
        String algorithmTypeName = null;
        if (null != ctx.algorithmDefinition()) {
            algorithmTypeName = getIdentifierValue(ctx.algorithmDefinition().algorithmTypeName());
            props = getProperties(ctx.algorithmDefinition().propertiesDefinition());
        }
        if (null == ctx.staticQuerySplittingRuleDefinition()) {
            return new QuerySplittingRuleSegment(getIdentifierValue(ctx.ruleName()), getIdentifierValue(ctx.dynamicQuerySplittingRuleDefinition().resourceName()),
                    getIdentifierValue(ctx.dynamicQuerySplittingRuleDefinition().writeDataSourceQueryEnabled()), algorithmTypeName, props);
        }
        QuerySplittingDistSQLStatementParser.StaticQuerySplittingRuleDefinitionContext staticRuleDefinitionCtx = ctx.staticQuerySplittingRuleDefinition();
        return new QuerySplittingRuleSegment(getIdentifierValue(ctx.ruleName()),
                getIdentifierValue(staticRuleDefinitionCtx.writeStorageUnitName()),
                staticRuleDefinitionCtx.readStorageUnitsNames().storageUnitName().stream().map(this::getIdentifierValue).collect(Collectors.toList()),
                algorithmTypeName, props);
    }
    
    @Override
    public ASTNode visitDatabaseName(final QuerySplittingDistSQLStatementParser.DatabaseNameContext ctx) {
        return new DatabaseSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), new IdentifierValue(ctx.getText()));
    }
    
    @Override
    public ASTNode visitAlgorithmDefinition(final QuerySplittingDistSQLStatementParser.AlgorithmDefinitionContext ctx) {
        return new AlgorithmSegment(getIdentifierValue(ctx.algorithmTypeName()), getProperties(ctx.propertiesDefinition()));
    }
    
    @Override
    public ASTNode visitShowStatusFromQuerySplittingRules(final QuerySplittingDistSQLStatementParser.ShowStatusFromQuerySplittingRulesContext ctx) {
        DatabaseSegment databaseSegment = Objects.nonNull(ctx.databaseName()) ? (DatabaseSegment) visit(ctx.databaseName()) : null;
        String groupName = getIdentifierValue(ctx.groupName());
        return new ShowStatusFromQuerySplittingRulesStatement(databaseSegment, groupName);
    }
    
    @Override
    public ASTNode visitSetQuerySplittingHintSource(final QuerySplittingDistSQLStatementParser.SetQuerySplittingHintSourceContext ctx) {
        return new SetQuerySplittingHintStatement(getIdentifierValue(ctx.sourceValue()));
    }
    
    private String getIdentifierValue(final ParseTree context) {
        return null == context ? null : new IdentifierValue(context.getText()).getValue();
    }
    
    @Override
    public ASTNode visitShowQuerySplittingHintStatus(final QuerySplittingDistSQLStatementParser.ShowQuerySplittingHintStatusContext ctx) {
        return new ShowQuerySplittingHintStatusStatement();
    }
    
    @Override
    public ASTNode visitClearQuerySplittingHint(final QuerySplittingDistSQLStatementParser.ClearQuerySplittingHintContext ctx) {
        return new ClearQuerySplittingHintStatement();
    }
    
    @Override
    public ASTNode visitCountQuerySplittingRule(final QuerySplittingDistSQLStatementParser.CountQuerySplittingRuleContext ctx) {
        return new CountQuerySplittingRuleStatement(Objects.nonNull(ctx.databaseName()) ? (DatabaseSegment) visit(ctx.databaseName()) : null);
    }
    
    private Properties getProperties(final QuerySplittingDistSQLStatementParser.PropertiesDefinitionContext ctx) {
        Properties result = new Properties();
        if (null == ctx || null == ctx.properties()) {
            return result;
        }
        for (QuerySplittingDistSQLStatementParser.PropertyContext each : ctx.properties().property()) {
            result.setProperty(IdentifierValue.getQuotedContent(each.key.getText()), IdentifierValue.getQuotedContent(each.value.getText()));
        }
        return result;
    }
}
