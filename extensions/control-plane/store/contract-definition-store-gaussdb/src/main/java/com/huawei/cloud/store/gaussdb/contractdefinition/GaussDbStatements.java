/*
 *
 *   Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft
 *
 *   See the NOTICE file(s) distributed with this work for additional
 *   information regarding copyright ownership.
 *
 *   This program and the accompanying materials are made available under the
 *   terms of the Apache License, Version 2.0 which is available at
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations
 *   under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 *
 */

package com.huawei.cloud.store.gaussdb.contractdefinition;

import org.eclipse.edc.connector.store.sql.contractdefinition.schema.postgres.ContractDefinitionMapping;
import org.eclipse.edc.connector.store.sql.contractdefinition.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.translation.SqlQueryStatement;

public class GaussDbStatements extends PostgresDialectStatements {

    public static final String ASSETS_SELECTOR_PREFIX = "assetsSelector.";

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {

        if (querySpec.containsAnyLeftOperand(ASSETS_SELECTOR_PREFIX)) {
            var select = "SELECT * FROM %s ".formatted(getContractDefinitionTable());

            var criteria = querySpec.getFilterExpression();

            // contains only criteria that target the assetSelector, i.e. will use JSON-query syntax
            var filteredCriteria = criteria.stream()
                    .filter(c -> c.getOperandLeft().toString().startsWith(ASSETS_SELECTOR_PREFIX))
                    .toList();

            // remove all criteria, that target the assetSelector
            criteria.removeAll(filteredCriteria);

            var stmt = new SqlQueryStatement(select, querySpec, new ContractDefinitionMapping(this));

            // manually construct a SELECT statement using json_array_elements syntax.
            // for reference, check this article: https://stackoverflow.com/a/30691077/7079724
            filteredCriteria.forEach(
                    fc -> {
                        var sanitizedLeftOp = fc.getOperandLeft().toString().replace(ASSETS_SELECTOR_PREFIX, "");
                        stmt.addWhereClause("? IN (SELECT json_array_elements( %s.%s ) ->> ?)".formatted(getContractDefinitionTable(), getAssetsSelectorColumn()), fc.getOperandRight(), sanitizedLeftOp);
                    });
            return stmt;
        }
        return super.createQuery(querySpec);
    }
}
