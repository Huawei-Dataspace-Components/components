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

package com.huawei.cloud.store.gaussdb.transferprocess;

import org.eclipse.edc.azure.testfixtures.GaussDbTestExtension;
import org.eclipse.edc.azure.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.store.sql.transferprocess.store.SqlTransferProcessStore;
import org.eclipse.edc.connector.store.sql.transferprocess.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.connector.transfer.spi.store.TransferProcessStore;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.sql.lease.testfixtures.LeaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;

import static org.eclipse.edc.azure.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbTransferProcessStoreTest {

    protected static final String CONNECTOR_NAME = "test-connector";
    private static final PostgresDialectStatements SQL_STATEMENTS = new PostgresDialectStatements();
    protected final Clock clock = Clock.systemUTC();
    private LeaseUtil leaseUtil;
    private TransferProcessStore transferProcessStore;

    @BeforeEach
    void setUp(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper helper, QueryExecutor queryExecutor) {
        var clock = Clock.systemUTC();
        var typeManager = new TypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        transferProcessStore = new SqlTransferProcessStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), typeManager.getMapper(), SQL_STATEMENTS, CONNECTOR_NAME, clock, queryExecutor);

        leaseUtil = new LeaseUtil(extension.getTransactionContext(), extension::newConnection, SQL_STATEMENTS, clock);

        helper.truncateTable(SQL_STATEMENTS.getTransferProcessTableName());
        helper.truncateTable(SQL_STATEMENTS.getDataRequestTable());
        helper.truncateTable(SQL_STATEMENTS.getLeaseTableName());
    }

    @Test
    void foo() {

    }

    protected TransferProcessStore getTransferProcessStore() {
        return transferProcessStore;
    }

    protected void leaseEntity(String negotiationId, String owner) {
        leaseEntity(negotiationId, owner, Duration.ofSeconds(60));
    }

    protected void leaseEntity(String negotiationId, String owner, Duration duration) {
        leaseUtil.leaseEntity(negotiationId, owner, duration);
    }

    protected boolean isLeasedBy(String negotiationId, String owner) {
        return leaseUtil.isLeased(negotiationId, owner);
    }

    @BeforeAll
    static void createDatabase(GaussDbTestExtension.SqlHelper runner) throws IOException {
        var schema = Files.readString(Paths.get("docs/schema.sql"));
        runner.executeStatement(schema);
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getTransferProcessTableName());
        runner.dropTable(SQL_STATEMENTS.getDataRequestTable());
        runner.dropTable(SQL_STATEMENTS.getLeaseTableName());
    }
}