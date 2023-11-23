package com.huawei.cloud.store.gaussdb.policymonitor;

import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorStore;
import org.eclipse.edc.connector.policy.monitor.store.sql.SqlPolicyMonitorStore;
import org.eclipse.edc.connector.policy.monitor.store.sql.schema.PostgresPolicyMonitorStatements;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
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

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbPolicyMonitorProcessStoreTest {

    protected static final String CONNECTOR_NAME = "test-connector";
    private static final PostgresPolicyMonitorStatements SQL_STATEMENTS = new PostgresPolicyMonitorStatements();
    protected final Clock clock = Clock.systemUTC();
    private LeaseUtil leaseUtil;
    private SqlPolicyMonitorStore transferProcessStore;

    @BeforeEach
    void setUp(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper helper, QueryExecutor queryExecutor) {
        var clock = Clock.systemUTC();
        var typeManager = new TypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        transferProcessStore = new SqlPolicyMonitorStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), SQL_STATEMENTS, typeManager.getMapper(), clock, queryExecutor, CONNECTOR_NAME);

        leaseUtil = new LeaseUtil(extension.getTransactionContext(), extension::newConnection, SQL_STATEMENTS, clock);

        helper.truncateTable(SQL_STATEMENTS.getPolicyMonitorTable());
        helper.truncateTable(SQL_STATEMENTS.getLeaseTableName());
    }

    @Test
    void foo() {
    }

    protected PolicyMonitorStore getTransferProcessStore() {
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

    private void delayByTenMillis(TransferProcess t) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {
            // noop
        }
        t.updateStateTimestamp();
    }

    @BeforeAll
    static void createDatabase(GaussDbTestExtension.SqlHelper runner) throws IOException {
        var schema = Files.readString(Paths.get("docs/schema.sql"));
        runner.executeStatement(schema);
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getPolicyMonitorTable());
        runner.dropTable(SQL_STATEMENTS.getLeaseTableName());
    }
}