/*
 * Copyright 2010-2018 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.resolver.jdbc;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.executor.Context;
import org.flywaydb.core.api.executor.MigrationExecutor;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;

import java.sql.SQLException;

/**
 * Adapter for executing migrations implementing JdbcMigration.
 */
public class JdbcMigrationExecutor implements MigrationExecutor {
    private static final Log LOG = LogFactory.getLog(JdbcMigrationExecutor.class);
    /**
     * The JdbcMigration to execute.
     */
    private final JdbcMigration jdbcMigration;

    /**
     * Creates a new JdbcMigrationExecutor.
     *
     * @param jdbcMigration The JdbcMigration to execute.
     */
    JdbcMigrationExecutor(JdbcMigration jdbcMigration) {
        this.jdbcMigration = jdbcMigration;
    }

    @Override
    public void execute(Context context) throws SQLException {
        int retryCount = 0;
        while (true) {
            try {
                LOG.debug("JDBC retrycount:" + retryCount);
                jdbcMigration.migrate(context.getConnection());
                break;
            } catch (SQLException e) {
                if ((e.getSQLState() != "40001") || (retryCount >= 50)) {
                    LOG.info("error: " + e);
                    throw e;
                }
                retryCount++;
                continue;
            } catch (Exception e) {
                throw new FlywayException("Migration failed !", e);
            }
        }
    }

    @Override
    public boolean canExecuteInTransaction() {
        return true;
    }
}