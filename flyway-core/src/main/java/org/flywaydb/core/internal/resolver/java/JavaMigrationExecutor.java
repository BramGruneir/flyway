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
package org.flywaydb.core.internal.resolver.java;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.executor.Context;
import org.flywaydb.core.api.executor.MigrationExecutor;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Adapter for executing migrations implementing JavaMigration.
 */
public class JavaMigrationExecutor implements MigrationExecutor {
    private static final Log LOG = LogFactory.getLog(JavaMigrationExecutor.class);
    /**
     * The JavaMigration to execute.
     */
    private final JavaMigration javaMigration;

    /**
     * Creates a new JavaMigrationExecutor.
     *
     * @param javaMigration The JavaMigration to execute.
     */
    JavaMigrationExecutor(JavaMigration javaMigration) {
        this.javaMigration = javaMigration;
    }

    @Override
    public void execute(final Context context) throws SQLException {
        int retryCount = 0;
        while (true) {
            try {
                LOG.debug("Java retrycount:" + retryCount);
                javaMigration.migrate(new org.flywaydb.core.api.migration.Context() {
                    @Override
                    public Configuration getConfiguration() {
                        return context.getConfiguration();
                    }

                    @Override
                    public Connection getConnection() {
                        return context.getConnection();
                    }
                });
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