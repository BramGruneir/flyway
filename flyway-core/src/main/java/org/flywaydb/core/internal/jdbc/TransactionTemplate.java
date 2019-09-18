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
package org.flywaydb.core.internal.jdbc;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.exception.FlywaySqlException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Spring-like template for executing transactions.
 */
public class TransactionTemplate {
    private static final Log LOG = LogFactory.getLog(TransactionTemplate.class);

    /**
     * The connection for the transaction.
     */
    private final Connection connection;

    /**
     * Whether to roll back the transaction when an exception is thrown.
     */
    private final boolean rollbackOnException;

    /**
     * Creates a new transaction template for this connection.
     *
     * @param connection The connection for the transaction.
     */
    public TransactionTemplate(Connection connection) {
        this(connection, true);
    }

    /**
     * Creates a new transaction template for this connection.
     *
     * @param connection          The connection for the transaction.
     * @param rollbackOnException Whether to roll back the transaction when an
     *                            exception is thrown.
     */
    public TransactionTemplate(Connection connection, boolean rollbackOnException) {
        this.connection = connection;
        this.rollbackOnException = rollbackOnException;
    }

    /**
     * Executes this callback within a transaction.
     *
     * @param transactionCallback The callback to execute.
     * @return The result of the transaction code.
     */
    public <T> T execute(Callable<T> transactionCallback) {
        int retryCount = 0;
        while (true) {
            try {
                LOG.debug("Txn-Execute retrycount:" + retryCount);
                return transactionCallback.call();
            } catch (SQLException e) {
                if ((e.getSQLState() != "40001") || (retryCount >= 50)) {
                    LOG.info("error: " + e);
                    throw new FlywayException(e);
                }
            } catch (Exception e) {
                throw new FlywayException(e);
            }
            retryCount++;
        }
    }
}