/*-
 * #%L
 * athena-mysql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandlerFactory;
import com.amazonaws.athena.connectors.oracle.resolver.OracleJDBCCaseResolver;
import org.apache.arrow.util.VisibleForTesting;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

class OracleMetadataHandlerFactory
        implements JdbcMetadataHandlerFactory
{
    @Override
    public String getEngine()
    {
        return OracleConstants.ORACLE_NAME;
    }

    @Override
    public JdbcMetadataHandler createJdbcMetadataHandler(DatabaseConnectionConfig config, java.util.Map<String, String> configOptions)
    {
        return new OracleMetadataHandler(config, configOptions);
    }
}

public class OracleMuxMetadataHandler
        extends MultiplexingJdbcMetadataHandler
{
    public OracleMuxMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(new OracleMetadataHandlerFactory(), configOptions);
    }

    @VisibleForTesting
    protected OracleMuxMetadataHandler(SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory,
                                     Map<String, JdbcMetadataHandler> metadataHandlerMap, DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(secretsManager, athena, jdbcConnectionFactory, metadataHandlerMap, databaseConnectionConfig, configOptions, new OracleJDBCCaseResolver(OracleConstants.ORACLE_NAME));
    }
}
