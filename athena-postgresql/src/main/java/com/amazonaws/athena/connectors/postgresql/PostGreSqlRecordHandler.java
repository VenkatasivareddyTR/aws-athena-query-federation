/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

public class PostGreSqlRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlRecordHandler.class);

    private static final int FETCH_SIZE = 1000;

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
     */
    public PostGreSqlRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions), configOptions);
    }

    public PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, PostGreSqlMetadataHandler.JDBC_PROPERTIES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT)),
                new PostGreSqlQueryStringBuilder(POSTGRES_QUOTE_CHARACTER, new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    protected PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager,
            AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
            }
            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        return preparedStatement;
    }
}
