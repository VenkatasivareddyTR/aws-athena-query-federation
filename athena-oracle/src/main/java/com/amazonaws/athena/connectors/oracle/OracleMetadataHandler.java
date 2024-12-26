
/*-
 * #%L
 * athena-oracle
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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleTypes;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.MODULUS_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.NULLIF_FUNCTION_NAME;

/**
 * Handles metadata for ORACLE. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class OracleMetadataHandler
        extends JdbcMetadataHandler
{
    static final String GET_PARTITIONS_QUERY = "Select DISTINCT PARTITION_NAME as \"partition_name\" FROM USER_TAB_PARTITIONS where table_name= ?";
    static final String BLOCK_PARTITION_COLUMN_NAME = "PARTITION_NAME".toLowerCase();
    static final String ALL_PARTITIONS = "0";
    static final String PARTITION_COLUMN_NAME = "PARTITION_NAME".toLowerCase();
    static final String CASING_MODE = "casing_mode";
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String ORACLE_QUOTE_CHARACTER = "\"";

    static final String LIST_PAGINATED_TABLES_QUERY = "SELECT TABLE_NAME as \"TABLE_NAME\", OWNER as \"TABLE_SCHEM\" FROM all_tables WHERE owner = ? ORDER BY TABLE_NAME OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link OracleMuxCompositeHandler} instead.
     */
    public OracleMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(OracleConstants.ORACLE_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public OracleMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new OracleJdbcConnectionFactory(databaseConnectionConfig, new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS, OracleConstants.ORACLE_DEFAULT_PORT)), configOptions);
    }

    public OracleMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
    }

    @VisibleForTesting
    protected OracleMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     *
     * If it is a table with no partition, then data will be fetched with single split.
     * If it is a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
     * for parallel processing.
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     */
    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.debug("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), transformString(getTableLayoutRequest.getTableName().getSchemaName(), true),
                transformString(getTableLayoutRequest.getTableName().getTableName(), true));
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
          List<String> parameters = Arrays.asList(transformString(getTableLayoutRequest.getTableName().getTableName(), true));
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
                ResultSet resultSet = preparedStatement.executeQuery()) {
                // Return a single partition if no partitions defined
                if (!resultSet.next()) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        LOGGER.debug("Parameters: " + BLOCK_PARTITION_COLUMN_NAME + " " + rowNum + " " + ALL_PARTITIONS);
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        LOGGER.info("Adding partition {}", ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                else {
                    do {
                        final String partitionName = resultSet.getString(PARTITION_COLUMN_NAME);

                        // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                        // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                        blockWriter.writeRows((Block block, int rowNum) -> {
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                            LOGGER.debug("Adding partition {}", partitionName);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    while (resultSet.next() && queryStatusChecker.isQueryRunning());
                }
            }
        }
    }

    /**
     *
     * @param blockAllocator
     * @param getSplitsRequest
     * @return
     */
    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.debug("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }

        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);

            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

            LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition + 1));
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    @VisibleForTesting
    protected List<TableName> getPaginatedTables(Connection connection, String databaseName, int token, int limit) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setInt(2, token);
        preparedStatement.setInt(3, limit);
        LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    @Override
    protected ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        String token = listTablesRequest.getNextToken();
        int pageSize = listTablesRequest.getPageSize();

        int t = token != null ? Integer.parseInt(token) : 0;

        LOGGER.info("Starting pagination at {} with page size {}", token, pageSize);
        List<TableName> paginatedTables = getPaginatedTables(connection, listTablesRequest.getSchemaName(), t, pageSize);
        LOGGER.info("{} tables returned. Next token is {}", paginatedTables.size(), t + pageSize);
        return new ListTablesResponse(listTablesRequest.getCatalogName(), paginatedTables, Integer.toString(t + pageSize));
    }

    /**
     * Overridden this method to describe the types of capabilities supported by a data source
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details about the catalog being used.
     * @return A GetDataSourceCapabilitiesResponse object which returns a map of supported capabilities
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        Set<StandardFunctions> unsupportedFunctions = ImmutableSet.of(NULLIF_FUNCTION_NAME, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, MODULUS_FUNCTION_NAME);
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .filter(values -> !unsupportedFunctions.contains(values))
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));
        
        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
          throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = new TableName(transformString(getTableRequest.getTableName().getSchemaName(), false), transformString(getTableRequest.getTableName().getTableName(), false));
            return new GetTableResponse(getTableRequest.getCatalogName(), tableName, getSchema(connection, tableName, partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
        }
    }

    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    protected String escapeNamePattern(final String name, final String escape)
    {
        if ((name == null) || (escape == null)) {
            return name;
        }
        Preconditions.checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        Preconditions.checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        String escapedName = name.replace(escape, escape + escape);
        escapedName = escapedName.replace("_", escape + "_");
        escapedName = escapedName.replace("%", escape + "%");
        return escapedName;
    }

    /**
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            while (resultSet.next()) {
                ArrowType arrowColumnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);

                String columnName = resultSet.getString(COLUMN_NAME);
                int jdbcColumnType = resultSet.getInt("DATA_TYPE");
                int scale = resultSet.getInt("COLUMN_SIZE");

                LOGGER.debug("columnName: {}", columnName);
                LOGGER.debug("arrowColumnType: {}", arrowColumnType);
                LOGGER.debug("jdbcColumnType: {}", jdbcColumnType);

                /**
                 * below data type conversion doing since a framework not giving appropriate
                 * data types for oracle data types.
                 */

                /** Handling TIMESTAMP, DATE, 0 Precision **/
                if (arrowColumnType != null && arrowColumnType.getTypeID().equals(ArrowType.ArrowTypeID.Decimal)) {
                    String[] data = arrowColumnType.toString().split(",");
                    if (scale == 0 || Integer.parseInt(data[1].trim()) < 0) {
                        arrowColumnType = Types.MinorType.BIGINT.getType();
                    }
                }

                /**
                 * Converting an Oracle date data type into DATEDAY MinorType
                 */
                if (jdbcColumnType == java.sql.Types.TIMESTAMP && scale == 7) {
                    arrowColumnType = Types.MinorType.DATEDAY.getType();
                }

                /**
                 * Converting an Oracle TIMESTAMP_WITH_TZ & TIMESTAMP_WITH_LOCAL_TZ data type into DATEMILLI MinorType
                 */
                if (jdbcColumnType == OracleTypes.TIMESTAMPLTZ || jdbcColumnType == OracleTypes.TIMESTAMPTZ) {
                    arrowColumnType = Types.MinorType.DATEMILLI.getType();
                }

                if (arrowColumnType != null && !SupportedTypes.isSupported(arrowColumnType)) {
                    LOGGER.warn("getSchema: Unable to map type JDBC type [{}] for column[{}] to a supported type, attempted {}", jdbcColumnType, columnName, arrowColumnType);
                    arrowColumnType = Types.MinorType.VARCHAR.getType();
                }

                if (arrowColumnType == null) {
                    LOGGER.warn("getSchema: column[{}]  type is null setting it to varchar | JDBC Type is [{}]", columnName, jdbcColumnType);
                    arrowColumnType = Types.MinorType.VARCHAR.getType();
                }
                schemaBuilder.addField(FieldBuilder.newBuilder(columnName, arrowColumnType).build());
            }

            partitionSchema.getFields().forEach(schemaBuilder::addField);
            LOGGER.debug("Oracle Table Schema" + schemaBuilder.toString());
            return schemaBuilder.build();
        }
    }

    /**
     * Always adds double quotes around the string
     * If the lambda uses a glue connection, return the string as is (lowercased by the trino engine)
     * Otherwise uppercase it (the default of oracle)
     * @param str
     * @param quote
     * @return
     */
    private String transformString(String str, boolean quote)
    {
        boolean isGlueConnection = StringUtils.isNotBlank(configOptions.get(DEFAULT_GLUE_CONNECTION));
        boolean uppercase = configOptions.getOrDefault(CASING_MODE, isGlueConnection ? "lower" : "upper").toLowerCase().equals("upper");
        if (uppercase) {
            str = str.toUpperCase();
        }
        if (quote && !str.contains(ORACLE_QUOTE_CHARACTER)) {
            str = ORACLE_QUOTE_CHARACTER + str + ORACLE_QUOTE_CHARACTER;
        }
        return str;
    }
}
