/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SaphanaEnvironmentPropertiesTest
{
    private static final String TEST_HOST = "localhost";
    private static final String TEST_PORT = "39013";
    private static final String TEST_SECRET_NAME = "saphana-secret";
    private static final String TEST_JDBC_PARAMS = "encrypt=true";
    private static final String EXPECTED_CONNECTION_PREFIX = "saphana://jdbc:sap://";

    private final SaphanaEnvironmentProperties saphanaEnvironmentProperties = new SaphanaEnvironmentProperties();
    private Map<String, String> connectionProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, TEST_HOST);
        connectionProperties.put(PORT, TEST_PORT);
    }

    @Test
    public void testGetConnectionStringPrefix()
    {
        String prefix = saphanaEnvironmentProperties.getConnectionStringPrefix(connectionProperties);

        assertEquals(EXPECTED_CONNECTION_PREFIX, prefix);
    }

    @Test
    public void testGetDatabase()
    {
        String database = saphanaEnvironmentProperties.getDatabase(connectionProperties);

        assertEquals("/", database);
    }

    @Test
    public void testConnectionPropertiesToEnvironment_withAllProperties()
    {
        connectionProperties.put(JDBC_PARAMS, TEST_JDBC_PARAMS);
        connectionProperties.put(SECRET_NAME, TEST_SECRET_NAME);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = EXPECTED_CONNECTION_PREFIX + TEST_HOST + ":" + TEST_PORT + "/?" + TEST_JDBC_PARAMS + "&${" + TEST_SECRET_NAME + "}";
        assertTrue(environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesToEnvironment_withoutJdbcParams()
    {
        connectionProperties.put(SECRET_NAME, TEST_SECRET_NAME);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = EXPECTED_CONNECTION_PREFIX + TEST_HOST + ":" + TEST_PORT + "/?${" + TEST_SECRET_NAME + "}";
        assertTrue(environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesToEnvironment_withoutSecretName()
    {
        connectionProperties.put(JDBC_PARAMS, TEST_JDBC_PARAMS);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = EXPECTED_CONNECTION_PREFIX + TEST_HOST + ":" + TEST_PORT + "/?" + TEST_JDBC_PARAMS;
        assertTrue(environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesToEnvironment_minimalProperties()
    {
        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = EXPECTED_CONNECTION_PREFIX + TEST_HOST + ":" + TEST_PORT + "/?";
        assertTrue(environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }
} 