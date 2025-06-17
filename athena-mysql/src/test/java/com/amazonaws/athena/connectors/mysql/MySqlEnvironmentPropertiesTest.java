/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MySqlEnvironmentPropertiesTest {

    private final MySqlEnvironmentProperties environmentProperties = new MySqlEnvironmentProperties();;
    private static final String expectedPrefix = "mysql://jdbc:mysql://";

    @Test
    public void getConnectionStringPrefix_withValidConnectionProperties_returnsCorrectPrefix() {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put("host", "localhost");
        connectionProperties.put("port", "3306");
        connectionProperties.put("database", "testdb");

        String actualPrefix = environmentProperties.getConnectionStringPrefix(connectionProperties);
        Assert.assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void getConnectionStringPrefix_withEmptyConnectionProperties_returnsCorrectPrefix() {
        Map<String, String> connectionProperties = new HashMap<>();
        String actualPrefix = environmentProperties.getConnectionStringPrefix(connectionProperties);
        Assert.assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void getConnectionStringPrefix_withNullConnectionProperties_returnsCorrectPrefix() {
        String actualPrefix = environmentProperties.getConnectionStringPrefix(null);
        Assert.assertEquals(expectedPrefix, actualPrefix);
    }
} 