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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SaphanaFederationExpressionParserTest
{
    private final SaphanaFederationExpressionParser parser = new SaphanaFederationExpressionParser("\"");
    private final ArrowType type = new ArrowType.Utf8();
    private static final String testValue1 = "value1";
    private static final String testValue2 = "value2";
    private static final String testValue3 = "value3";

    @Test
    public void testWriteArrayConstructorClauseWithEmptyList()
    {
        String result = parser.writeArrayConstructorClause(type, Collections.emptyList());
        assertEquals("", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithSingleElement()
    {
        String result = parser.writeArrayConstructorClause(type, Collections.singletonList(testValue1));
        assertEquals(testValue1, result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithMultipleElements()
    {
        List<String> arguments = Arrays.asList(testValue1, testValue2, testValue3);
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals("value1, value2, value3", result);
    }

    @Test
    public void testWriteArrayConstructorClauseWithNumericValues()
    {
        ArrowType type = new ArrowType.Int(64, true);
        List<String> arguments = Arrays.asList("100", "200", "300");
        
        String result = parser.writeArrayConstructorClause(type, arguments);
        assertEquals("100, 200, 300", result);
    }
} 