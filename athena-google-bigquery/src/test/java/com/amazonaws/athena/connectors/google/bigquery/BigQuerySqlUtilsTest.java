/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryTestUtils.makeSchema;
import static org.junit.Assert.assertEquals;

public class BigQuerySqlUtilsTest
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    static final TableName tableName = new TableName(SCHEMA_NAME, TABLE_NAME);
    private static final String TEST_DATE = "2023-01-01";
    private static final String TEST_TIME = "10:30:00";
    private static final String TEST_DATETIME = TEST_DATE + "T" + TEST_TIME;
    private static final String TEST_DATETIME_MICROS = TEST_DATETIME + ".123";
    private static final String TEST_DATETIME_PADDED = TEST_DATE + " " + TEST_TIME + ".000000";
    private static final String INT_COL = "intCol";
    private static final String VALUE_PREFIX = "value";
    private static final double TEST_FLOAT = 123.456;
    static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    static final ArrowType FLOAT_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    static final ArrowType DATE_TYPE = new ArrowType.Date(DateUnit.DAY);
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testSqlWithConstraintsRanges()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).add(new Range(Marker.above(allocator, INT_TYPE, 10),
                Marker.below(allocator, INT_TYPE, 20))).build();

        ValueSet isNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();

        ValueSet isNonNullRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();

        ValueSet stringRangeSet = SortedRangeSet.newBuilder(STRING_TYPE, false).add(new Range(Marker.exactly(allocator, STRING_TYPE, "a_low"),
                Marker.below(allocator, STRING_TYPE, "z_high"))).build();

        ValueSet booleanRangeSet = SortedRangeSet.newBuilder(BOOLEAN_TYPE, false).add(new Range(Marker.exactly(allocator, BOOLEAN_TYPE, true),
                Marker.exactly(allocator, BOOLEAN_TYPE, true))).build();

        ValueSet integerInRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 1000_000), Marker.exactly(allocator, INT_TYPE, 1000_000)))
                .build();

        constraintMap.put("integerRange", rangeSet);
        constraintMap.put("isNullRange", isNullRangeSet);
        constraintMap.put("isNotNullRange", isNonNullRangeSet);
        constraintMap.put("stringRange", stringRangeSet);
        constraintMap.put("booleanRange", booleanRangeSet);
        constraintMap.put("integerInRange", integerInRangeSet);

        final List<QueryParameterValue> expectedParameterValues = ImmutableList.of(QueryParameterValue.int64(10), QueryParameterValue.int64(20),
                QueryParameterValue.string("a_low"), QueryParameterValue.string("z_high"),
                QueryParameterValue.bool(true),
                QueryParameterValue.int64(10), QueryParameterValue.int64(1000000));

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals(expectedParameterValues, parameterValues);
            assertEquals("SELECT `integerRange`,`isNullRange`,`isNotNullRange`,`stringRange`,`booleanRange`,`integerInRange` from `schema`.`table` " +
                    "WHERE ((integerRange IS NULL) OR (`integerRange` > ? AND `integerRange` < ?)) " +
                    "AND (isNullRange IS NULL) AND (isNotNullRange IS NOT NULL) " +
                    "AND ((`stringRange` >= ? AND `stringRange` < ?)) " +
                    "AND (`booleanRange` = ?) " +
                    "AND (`integerInRange` IN (?,?))", sql);
        }
    }

    @Test
    public void testSqlWithOrderByAndLimit()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 0), Marker.below(allocator, INT_TYPE, 100)))
                .build();
        constraintMap.put(INT_COL, rangeSet);

        List<OrderByField> orderByFields = ImmutableList.of(
            new OrderByField(INT_COL, OrderByField.Direction.ASC_NULLS_FIRST),
            new OrderByField("stringCol", OrderByField.Direction.DESC_NULLS_LAST)
        );

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), orderByFields, 10, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals(ImmutableList.of(QueryParameterValue.int64(0), QueryParameterValue.int64(100)), parameterValues);
            assertEquals("SELECT `intCol` from `schema`.`table` WHERE ((`intCol` > ? AND `intCol` < ?)) " +
                    "ORDER BY `intCol` ASC NULLS FIRST, `stringCol` DESC NULLS LAST limit 10", sql);
        }
    }

    @Test
    public void testSqlWithComplexDataTypes()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();

        // Float test
        ValueSet floatSet = SortedRangeSet.newBuilder(FLOAT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, FLOAT_TYPE, TEST_FLOAT), 
                             Marker.exactly(allocator, FLOAT_TYPE, TEST_FLOAT)))
                .build();
        constraintMap.put("floatCol", floatSet);

        // Date test
        // Calculate days since epoch for 2023-01-01
        long daysFromEpoch = java.time.LocalDate.of(2023, 1, 1).toEpochDay();
        ValueSet dateSet = SortedRangeSet.newBuilder(DATE_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DATE_TYPE, daysFromEpoch), 
                             Marker.exactly(allocator, DATE_TYPE, daysFromEpoch)))
                .build();
        constraintMap.put("dateCol", dateSet);

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            
            List<QueryParameterValue> expectedParams = ImmutableList.of(
                QueryParameterValue.float64(123.456),
                QueryParameterValue.date(TEST_DATE)
            );
            assertEquals(expectedParams, parameterValues);
            assertEquals("SELECT `floatCol`,`dateCol` from `schema`.`table` " +
                    "WHERE (`floatCol` = ?) AND (`dateCol` = ?)", sql);
        }
    }

    @Test
    public void testSqlWithNullAndEmptyChecks()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();

        ValueSet nullSet = SortedRangeSet.newBuilder(STRING_TYPE, true).build();
        constraintMap.put("nullCol", nullSet);

        ValueSet nonNullSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, STRING_TYPE), 
                             Marker.upperUnbounded(allocator, STRING_TYPE)))
                .build();
        constraintMap.put("nonNullCol", nonNullSet);

        ValueSet emptyStringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, ""), 
                             Marker.exactly(allocator, STRING_TYPE, "")))
                .build();
        constraintMap.put("emptyCol", emptyStringSet);

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            
            List<QueryParameterValue> expectedParams = ImmutableList.of(QueryParameterValue.string(""));
            assertEquals(expectedParams, parameterValues);
            assertEquals("SELECT `nullCol`,`nonNullCol`,`emptyCol` from `schema`.`table` " +
                    "WHERE (nullCol IS NULL) AND (nonNullCol IS NOT NULL) AND (`emptyCol` = ?)", sql);
        }
    }

    @Test
    public void testSqlWithMultipleInValues()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        String valueOne = "1";
        String valueTwo = "2";
        String valueThree = "3";
        // Multiple exact values using IN clause
        ValueSet inSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueOne),
                             Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueOne)))
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueTwo),
                             Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueTwo)))
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueThree),
                             Marker.exactly(allocator, STRING_TYPE, VALUE_PREFIX + valueThree)))
                .build();
        constraintMap.put("multiValueCol", inSet);

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            
            List<QueryParameterValue> expectedParams = ImmutableList.of(
                QueryParameterValue.string(VALUE_PREFIX + valueOne),
                QueryParameterValue.string(VALUE_PREFIX + valueTwo),
                QueryParameterValue.string(VALUE_PREFIX + valueThree)
            );
            assertEquals(expectedParams, parameterValues);
            assertEquals("SELECT `multiValueCol` from `schema`.`table` WHERE (`multiValueCol` IN (?,?,?))", sql);
        }
    }

    @Test
    public void testSqlWithEmptySchema()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals("SELECT null from `schema`.`table`", sql);
        }
    }

    @Test
    public void testSqlWithoutConstraints()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 0), Marker.below(allocator, INT_TYPE, 100)))
                .build();
        constraintMap.put(INT_COL, rangeSet);

        try (Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals("SELECT `intCol` from `schema`.`table`", sql);
        }
    }

    @Test
    public void testSqlWithEmptyConstraints()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 0), Marker.below(allocator, INT_TYPE, 100)))
                .build();
        constraintMap.put(INT_COL, rangeSet);

        try (Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, makeSchema(constraintMap), constraints, parameterValues);
            assertEquals("SELECT `intCol` from `schema`.`table`", sql);
        }
    }

    @Test
    public void testDateMicrosecondsHandling()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();

        // Test date with timestamp string without microseconds (length == 19)
        ValueSet dateNoMicrosSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, TEST_DATETIME),
                        Marker.exactly(allocator, STRING_TYPE, TEST_DATETIME)))
                .build();
        String dateNoMicrosCol = "dateNoMicrosCol";
        constraintMap.put(dateNoMicrosCol, dateNoMicrosSet);

        // Test date with timestamp string with microseconds (length > 19)
        ValueSet dateMicrosSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, TEST_DATETIME_MICROS),
                        Marker.exactly(allocator, STRING_TYPE, TEST_DATETIME_MICROS)))
                .build();
        String dateMicrosCol = "dateMicrosCol";
        constraintMap.put(dateMicrosCol, dateMicrosSet);

        // Create schema that maps string values to DATE_TYPE to trigger the if block
        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable(dateNoMicrosCol, DATE_TYPE));  // Map string to DATE_TYPE
        fields.add(Field.nullable(dateMicrosCol, DATE_TYPE));    // Map string to DATE_TYPE
        Schema schema = new Schema(fields);

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, schema, constraints, parameterValues);

            // For dateNoMicrosCol (length == 19), we expect .0 to be appended and padded to 6 digits
            // For dateMicrosCol (length > 19), we expect the .123 to be padded to 6 digits
            List<QueryParameterValue> expectedParams = ImmutableList.of(
                    QueryParameterValue.dateTime(TEST_DATETIME_PADDED),  // .0 appended and padded
                    QueryParameterValue.dateTime(TEST_DATE + " " + TEST_TIME + ".123000")   // .123 padded
            );

            assertEquals(expectedParams, parameterValues);
            assertEquals("SELECT `dateNoMicrosCol`,`dateMicrosCol` from `schema`.`table` " +
                    "WHERE (`dateNoMicrosCol` = ?) AND (`dateMicrosCol` = ?)", sql);
        }
    }

    @Test
    public void testDateRangePredicates()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        String dateValue = "2023-01-01T10:30:00";
        // Test date with <= predicate using string format (contains "-")
        ValueSet dateStringLteSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, STRING_TYPE),
                             Marker.exactly(allocator, STRING_TYPE, dateValue)))
                .build();
        String dateStringLteCol = "dateStringLteCol";
        constraintMap.put(dateStringLteCol, dateStringLteSet);

        // Test date with >= predicate using string format
        ValueSet dateStringGteSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, dateValue),
                             Marker.upperUnbounded(allocator, STRING_TYPE)))
                .build();
        String dateStringGteCol = "dateStringGteCol";
        constraintMap.put(dateStringGteCol, dateStringGteSet);

        // Test date with <= predicate using epoch days
        long epochDays = java.time.LocalDate.of(2023, 1, 1).toEpochDay();
        ValueSet dateEpochLteSet = SortedRangeSet.newBuilder(DATE_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, DATE_TYPE),
                             Marker.exactly(allocator, DATE_TYPE, epochDays)))
                .build();
        String dateEpochLteCol = "dateEpochLteCol";
        constraintMap.put(dateEpochLteCol, dateEpochLteSet);

        // Test date with >= predicate using epoch days
        ValueSet dateEpochGteSet = SortedRangeSet.newBuilder(DATE_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DATE_TYPE, epochDays),
                             Marker.upperUnbounded(allocator, DATE_TYPE)))
                .build();
        String dateEpochGteCol = "dateEpochGteCol";
        constraintMap.put(dateEpochGteCol, dateEpochGteSet);

        // Create schema that maps string values to DATE_TYPE to trigger the if block
        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable(dateStringLteCol, DATE_TYPE));    // Map string to DATE_TYPE
        fields.add(Field.nullable(dateStringGteCol, DATE_TYPE));    // Map string to DATE_TYPE
        fields.add(Field.nullable(dateEpochLteCol, DATE_TYPE));     // Keep as DATE_TYPE
        fields.add(Field.nullable(dateEpochGteCol, DATE_TYPE));     // Keep as DATE_TYPE
        Schema schema = new Schema(fields);

        try (Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null)) {
            List<QueryParameterValue> parameterValues = new ArrayList<>();
            String sql = BigQuerySqlUtils.buildSql(tableName, schema, constraints, parameterValues);

            // For string dates, we expect datetime parameters with microseconds
            // For epoch days, we expect date parameters
            List<QueryParameterValue> expectedParams = ImmutableList.of(
                QueryParameterValue.dateTime(TEST_DATETIME_PADDED),  // String LTE
                QueryParameterValue.dateTime(TEST_DATETIME_PADDED),  // String GTE
                QueryParameterValue.date(TEST_DATE),                 // Epoch LTE
                QueryParameterValue.date(TEST_DATE)                  // Epoch GTE
            );

            assertEquals(expectedParams, parameterValues);
            assertEquals("SELECT `dateStringLteCol`,`dateStringGteCol`,`dateEpochLteCol`,`dateEpochGteCol` from `schema`.`table` " +
                    "WHERE ((`dateStringLteCol` <= ?)) AND ((`dateStringGteCol` >= ?)) AND " +
                    "((`dateEpochLteCol` <= ?)) AND ((`dateEpochGteCol` >= ?))", sql);
        }
    }
}
