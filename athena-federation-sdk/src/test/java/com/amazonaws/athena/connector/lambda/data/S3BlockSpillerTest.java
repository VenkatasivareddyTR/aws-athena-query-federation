package com.amazonaws.athena.connector.lambda.data;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class S3BlockSpillerTest
{
    private static final Logger logger = LoggerFactory.getLogger(S3BlockSpillerTest.class);

    private String bucket = "MyBucket";
    private String prefix = "blocks/spill";
    private String requestId = "requestId";
    private String splitId = "splitId";

    @Mock
    private S3Client mockS3;

    private S3BlockSpiller blockWriter;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private Block expected;
    private BlockAllocatorImpl allocator;
    private SpillConfig spillConfig;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        spillConfig = SpillConfig.newBuilder().withEncryptionKey(keyFactory.create())
                .withRequestId(requestId)
                .withSpillLocation(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withPrefix(prefix)
                        .withQueryId(requestId)
                        .withSplitId(splitId)
                        .withIsDirectory(true)
                        .build())
                .withRequestId(requestId)
                .build();

        blockWriter = new S3BlockSpiller(mockS3, spillConfig, allocator, schema, ConstraintEvaluator.emptyEvaluator(), com.google.common.collect.ImmutableMap.of());

        expected = allocator.createBlock(schema);
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 100);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar");
        BlockUtils.setValue(expected.getFieldVector("col1"), 1, 101);
        BlockUtils.setValue(expected.getFieldVector("col2"), 1, "VarChar1");
        expected.setRowCount(2);
    }

    @After
    public void tearDown()
            throws Exception
    {
        expected.close();
        allocator.close();
        blockWriter.close();
    }

    @Test
    public void spillTest()
            throws IOException
    {
        logger.info("spillTest: enter");

        logger.info("spillTest: starting write test");

        final ByteHolder byteHolder = new ByteHolder();

        ArgumentCaptor<PutObjectRequest> requestArgument = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyArgument = ArgumentCaptor.forClass(RequestBody.class);

        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        PutObjectResponse response = PutObjectResponse.builder().build();
                        InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        return response;
                    }
                });

        SpillLocation blockLocation = blockWriter.write(expected);

        if (blockLocation instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".0", ((S3SpillLocation) blockLocation).getKey());
        }
        verify(mockS3, times(1)).putObject(requestArgument.capture(), bodyArgument.capture());
        assertEquals(requestArgument.getValue().bucket(), bucket);
        assertEquals(requestArgument.getValue().key(), prefix + "/" + requestId + "/" + splitId + ".0");

        SpillLocation blockLocation2 = blockWriter.write(expected);

        if (blockLocation2 instanceof S3SpillLocation) {
            assertEquals(bucket, ((S3SpillLocation) blockLocation2).getBucket());
            assertEquals(prefix + "/" + requestId + "/" + splitId + ".1", ((S3SpillLocation) blockLocation2).getKey());
        }

        verify(mockS3, times(2)).putObject(requestArgument.capture(), bodyArgument.capture());
        assertEquals(requestArgument.getValue().bucket(), bucket);
        assertEquals(requestArgument.getValue().key(), prefix + "/" + requestId + "/" + splitId + ".1");

        verifyNoMoreInteractions(mockS3);
        reset(mockS3);

        logger.info("spillTest: Starting read test.");

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                    }
                });

        Block block = blockWriter.read((S3SpillLocation) blockLocation2, spillConfig.getEncryptionKey(), expected.getSchema());

        assertEquals(expected, block);

        verify(mockS3, times(1))
                .getObject(any(GetObjectRequest.class));

        verifyNoMoreInteractions(mockS3);

        logger.info("spillTest: exit");
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
