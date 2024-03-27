/*
 * Copyright 2024 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.jdbc.oracle;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.connect.jdbc.JdbcSinkConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.db.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotSame;

public class VerifyDeleteIT extends AbstractOracleIT {

    private static final String TEST_TOPIC_NAME = "SINK_TOPIC";
    private static final String CONNECTOR_NAME = "test-sink-connector";
    private static final int TEST_TOPIC_PARTITIONS = 1;

    private static final Schema VALUE_RECORD_SCHEMA = new Schema.Parser().parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"value\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ]\n"
            + "}");

    private static final String DROP_TABLE = String.format("DROP TABLE IF EXISTS %s", TEST_TOPIC_NAME);
    private static final String CREATE_TABLE = String.format("CREATE TABLE \"%s\" (\n"
            + "    \"id\" NUMBER NOT NULL,\n"
            + "    \"name\" VARCHAR2(255) NOT NULL,\n"
            + "    \"value\" VARCHAR2(255) NOT NULL,\n"
            + "PRIMARY KEY(\"id\")"
            + ")", TEST_TOPIC_NAME);

    private Map<String, String> sinkConnectorConfigForDelete() {
        final Map<String, String> config = basicConnectorConfig();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", JdbcSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC_NAME);
        config.put("pk.mode", "record_key");
        config.put("pk.fields", "id"); // assigned name for the primitive key
        config.put("delete.enabled", String.valueOf(true));
        return config;
    }

    private void assertNotEmptyPoll(final Duration duration) {
        final ConsumerRecords<?, ?> records = consumer.poll(duration);
        assertNotSame(ConsumerRecords.empty(), records);
    }

    private ProducerRecord<String, GenericRecord> createRecord(
            final int id, final int partition, final String name, final String value) {
        final GenericRecord record = new GenericData.Record(VALUE_RECORD_SCHEMA);
        record.put("name", name);
        record.put("value", value);
        return new ProducerRecord<>(TEST_TOPIC_NAME, partition, String.valueOf(id), record);
    }

    private ProducerRecord<String, GenericRecord> createTombstoneRecord(
            final int id, final int partition) {
        return new ProducerRecord<>(TEST_TOPIC_NAME, partition, String.valueOf(id), null);
    }

    private void sendTestData(final int numberOfRecords) throws InterruptedException, ExecutionException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String recordName = "user-" + i;
                final String recordValue = "value-" + i;
                final ProducerRecord<String, GenericRecord> msg = createRecord(i, partition, recordName, recordValue);
                sendFutures.add(producer.send(msg));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    private void sendTestDataWithTombstone(final int numberOfRecords) throws InterruptedException, ExecutionException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final ProducerRecord<String, GenericRecord> record = createTombstoneRecord(i, partition);
                sendFutures.add(producer.send(record));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    private void sendMixedTestDataWithTombstone(final int numberOfRecords, final int numberOfTombstoneRecords)
            throws InterruptedException, ExecutionException {
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String recordName = "user-" + i;
                final String recordValue = "value-" + i;
                final ProducerRecord<String, GenericRecord> msg = createRecord(
                        i, partition, recordName, recordValue);
                sendFutures.add(producer.send(msg));
                if (i < numberOfTombstoneRecords) {
                    final ProducerRecord<String, GenericRecord> record = createTombstoneRecord(i, partition);
                    sendFutures.add(producer.send(record));
                }
            }
        }

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }
    }

    @BeforeEach
    public void afterEach() throws SQLException {
        executeSqlStatement(DROP_TABLE);
        executeSqlStatement(CREATE_TABLE);
    }

    @Test
    public void testDeleteTombstoneRecord() throws Exception {
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        connectRunner.createConnector(sinkConnectorConfigForDelete());

        sendTestData(3);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(3);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("0")
                            .value().isEqualTo("1")
                            .value().isEqualTo("2");
                });

        // Send test data to Kafka topic (including a tombstone record)
        sendTestDataWithTombstone(1);

        assertNotEmptyPoll(Duration.ofSeconds(50));

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(2);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("1")
                            .value().isEqualTo("2");
                });
    }

    @Test
    public void testWithJustTombstoneRecordInInsertMode() throws Exception {
        // Test logic is similar to previous tests, but with tombstone records.
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        final Map<String, String> config = sinkConnectorConfigForDelete();
        connectRunner.createConnector(config);

        sendTestDataWithTombstone(2);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(0);
                });
    }

    @Test
    public void testMultiInsertMode() throws Exception {
        // Test logic is similar to previous tests, but with multi-insert mode enabled
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        final Map<String, String> config = sinkConnectorConfigForDelete();
        config.put("insert.mode", "MULTI");
        connectRunner.createConnector(config);

        sendTestData(5);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(5);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("0")
                            .value().isEqualTo("1")
                            .value().isEqualTo("2")
                            .value().isEqualTo("3")
                            .value().isEqualTo("4");
                });
    }

    @Test
    public void testDeleteTombstoneRecordWithMultiMode() throws Exception {
        // Test logic is similar to previous tests, but with multi-insert mode enabled and tombstone records included
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        final Map<String, String> config = sinkConnectorConfigForDelete();
        config.put("insert.mode", "MULTI");
        connectRunner.createConnector(config);

        sendTestData(5);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(5);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("0")
                            .value().isEqualTo("1")
                            .value().isEqualTo("2")
                            .value().isEqualTo("3")
                            .value().isEqualTo("4");
                });

        sendTestDataWithTombstone(1);

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(4);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("1")
                            .value().isEqualTo("2")
                            .value().isEqualTo("3")
                            .value().isEqualTo("4");
                });
    }

    @Test
    public void testWithJustTombstoneRecordWithMultiMode() throws Exception {
        // Test logic with multi-insert mode enabled and has only tombstone records
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        final Map<String, String> config = sinkConnectorConfigForDelete();
        config.put("insert.mode", "MULTI");
        connectRunner.createConnector(config);

        sendTestDataWithTombstone(2);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(18))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(0);
                });
    }

    @Test
    public void testMixTombstoneRecordsWithMultiMode() throws Exception {
        // Test logic is similar to previous tests, but with mixed tombstone records and multi-insert mode
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));

        // Start the sink connector
        final Map<String, String> config = sinkConnectorConfigForDelete();
        config.put("insert.mode", "MULTI");
        connectRunner.createConnector(config);

        sendMixedTestDataWithTombstone(5, 2);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(19))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(3);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("2")
                            .value().isEqualTo("3")
                            .value().isEqualTo("4");
                });
    }
}
