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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import io.aiven.connect.jdbc.JdbcSinkConnector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.db.type.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.db.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


public class VerifyInsertIT extends AbstractOracleIT {

    private static final String TEST_TOPIC_NAME = "SINK_TOPIC";
    private static final String CONNECTOR_NAME = "test-sink-connector";
    private static final int TEST_TOPIC_PARTITIONS = 1;
    private static final Schema VALUE_RECORD_SCHEMA = new Schema.Parser().parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\n"
            + "      \"name\": \"id\",\n"
            + "      \"type\": \"int\"\n"
            + "    },\n"
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
    private static final String CREATE_TABLE_WITH_PK = String.format("CREATE TABLE \"%s\" (\n"
            + "    \"id\" NUMBER NOT NULL,\n"
            + "    \"name\" VARCHAR2(255) NOT NULL,\n"
            + "    \"value\" VARCHAR2(255) NOT NULL,\n"
            + "PRIMARY KEY(\"id\")"
            + ")", TEST_TOPIC_NAME);
    private static final String CREATE_TABLE = String.format("CREATE TABLE \"%s\" (\n"
            + "    \"id\" NUMBER NOT NULL,\n"
            + "    \"name\" VARCHAR2(255) NOT NULL,\n"
            + "    \"value\" VARCHAR2(255) NOT NULL\n"
            + ")", TEST_TOPIC_NAME);


    private Map<String, String> basicSinkConnectorConfig() {
        final Map<String, String> config = basicConnectorConfig();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", JdbcSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC_NAME);
        return config;
    }

    private Map<String, String> sinkConnectorConfigWithPKModeRecordKey() {
        final Map<String, String> config = basicSinkConnectorConfig();
        config.put("pk.mode", "record_key");
        config.put("pk.fields", "id"); // assigned name for the primitive key
        return config;
    }

    private ProducerRecord<String, GenericRecord> createRecord(
            final int id, final int partition, final String name, final String value) {
        final GenericRecord record = new GenericData.Record(VALUE_RECORD_SCHEMA);
        record.put("id", id);
        record.put("name", name);
        record.put("value", value);
        return new ProducerRecord<>(TEST_TOPIC_NAME, partition, String.valueOf(id), record);
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

    @AfterEach
    public void afterEach() throws SQLException {
        executeSqlStatement(DROP_TABLE);
    }

    @Test
    public void testSinkConnector() throws Exception {
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        executeSqlStatement(CREATE_TABLE);
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));
        // Start the sink connector
        connectRunner.createConnector(basicSinkConnectorConfig());

        // Send test data to Kafka topic
        sendTestData(1);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(19))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(1);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("0");
                });
    }

    @Test
    public void testSinkWithPKModeRecordKeyConnector() throws Exception {
        createTopic(TEST_TOPIC_NAME, 1); // Create Kafka topic matching the table name
        executeSqlStatement(CREATE_TABLE_WITH_PK);
        consumer.assign(Collections.singleton(new TopicPartition(TEST_TOPIC_NAME, 0)));
        // Start the sink connector
        connectRunner.createConnector(sinkConnectorConfigWithPKModeRecordKey());

        // Send test data to Kafka topic
        sendTestData(1);

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofSeconds(19))
                .untilAsserted(() -> {
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).hasNumberOfRows(1);
                    assertThat(new Table(getDatasource(), TEST_TOPIC_NAME)).column("ID")
                            .value().isEqualTo("0");
                });
    }
}
