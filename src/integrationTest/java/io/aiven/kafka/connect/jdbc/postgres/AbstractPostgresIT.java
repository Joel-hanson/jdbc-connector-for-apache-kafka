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

package io.aiven.kafka.connect.jdbc.postgres;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.connect.jdbc.AbstractIT;

import org.assertj.core.util.Arrays;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

public class AbstractPostgresIT extends AbstractIT {

    public static final String DEFAULT_POSTGRES_TAG = "10.20";
    private static final DockerImageName DEFAULT_POSTGRES_IMAGE_NAME =
            DockerImageName.parse("postgres")
                    .withTag(DEFAULT_POSTGRES_TAG);

    @Container
    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER = new PostgreSQLContainer<>(
            DEFAULT_POSTGRES_IMAGE_NAME
    );

    protected void executeSqlStatement(final String sqlStatement) throws SQLException {
        try (final Connection connection = getDatasource().getConnection();
             final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sqlStatement);
        }
    }

    protected DataSource getDatasource() {
        final PGSimpleDataSource pgSimpleDataSource = new PGSimpleDataSource();
        pgSimpleDataSource.setServerNames(Arrays.array(POSTGRES_CONTAINER.getHost()));
        pgSimpleDataSource.setPortNumbers(new int[] {POSTGRES_CONTAINER.getMappedPort(5432)});
        pgSimpleDataSource.setDatabaseName(POSTGRES_CONTAINER.getDatabaseName());
        pgSimpleDataSource.setUser(POSTGRES_CONTAINER.getUsername());
        pgSimpleDataSource.setPassword(POSTGRES_CONTAINER.getPassword());
        return pgSimpleDataSource;
    }

    protected Map<String, String> basicConnectorConfig() {
        final HashMap<String, String> config = new HashMap<>();
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistryContainer.getSchemaRegistryUrl());
        config.put("tasks.max", "1");
        config.put("connection.url", POSTGRES_CONTAINER.getJdbcUrl());
        config.put("connection.user", POSTGRES_CONTAINER.getUsername());
        config.put("connection.password", POSTGRES_CONTAINER.getPassword());
        config.put("dialect.name", "PostgreSqlDatabaseDialect");
        return config;
    }

}
