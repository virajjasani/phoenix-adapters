package org.apache.phoenix.ddb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Phoenix client connections.
 * Loads connection properties from a configuration file at startup
 * and uses them to create connections.
 */
public class ConnectionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtil.class);
    private static final String CLIENT_CONNECTION_CONFIG_FILE =
            "phoenix-client-connection.properties";

    private static final Properties PROPS;

    static {
        try {
            PROPS = loadConfiguration();
        } catch (IOException e) {
            LOGGER.error("Failed to load Phoenix connection configuration", e);
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Returns a mutable copy of the connection properties.
     * Use this when you need to add or modify properties for a specific connection.
     */
    public static Properties getMutableProps() {
        return new Properties(PROPS);
    }

    /**
     * Get a Connection with the given url and loaded config properties.
     */
    public static Connection getConnection(String url) throws SQLException {
        return DriverManager.getConnection(url, PROPS);
    }

    private static Properties loadConfiguration() throws IOException {
        Properties props = new Properties();
        try (InputStream is = ConnectionUtil.class.getClassLoader()
                .getResourceAsStream(CLIENT_CONNECTION_CONFIG_FILE)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded Phoenix connection configuration from {}: {}",
                        CLIENT_CONNECTION_CONFIG_FILE, props);
            } else {
                throw new IOException(
                        "Configuration file not found: " + CLIENT_CONNECTION_CONFIG_FILE);
            }
        }
        return props;
    }
}