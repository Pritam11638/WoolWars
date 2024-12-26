package org.pritam.HikariCP;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.configuration.file.FileConfiguration;
import org.pritam.WoolWars;
import org.pritam.HikariCP.enums.SQLExitCode;
import org.pritam.utils.Pair;

import java.sql.Connection;
import java.sql.SQLException;

public class DataSource {
    private static HikariDataSource hikariDataSource;
    private static boolean dbEnabled;

    public static void init(FileConfiguration config) {
        configureDataSource(config);
        validateConnection();
    }

    private static void configureDataSource(FileConfiguration config) {
        HikariConfig hikariConfig = new HikariConfig();

        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%s/%s",
                config.getString("database.hostname"),
                config.getString("database.port"),
                config.getString("database.database-name")
        );

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getString("database.username"));
        hikariConfig.setPassword(config.getString("database.password"));
        hikariConfig.setMaximumPoolSize(config.getInt("maximumPoolSize", 10));
        hikariConfig.setMinimumIdle(config.getInt("minimumIdle", 2));
        hikariConfig.setIdleTimeout(config.getLong("idleTimeout", 30000)); // 30 seconds default
        hikariConfig.setConnectionTimeout(config.getLong("connectionTimeout", 30000));
        hikariConfig.setValidationTimeout(config.getLong("validationTimeout", 5000)); // 5 seconds default
        hikariConfig.setLeakDetectionThreshold(config.getLong("leakDetectionThreshold", 2000));
        hikariConfig.setMaximumPoolSize(config.getInt("maximumPoolSize", 10));

        hikariDataSource = new HikariDataSource(hikariConfig);
    }

    private static void validateConnection() {
        try (Connection testConnection = hikariDataSource.getConnection()) {
            dbEnabled = testConnection != null && testConnection.isValid(1);
        } catch (SQLException e) {
            dbEnabled = false;
            WoolWars.getPlugin().getLogger().severe("Database validation failed: " + e.getMessage());
        }
    }

    public static Pair<Connection, SQLExitCode> getConnection() {
        if (!dbEnabled) {
            return new Pair<>(null, SQLExitCode.NULL);
        }

        try {
            return new Pair<>(hikariDataSource.getConnection(), SQLExitCode.SUCCESS);
        } catch (SQLException e) {
            WoolWars.getPlugin().getLogger().severe("Failed to establish database connection: " + e.getMessage());
            return new Pair<>(null, SQLExitCode.ERROR);
        }
    }

    public static void close() {
        if (hikariDataSource != null && !hikariDataSource.isClosed()) {
            hikariDataSource.close();
        }
    }

    public static boolean isDbEnabled() {
        return dbEnabled;
    }
}