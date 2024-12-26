package org.pritam.HikariCP;

import org.bukkit.configuration.file.YamlConfiguration;
import org.pritam.HikariCP.records.SQL;
import org.pritam.WoolWars;
import org.pritam.HikariCP.enums.SQLExitCode;
import org.pritam.utils.Pair;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class DataStoreQueue {
    private static final ConcurrentLinkedQueue<SQL> saveQueue = new ConcurrentLinkedQueue<>();

    public static void addToSaveQueue(String query, Object... params) {
        saveQueue.add(new SQL(query, params));
    }

    public static void startSaveTask() {
        WoolWars.getPlugin().getServer().getScheduler().runTaskTimerAsynchronously(WoolWars.getPlugin(), () -> {
            Pair<Connection, SQLExitCode> connection = DataSource.getConnection();

            if (connection.getSecondary() == SQLExitCode.SUCCESS) {
                try (Connection conn = connection.getPrimary()) {
                    while (!saveQueue.isEmpty()) {
                        SQL sqlTask = saveQueue.poll();
                        try (PreparedStatement statement = conn.prepareStatement(sqlTask.query())) {
                            Object[] params = sqlTask.params();
                            for (int i = 0; i < params.length; i++) {
                                statement.setObject(i + 1, params[i]);
                            }
                            statement.executeUpdate();
                        } catch (SQLException e) {
                            WoolWars.getPlugin().getLogger().severe("Failed to execute SQL query: " + sqlTask.query() + " Error: " + e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    WoolWars.getPlugin().getLogger().severe("Database connection error: " + e.getMessage());
                    saveQueueToFile();
                    WoolWars.getPlugin().getLogger().info("SQL queue saved to file. Disabling plugin.");
                    WoolWars.getPlugin().getServer().getPluginManager().disablePlugin(WoolWars.getPlugin());
                }
            } else {
                WoolWars.getPlugin().getLogger().severe("Failed to establish database connection.");
                saveQueueToFile();
                WoolWars.getPlugin().getLogger().info("SQL queue saved to file. Disabling plugin.");
                WoolWars.getPlugin().getServer().getPluginManager().disablePlugin(WoolWars.getPlugin());
            }
        }, 0, 20 * 60);
    }

    private static void saveQueueToFile() {
        File configFile = new File(WoolWars.getPlugin().getDataFolder(), "sql-cache.yml");
        YamlConfiguration config = YamlConfiguration.loadConfiguration(configFile);

        List<String> sqlList = saveQueue.stream()
                .map(SQL::toString)
                .collect(Collectors.toList());
        config.set("queue", sqlList);

        boolean saveSuccessful = false;
        for (int i = 0; i < 3; i++) {
            try {
                config.save(configFile);
                saveSuccessful = true;
                WoolWars.getPlugin().getLogger().info("SQL queue successfully saved on attempt " + (i + 1));
                break;
            } catch (Exception e) {
                WoolWars.getPlugin().getLogger().warning("Attempt " + (i + 1) + " to save SQL queue failed: " + e.getMessage());
            }
        }
        if (!saveSuccessful) {
            WoolWars.getPlugin().getLogger().severe("Failed to save SQL queue after multiple attempts. Manual recovery may be required.");
        }
    }

    public static void recoverQueueFromFile() {
        File configFile = new File(WoolWars.getPlugin().getDataFolder(), "sql-cache.yml");
        if (configFile.exists()) {
            YamlConfiguration config = YamlConfiguration.loadConfiguration(configFile);
            List<String> sqlList = config.getStringList("queue");

            for (String sqlString : sqlList) {
                String[] parts = sqlString.split("\\|\\|");
                String query = parts[0];
                Object[] params = parts.length > 1 ? parseParams(parts[1]) : new Object[0];
                saveQueue.add(new SQL(query, params));
            }
            WoolWars.getPlugin().getLogger().info("Recovered SQL queue from file. Queue size: " + saveQueue.size());

            if (configFile.delete()) {
                WoolWars.getPlugin().getLogger().info("SQL cache file deleted after recovery.");
            } else {
                WoolWars.getPlugin().getLogger().warning("Failed to delete SQL cache file after recovery.");
            }
        }
    }

    private static Object[] parseParams(String paramString) {
        return paramString.isEmpty() ? new Object[0] : paramString.split(",");
    }

    public static List<Object[]> getResultSet(SQL sql) {
        Pair<Connection, SQLExitCode> connectionPair = DataSource.getConnection();

        if (connectionPair.getSecondary() != SQLExitCode.SUCCESS) {
            WoolWars.getPlugin().getLogger().severe("Failed to establish database connection.");
            return new ArrayList<>();
        }

        try (Connection conn = connectionPair.getPrimary();
             PreparedStatement statement = conn.prepareStatement(sql.query())) {

            for (int i = 0; i < sql.params().length; i++) {
                statement.setObject(i + 1, sql.params()[i]);
            }

            try (var resultSet = statement.executeQuery()) {
                List<Object[]> results = new ArrayList<>();
                int columnCount = resultSet.getMetaData().getColumnCount();

                while (resultSet.next()) {
                    Object[] row = new Object[columnCount];
                    for (int col = 1; col <= columnCount; col++) {
                        row[col - 1] = resultSet.getObject(col);
                    }
                    results.add(row);
                }
                return results;
            }

        } catch (SQLException e) {
            WoolWars.getPlugin().getLogger().severe("Error executing query: " + sql.query() + " Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }
}
