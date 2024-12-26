package org.pritam;

import org.bukkit.plugin.java.JavaPlugin;
import org.pritam.HikariCP.DataSource;
import org.pritam.HikariCP.DataStoreQueue;

public final class WoolWars extends JavaPlugin {
    public static WoolWars plugin;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        plugin = this;

        DataSource.init(getConfig());

        if (DataSource.isDbEnabled()) {
            DataStoreQueue.recoverQueueFromFile();
            DataStoreQueue.startSaveTask();
        }
    }

    @Override
    public void onDisable() {
        if (DataSource.isDbEnabled()) {
            DataSource.close();
        }
    }

    public static WoolWars getPlugin() {
        return plugin;
    }
}
