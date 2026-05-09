package org.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AppConfig {

    private static final String DB_CONFIG_PATH = "DbConnection.config";

    private final Properties kafkaProps;
    private final Properties dbProps;

    public static AppConfig load(String kafkaConfigPath) throws IOException {
        Properties kafka = loadFile(kafkaConfigPath);
        Properties db    = loadFile(DB_CONFIG_PATH);
        return new AppConfig(kafka, db);
    }

    private AppConfig(Properties kafkaProps, Properties dbProps) {
        this.kafkaProps = kafkaProps;
        this.dbProps    = dbProps;
    }

    public String bootstrapServers() { return kafkaProps.getProperty("kafka.bootstrap.servers"); }

    public String tickerTopic()   { return kafkaProps.getProperty("bainance.ticker.topic",    "crypto-ticker"); }
    public String tickerGroupId() { return kafkaProps.getProperty("bainance.ticker.group.id", "isetdx-ticker-consumer"); }
    public String klineTopic()    { return kafkaProps.getProperty("bainance.kline.topic",     "crypto-kline"); }
    public String klineGroupId()  { return kafkaProps.getProperty("bainance.kline.group.id",  "isetdx-kline-consumer"); }

    public String bainanceDbUrl()      { return dbProps.getProperty("db.url"); }
    public String bainanceDbUser()     { return dbProps.getProperty("db.user"); }
    public String bainanceDbPassword() { return dbProps.getProperty("db.password"); }

    private static Properties loadFile(String path) throws IOException {
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            p.load(fis);
        }
        return p;
    }
}
