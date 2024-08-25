package xogta.tech;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigManager {
    private Properties properties;

    public ConfigManager(String configPath) throws IOException {
        properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}