package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

public class PropertyFileReader {

    Properties properties = new Properties();
    String filename = "src/test/resources/application.properties";

    public PropertyFileReader() {
        loadProperty();
    }

    private void loadProperty() {
        try (InputStream inputStream = new FileInputStream(filename)) {
            properties.load(inputStream);
            System.getProperties().putAll(properties);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public String getProperty(String name) {
        return Optional.ofNullable(System.getProperty(name))
                .filter(value -> !value.isEmpty())
                .orElseThrow(() -> new IllegalStateException("missing property value " + name));
    }
}
