package com.example;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class ApplicationProperties {

    private static final String PROPERTIES_FILE_NAME = "key.properties";

    private static final Properties properties;

    private ApplicationProperties() {
    }

    static {
        properties = new Properties();
        load();
    }

    public static String get(String property) {
        return properties.getProperty(property);
    }

    @SneakyThrows
    private static void load() {
        Path rootPath =
                Paths.get(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResource(PROPERTIES_FILE_NAME)
                        ).toURI()
                );

        try (InputStream in = new FileInputStream(rootPath.toFile())) {
            properties.load(in);

            log.debug("Properties loaded: {}", properties);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public enum ProjectProperties {
        TWITTER_CONSUMER_API_KEY("twitter.consumer.api-key"),
        TWITTER_CONSUMER_API_KEY_SECRET("twitter.consumer.api-key.secret"),
        TWITTER_AUTHENTICATION_BEARER("twitter.authentication.bearer-token"),
        TWITTER_AUTHENTICATION_ACCESS("twitter.authentication.access-token"),
        TWITTER_AUTHENTICATION_ACCESS_SECRET("twitter.authentication.access-token.secret");

        private final String prop;

        ProjectProperties(String prop) {
            this.prop = prop;
        }

        public String get() {
            return ApplicationProperties.get(prop);
        }
    }
}
