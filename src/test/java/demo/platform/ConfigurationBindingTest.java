package demo.platform;

import demo.platform.misc.ApiExceptionHandler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigurationBindingTest {
    @Test
    void customDeploymentValuesBindWithoutLegacyProperties() throws Exception {
        assertFalse(new ClassPathResource("application.properties").exists());
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("deployment", Map.of(
            "DBAAS_POLL_INTERVAL_SECONDS", "13", "DBAAS_CONTACT_POINTS", "db.example:9042", "DBAAS_DATACENTER", "remote_dc", "DBAAS_MAX_KEYSPACES", "12")));
        for (var source : new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml")))
            environment.getPropertySources().addLast(source);
        var properties = Binder.get(environment).bind("demo", ApiExceptionHandler.CassandraProperties.class).get();
        assertEquals("db.example:9042", properties.getContactPoints());
        assertEquals("remote_dc", properties.getDatacenter());
        assertEquals(12, properties.getMaxKeyspaces());
        assertEquals(13, properties.getPollIntervalSeconds());
    }

    @Test
    void demoPollingDefaultsToTwentySecondsAndRejectsBusyLoops() {
        var properties = new ApiExceptionHandler.CassandraProperties();
        assertEquals(20, properties.getPollIntervalSeconds());
        assertThrows(IllegalArgumentException.class, () -> properties.setPollIntervalSeconds(0));
    }
}
