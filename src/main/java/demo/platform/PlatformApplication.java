package demo.platform;

import demo.platform.misc.ApiExceptionHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ApiExceptionHandler.CassandraProperties.class)
public class PlatformApplication {
    public static void main(String[] args) {
        // OpenJDK RMI client limits must be installed before RMI initializes. Per-request
        // Future timeouts cannot cancel a remote repair and are not transport timeouts.
        System.getProperties().putIfAbsent("sun.rmi.transport.tcp.responseTimeout", "10000");
        System.getProperties().putIfAbsent("sun.rmi.transport.tcp.handshakeTimeout", "10000");
        SpringApplication.run(PlatformApplication.class, args);
    }
}
