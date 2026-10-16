package demo.platform;

import demo.platform.common.PlatformProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(PlatformProperties.class)
public class PlatformApplication {
    public static void main(String[] args) {
        // RMI transport timeouts must be set before RMI initializes; they bound every JMX call.
        System.getProperties().putIfAbsent("sun.rmi.transport.tcp.responseTimeout", "10000");
        System.getProperties().putIfAbsent("sun.rmi.transport.tcp.handshakeTimeout", "10000");
        SpringApplication.run(PlatformApplication.class, args);
    }
}
