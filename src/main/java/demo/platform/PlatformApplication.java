package demo.platform;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.net.InetSocketAddress;

@SpringBootApplication
public class PlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(PlatformApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    CqlSession session(@Value("${demo.contact-points}") String points,
                       @Value("${demo.datacenter}") String dc) {
        var builder = CqlSession.builder().withLocalDatacenter(dc);
        for (String point : points.split(",")) {
            String[] parts = point.trim().split(":");
            builder.addContactPoint(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
        }
        return builder.build();
    }
}
