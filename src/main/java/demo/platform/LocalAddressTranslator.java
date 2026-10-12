package demo.platform;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Translate Docker discovery addresses into host-published ports.
 */
public class LocalAddressTranslator implements AddressTranslator {
    private static final Map<String, Integer> PORTS = Map.of("172.28.91.11", 9042, "172.28.91.12", 9043, "172.28.91.13", 9044);

    public LocalAddressTranslator(DriverContext context) {
    }

    public InetSocketAddress translate(InetSocketAddress address) {
        Integer port = PORTS.get(address.getAddress() == null ? address.getHostString() : address.getAddress().getHostAddress());
        return port == null ? address : new InetSocketAddress("127.0.0.1", port);
    }

    public void close() {
    }
}
