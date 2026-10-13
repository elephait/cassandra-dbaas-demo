package demo.platform;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Translate Docker discovery addresses into host-published ports.
 */
public class LocalAddressTranslator implements AddressTranslator {
    private static final AtomicReference<Map<String, InetSocketAddress>> CONFIGURED_ADDRESSES =
            new AtomicReference<>(Map.of());

    private final Map<String, InetSocketAddress> addresses;

    public LocalAddressTranslator(DriverContext context) {
        this(CONFIGURED_ADDRESSES.get());
    }

    public static void configure(Map<String, InetSocketAddress> addresses) {
        CONFIGURED_ADDRESSES.set(Map.copyOf(new HashMap<>(addresses)));
    }

    public LocalAddressTranslator(Map<String, InetSocketAddress> addresses) {
        this.addresses = Map.copyOf(new HashMap<>(addresses));
    }

    public InetSocketAddress translate(InetSocketAddress address) {
        String host = address.getAddress() == null ? address.getHostString() : address.getAddress().getHostAddress();
        return addresses.getOrDefault(host, address);
    }

    public void close() {
    }
}
