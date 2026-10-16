package demo.platform.cluster.cassandra;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalAddressTranslator implements AddressTranslator {
    public static final DriverOption MAPPINGS =
        () -> "advanced.local-address-mappings";

    private final Map<String, InetSocketAddress> addresses;

    public LocalAddressTranslator(DriverContext context) {
        this(context.getConfig().getDefaultProfile().getStringMap(MAPPINGS, Map.of()).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                String target = entry.getValue();
                int colon = target.lastIndexOf(':');
                return new InetSocketAddress(target.substring(0, colon), Integer.parseInt(target.substring(colon + 1)));
            })));
    }

    public LocalAddressTranslator(Map<String, InetSocketAddress> addresses) {
        this.addresses = Map.copyOf(new HashMap<>(addresses));
    }

    @Override
    public InetSocketAddress translate(InetSocketAddress address) {
        String host = address.getAddress() == null
            ? address.getHostString() : address.getAddress().getHostAddress();
        return addresses.getOrDefault(host, address);
    }

    @Override
    public void close() {
    }
}
