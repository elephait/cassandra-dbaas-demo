package demo.platform.common;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Single source of time for leases and deadlines. Every replica compares against the database clock,
 * so application-host clock skew cannot extend or shorten a lease.
 */
@Component
@RequiredArgsConstructor
public class DatabaseClock {
    private final JdbcTemplate jdbc;

    public Instant now() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", (rs, row) -> rs.getTimestamp(1).toInstant());
    }
}
