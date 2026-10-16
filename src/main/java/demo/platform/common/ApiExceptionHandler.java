package demo.platform.common;

import com.datastax.oss.driver.api.core.DriverException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** The only place that maps domain failures to HTTP status codes. */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static Map<String, String> error(String message, String fallback) {
        return Map.of("error", message == null ? fallback : message);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> invalid(IllegalArgumentException exception) {
        return error(exception.getMessage(), "Invalid request");
    }

    @ExceptionHandler(ControlPlaneExceptions.Forbidden.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public Map<String, String> forbidden(ControlPlaneExceptions.Forbidden exception) {
        return error(exception.getMessage(), "Forbidden");
    }

    @ExceptionHandler({ControlPlaneExceptions.UnknownCluster.class, ControlPlaneExceptions.NotFound.class})
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> notFound(RuntimeException exception) {
        return error(exception.getMessage(), "Not found");
    }

    @ExceptionHandler({ControlPlaneExceptions.Conflict.class, ControlPlaneExceptions.KeyspaceQuotaExceeded.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, String> conflict(RuntimeException exception) {
        return error(exception.getMessage(), "Conflict");
    }

    @ExceptionHandler(ControlPlaneExceptions.Unavailable.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> unavailable(ControlPlaneExceptions.Unavailable exception) {
        return error(exception.getMessage(), "Temporarily unavailable; retry");
    }

    @ExceptionHandler(DataAccessResourceFailureException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> databaseUnavailable() {
        return Map.of("error", "Control-plane database unavailable; retry safely using the same request key");
    }

    @ExceptionHandler(DriverException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> cassandraUnavailable() {
        return Map.of("error", "Cassandra request failed; check cluster health and keyspace provisioning");
    }
}
