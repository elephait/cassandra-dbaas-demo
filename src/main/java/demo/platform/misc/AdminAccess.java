package demo.platform.misc;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
@RequiredArgsConstructor
public class AdminAccess {
    private final ApiExceptionHandler.CassandraProperties properties;

    public void require(String supplied) {
        if (!properties.getAdminToken().equals(supplied))
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid admin token");
    }

    public int validateLimit(int limit) {
        if (limit < 1 || limit > 200)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and 200");
        return limit;
    }
}
