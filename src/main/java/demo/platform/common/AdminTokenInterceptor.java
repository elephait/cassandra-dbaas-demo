package demo.platform.common;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Demo-grade authorization: one shared admin token, checked once for every route except health.
 * Production would replace this with real identity and per-cluster authorization.
 */
@Configuration
@RequiredArgsConstructor
public class AdminTokenInterceptor implements HandlerInterceptor, WebMvcConfigurer {
    static final String HEADER = "X-Admin-Token";

    private final PlatformProperties properties;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Health is public so load balancers and operators can probe it.
        registry.addInterceptor(this).addPathPatterns("/**").excludePathPatterns("/clusters/*/health", "/error");
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String supplied = request.getHeader(HEADER);
        byte[] expected = properties.getAdminToken().getBytes(StandardCharsets.UTF_8);
        if (supplied == null || !MessageDigest.isEqual(expected, supplied.getBytes(StandardCharsets.UTF_8)))
            throw new ControlPlaneExceptions.Forbidden("Invalid admin token");
        return true;
    }
}
