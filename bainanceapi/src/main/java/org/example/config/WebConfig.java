package org.example.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * REST API CORS 설정.
 *
 * <p>React 개발 서버(보통 localhost:5173)에서 이 API를 호출할 때 브라우저의 CORS 정책을 통과하기 위해 필요하다.
 * 운영 배포 시에는 allowedOriginPatterns를 실제 프론트엔드 도메인으로 제한해야 한다.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET");
    }
}
