package org.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class IsetdxDataSourceConfig {

    @Value("${isetdx.datasource.url}")
    private String url;

    @Value("${isetdx.datasource.username}")
    private String username;

    @Value("${isetdx.datasource.password}")
    private String password;

    @Value("${isetdx.datasource.driver-class-name}")
    private String driverClassName;

    @Bean(name = "isetdxJdbc")
    public JdbcTemplate isetdxJdbc() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName(driverClassName);
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        return new JdbcTemplate(ds);
    }
}
