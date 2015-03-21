package me.yonatan.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by yonatan on 12/1/14.
 */
@Configuration
public class Config {
    @Bean
    ObjectMapper objectMapper(){
        return new ObjectMapper();
    }
}
