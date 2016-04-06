package disco.consul;

import com.orbitz.consul.NotRegisteredException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * @author Maxim Neverov
 */
@SpringBootApplication
@EnableWebMvc
public class Configuration {

    public static void main(String[] args) throws NotRegisteredException {
        SpringApplication.run(Configuration.class, args);
    }
}


