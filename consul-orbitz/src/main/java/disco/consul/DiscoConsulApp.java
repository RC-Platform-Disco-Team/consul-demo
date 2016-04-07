package disco.consul;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.ImmutableServiceHealth;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.util.LeaderElectionUtil;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Maxim Neverov
 */
@Controller
@RequestMapping("/proxy")
public class DiscoConsulApp {

    private static final String serviceName = "disco-consul-service";
    private static final int RETRY_NUM = 3;
    private static final long PAUSE_BETWEEN_RETRY_MILLIS = 1000;
    private static final String SERVICE_INFO = "{\"name\": \"" + serviceName + "\",\"address\": \"127.0.0.1:9000\"}";


    Consul consul;
    AgentClient agentClient;
    KeyValueClient keyValueClient;
    SessionClient sessionClient;
    HealthClient healthClient;
    LeaderElectionUtil leUtil;
    private Map<String, ServiceHealthCache> listeners = new HashMap<>();
    ServiceHealthCache svHealth;


    public DiscoConsulApp() throws Exception {
        // connect to Consul on localhost
        consul = Consul.builder().build();

        // build clients
        this.agentClient = consul.agentClient();
        leUtil = new LeaderElectionUtil(consul);
        this.keyValueClient = consul.keyValueClient();
        this.sessionClient = consul.sessionClient();
        healthClient = consul.healthClient();

        // register service
        String serviceId = "disco-consul-service-id-01";
        agentClient.register(9000, new URL("http://127.0.0.1:9000/proxy/health"), 10L, serviceName, serviceId);

        // leader election
        chooseLeader();

        svHealth = ServiceHealthCache.newCache(healthClient, serviceName);
        addHealthCheckListener();
        svHealth.start();
    }

    private void addHealthCheckListener() {
        svHealth.addListener(newValues -> {
            for (ServiceHealth serviceHealth : newValues.values()) {
                for (HealthCheck healthCheck : serviceHealth.getChecks()) {
                    if ("critical".equals(healthCheck.getStatus())) {
                        try {
                            // TODO: retry if release failed
                            if (leUtil.releaseLockForService(serviceName)) {
                                chooseLeader();
                            }
                        } catch (Exception ignored) {
                            ignored.printStackTrace();
                        }
                    }
                }
            }
        });
    }

    @RequestMapping("/health")
    public @ResponseBody Response healthCheck() {
        return Response.ok().build();
    }

    @RequestMapping("/electLeader")
    public @ResponseBody String chooseLeader() throws Exception {
        for (int i = 0; i < RETRY_NUM; i++) {
            Optional<String> result = leUtil.electNewLeaderForService(serviceName, SERVICE_INFO);
            if (result.isPresent()) {
                return result.get();
            } else {
                try {
                    Thread.sleep((long) (PAUSE_BETWEEN_RETRY_MILLIS * Math.pow(2, i)));
                } catch (InterruptedException ignored) {
                    ignored.printStackTrace();
                }
            }
        }
        throw new IllegalStateException("Leader election failed. Please check the consul agent.");
    }

    @RequestMapping("/releaseLock")
    public @ResponseBody boolean releaseLock() {
        return leUtil.releaseLockForService(serviceName);
    }

    @RequestMapping("/leader")
    public @ResponseBody String getLeader() {
        return leUtil.getLeaderInfoForService(serviceName).get();
    }

    @RequestMapping("/sessions")
    public @ResponseBody String getSessions() {
        return sessionClient.listSessions().toString();
    }

    @RequestMapping("/HealthyServiceInstance")
    public @ResponseBody String getHealthyServiceInstances() {
        return healthClient.getHealthyServiceInstances(serviceName).getResponse().toString();
    }

}
