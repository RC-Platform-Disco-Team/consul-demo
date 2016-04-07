package disco.consul;

import java.math.BigInteger;
import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.State;
import com.orbitz.consul.model.health.HealthCheck;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.Session;
import com.orbitz.consul.option.QueryOptions;
import com.orbitz.consul.util.LeaderElectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Maxim Neverov
 */
@EnableConfigurationProperties
@Controller
@RequestMapping("/proxy")
public class DiscoConsulApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoConsulApp.class);
    private static boolean tryElectLeader = true;

    @org.springframework.beans.factory.annotation.Value("${service.name}")
    private String serviceName;

    @org.springframework.beans.factory.annotation.Value("${service.id}")
    private String serviceId;

    @org.springframework.beans.factory.annotation.Value("${service.info}")
    private String serviceInfo;

    @org.springframework.beans.factory.annotation.Value("${server.port}")
    private int port;

    @org.springframework.beans.factory.annotation.Value("${service.healthCheck.address}")
    private String serviceHealthCheckAddress;

    @org.springframework.beans.factory.annotation.Value("${leader.election.url}")
    private String leaderLockUrl;

    @org.springframework.beans.factory.annotation.Value("${leader.election.retry_number}")
    private int retryNumber;

    @org.springframework.beans.factory.annotation.Value("${leader.election.initial_time_interval_between_retry_millis}")
    private int retryInterval;

    @org.springframework.beans.factory.annotation.Value("${service.healthCheck.interval_seconds}")
    private long healthCheckIntervalSeconds;

    @org.springframework.beans.factory.annotation.Value("${leader.election.lock_acquire_block_minutes}")
    private int lockAcquireBlockMinutes;

    Consul consul;
    AgentClient agentClient;
    KeyValueClient keyValueClient;
    SessionClient sessionClient;
    HealthClient healthClient;
    LeaderElectionUtil leUtil;
    ServiceHealthCache svHealth;

    public DiscoConsulApp() {
        // connect to Consul on localhost:8500
        consul = Consul.builder().build();

        this.agentClient = consul.agentClient();
        this.leUtil = new LeaderElectionUtil(consul);
        this.keyValueClient = consul.keyValueClient();
        this.sessionClient = consul.sessionClient();
        this.healthClient = consul.healthClient();
    }

    @PostConstruct
    public void init() throws Exception {
        agentClient.register(port, new URL(serviceHealthCheckAddress), healthCheckIntervalSeconds, serviceName, serviceId);
        svHealth = ServiceHealthCache.newCache(healthClient, serviceName);
        addHealthCheckListener();
        svHealth.start();
    }

    private void addHealthCheckListener() {
        svHealth.addListener(newValues -> {
            boolean healthy = true;

            for (ServiceHealth serviceHealth : newValues.values()) {
                for (HealthCheck healthCheck : serviceHealth.getChecks()) {
                    if (State.FAIL.getName().equals(healthCheck.getStatus())) {
                        healthy = false;
                    }
                }
            }
            if (healthy && tryElectLeader) {
                chooseLeader();
                subscribeToLeaderChange();
                tryElectLeader = false;
            }
        });
    }

    private void subscribeToLeaderChange() {
        ConsulResponseCallback<Optional<Value>> callback = new ConsulResponseCallback<Optional<Value>>() {
            AtomicReference<BigInteger> index = new AtomicReference<>(null);

            @Override
            public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {
                Optional<Value> response = consulResponse.getResponse();
                if (!response.isPresent() ||
                        response.isPresent() && !response.get().getSession().isPresent()) {
                    releaseLock();
                    chooseLeader();
                }
                index.set(consulResponse.getIndex());
                watch();
            }

            void watch() {
                keyValueClient.getValue(leaderLockUrl,
                        QueryOptions.blockMinutes(lockAcquireBlockMinutes, index.get()).build(), this);
            }

            @Override
            public void onFailure(Throwable throwable) {
                LOGGER.info("leader change subscription failed", throwable);
                watch();
            }
        };
        keyValueClient.getValue(leaderLockUrl,
                QueryOptions.blockMinutes(lockAcquireBlockMinutes, new BigInteger("0")).build(), callback);
    }

    @RequestMapping("/health")
    public @ResponseBody Response healthCheck() {
        return Response.ok().build();
    }

    @RequestMapping("/electLeader")
    public @ResponseBody String chooseLeader() {
        for (int i = 0; i < retryNumber; i++) {
            Optional<String> result = electNewLeaderForService(serviceName, serviceInfo);
            if (result.isPresent()) {
                return result.get();
            } else {
                try {
                    Thread.sleep((long) (retryInterval * Math.pow(2, i)));
                } catch (InterruptedException ignored) {
                    LOGGER.info("Pause between leader election was interrupted", ignored);
                }
            }
        }
        throw new IllegalStateException("Leader election failed. Please check the consul agent.");
    }

    private Optional<String> electNewLeaderForService(final String serviceName, final String info) {
        final String key = getServiceKey(serviceName);
        String sessionId = createSession(serviceName);
        if(keyValueClient.acquireLock(key, info, sessionId)){
            return Optional.of(info);
        }else{
            return getLeaderInfoForService(serviceName);
        }
    }

    private Optional<String> getLeaderInfoForService(final String serviceName) {
        String key = getServiceKey(serviceName);
        Optional<Value> value = keyValueClient.getValue(key);
        if(value.isPresent()){
            if(value.get().getSession().isPresent()) {
                return value.get().getValueAsString();
            }
        }
        return Optional.absent();
    }

    private String createSession(String serviceName) {
        final Session session = ImmutableSession.builder()
                .name(serviceName)
                .behavior("delete")
                .addChecks("serfHealth", "service:" + serviceId)
                .build();
        return sessionClient.createSession(session).getId();
    }

    private static String getServiceKey(String serviceName) {
        return "service/" + serviceName + "/leader";
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
