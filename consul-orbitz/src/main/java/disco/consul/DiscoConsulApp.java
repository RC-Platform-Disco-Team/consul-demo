package disco.consul;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import javax.ws.rs.core.Response;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.async.ConsulResponseCallback;
import com.orbitz.consul.model.ConsulResponse;
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

    private static final int RETRY_NUM = 3;
    private static final long PAUSE_BETWEEN_RETRY_MILLIS = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoConsulApp.class);


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

    Consul consul;
    AgentClient agentClient;
    KeyValueClient keyValueClient;
    SessionClient sessionClient;
    HealthClient healthClient;
    LeaderElectionUtil leUtil;

    public DiscoConsulApp() {
        // connect to Consul on localhost:8500
        consul = Consul.builder().build();

        // build clients
        this.agentClient = consul.agentClient();
        this.leUtil = new LeaderElectionUtil(consul);
        this.keyValueClient = consul.keyValueClient();
        this.sessionClient = consul.sessionClient();
        this.healthClient = consul.healthClient();
    }

    @PostConstruct
    public void init() throws MalformedURLException {
        agentClient.register(port, new URL(serviceHealthCheckAddress), 10L, serviceName, serviceId);
        chooseLeader();
        subscribeToLeaderChange();
    }

    private void subscribeToLeaderChange() {
        ConsulResponseCallback<Optional<Value>> callback = new ConsulResponseCallback<Optional<Value>>() {
            AtomicReference<BigInteger> index = new AtomicReference<>(null);

            @Override
            public void onComplete(ConsulResponse<Optional<Value>> consulResponse) {
                Optional<Value> response = consulResponse.getResponse();
                if (!response.isPresent() ||
                        response.isPresent() && !response.get().getSession().isPresent()) {
                    LOGGER.info("leader was overthrown: " + new String(BaseEncoding.base64().decode(response.get().getValue().get())));
                    releaseLock();
                    chooseLeader();
                }
                index.set(consulResponse.getIndex());
                watch();
            }

            void watch() {
                keyValueClient.getValue("service/disco-consul-service/leader", QueryOptions.blockSeconds(30, index.get()).build(), this);
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
                watch();
            }
        };
        keyValueClient.getValue("service/disco-consul-service/leader", QueryOptions.blockSeconds(30, new BigInteger("0")).build(), callback);
    }

    @RequestMapping("/health")
    public @ResponseBody Response healthCheck() {
        return Response.ok().build();
    }

    @RequestMapping("/electLeader")
    public @ResponseBody String chooseLeader() {
        for (int i = 0; i < RETRY_NUM; i++) {
            Optional<String> result = electNewLeaderForService(serviceName, serviceInfo);
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
        final Session session = ImmutableSession.builder().name(serviceName).behavior("delete").build();
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
