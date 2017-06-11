package com.lab.loadbalancer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.netflix.ribbon.RibbonClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.lab.models.View;

@SpringBootApplication
@RestController
@RibbonClient(name = "loadbalancer", configuration = LoadBalancerConfiguration.class)
public class LoadbalancerApplication {

    private static final String GET_VIEWS_COUNT_METRIC = "view-request-loadbalancer";

    @Value("${graphite.hostname}")
    private String graphiteHostName;

    @Value("${graphite.port}")
    private int graphitePort;

    private static final Logger logger = LoggerFactory.getLogger(LoadbalancerApplication.class);
    private MetricRegistry metrics;

    @LoadBalanced
    @Bean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Autowired
    RestTemplate restTemplate;

    @RequestMapping(value = "view", method = RequestMethod.POST)
    public String saveRequest(@RequestBody View view) {
        Meter meter = metrics.meter(GET_VIEWS_COUNT_METRIC);
        meter.mark();

        String response = this.restTemplate.postForObject("http://backend/view", view, String.class);
        return response;
    }

    public static void main(String[] args) {
        SpringApplication.run(LoadbalancerApplication.class, args);
    }

    @PostConstruct
    public void init() throws IOException {
        metrics = new MetricRegistry();

         final Graphite graphite = new Graphite(new
         InetSocketAddress(graphiteHostName, graphitePort));
         final GraphiteReporter reporter =
         GraphiteReporter.forRegistry(metrics)
             .prefixedWith("Metrics")
             .convertRatesTo(TimeUnit.SECONDS)
             .convertDurationsTo(TimeUnit.MILLISECONDS)
             .filter(MetricFilter.ALL)
             .build(graphite);
         reporter.start(1, TimeUnit.SECONDS);
    }

}
