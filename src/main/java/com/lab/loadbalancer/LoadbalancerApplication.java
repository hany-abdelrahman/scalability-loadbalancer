package com.lab.loadbalancer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
    private static final String TOTAL_VIEWS_TABLE = "views_total_count";

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

    @RequestMapping(value = "count", method = RequestMethod.GET)
    public String countRequest(int itemId) {
        Meter meter = metrics.meter(GET_VIEWS_COUNT_METRIC);
        meter.mark();

        String response = this.restTemplate.getForObject("http://backend/count", String.class, itemId);
        return response;
    }

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
    public void initMetrics() throws IOException {
        metrics = new MetricRegistry();

        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostName, graphitePort));
        final GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                                                          .prefixedWith("Metrics")
                                                          .convertRatesTo(TimeUnit.SECONDS)
                                                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                          .filter(MetricFilter.ALL)
                                                          .build(graphite);
        reporter.start(1, TimeUnit.SECONDS);
    }

    @PostConstruct
    public void initHBase() throws IOException {
        createHBaseTables();
        fillHBaseTables();
    }

    public void createHBaseTables() throws IOException {
        final int REPLICATION_FACTOR = 3;
        HBaseAdmin admin = null;
        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TOTAL_VIEWS_TABLE));
            tableDescriptor.addFamily(new HColumnDescriptor("item"));
            tableDescriptor.setRegionReplication(REPLICATION_FACTOR);
            Configuration config = HBaseConfiguration.create();
            admin = new HBaseAdmin(config);
            if (!admin.tableExists(TOTAL_VIEWS_TABLE)) {
                admin.createTable(tableDescriptor);
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
        finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    public void fillHBaseTables() throws IOException  {
        final int BATCH_SIZE = 1000;
        final int max_video_id = 1000000;
        final int max_views_count = 10000000;
        Random generator = new Random();
        Configuration config = HBaseConfiguration.create();
        HTable hTable = new HTable(config, TOTAL_VIEWS_TABLE);
        try {
            List<Put> rows = new ArrayList<Put>(BATCH_SIZE);
            for (int i = 0; i < max_video_id; ++i) {
                Put record = new Put(Bytes.toBytes(Integer.toString(i)));
                int total_views = generator.nextInt(max_views_count);
                record.add(Bytes.toBytes("item"), Bytes.toBytes("item_id"), Bytes.toBytes(Integer.toString(i)));
                record.add(Bytes.toBytes("item"), Bytes.toBytes("views_count"), Bytes.toBytes(Integer.toString(total_views)));

                rows.add(record);
                if (i % BATCH_SIZE == 0) {
                    hTable.put(rows);
                    rows.clear();
                }
            }
            hTable.close();
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
