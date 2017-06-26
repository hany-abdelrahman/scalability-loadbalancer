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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            createHBaseTables(connection);
            fillHBaseTables(connection);
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void createHBaseTables(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("views_total_count"));
        tableDescriptor.addFamily(new HColumnDescriptor("item"));
        admin.createTable(tableDescriptor);
    }

    public void fillHBaseTables(Connection conn) throws IOException {
        final int max_video_id = 1000000;
        Random generator = new Random();
        TableName tableName = TableName.valueOf("total_views");
        Table table = conn.getTable(tableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("views_total_count"));
        tableDescriptor.addFamily(new HColumnDescriptor("item"));
        List<Put> rows = new ArrayList<Put>();
        for (int i = 0; i < max_video_id; ++i) {
            Put record = new Put(Bytes.toBytes(i));
            int total_views = generator.nextInt(10000000);
            record.addColumn(Bytes.toBytes("item"), Bytes.toBytes("item_id"), Bytes.toBytes(i));
            record.addColumn(Bytes.toBytes("item"), Bytes.toBytes("views_count"), Bytes.toBytes(total_views));

            rows.add(record);
        }
        table.put(rows);
        table.close();
    }

}
