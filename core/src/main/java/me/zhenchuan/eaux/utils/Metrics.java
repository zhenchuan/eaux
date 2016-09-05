package me.zhenchuan.eaux.utils;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Metrics {

    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    private static final MetricRegistry registry = new MetricRegistry();

    static {
        try {
            Configuration configuration = new PropertiesConfiguration("graphite.properties");
            String nodeName =  configuration.getString("graphite.prefix", null);
            String graphiteHost = configuration.getString("graphite.host", null);
            int graphitePort = configuration.getInt("graphite.port", 2003);
            int interval = configuration.getInt("graphite.poll.interval.seconds", 60);
            if(StringUtils.isNotEmpty(nodeName) && StringUtils.isNotEmpty(graphiteHost)){
                Graphite graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));

                GraphiteReporter reporter = GraphiteReporter
                        .forRegistry(registry)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .prefixedWith(nodeName)
                        .build(graphite);
                reporter.start(interval, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("failed to load graphite.properties");
        }

        final Slf4jReporter reporter = Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger("me.zhenchuan.Metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);


    }

    static class PrefixedMetricSet implements MetricSet {
        private final Map<String, Metric> metricMap;

        PrefixedMetricSet(final MetricSet metricSet, final String prefix1, final String prefix2) {
            metricMap = Collections.unmodifiableMap(new HashMap<String, Metric>(){{
                for (Map.Entry<String, Metric> stringMetricEntry : metricSet.getMetrics().entrySet()) {
                    put(MetricRegistry.name(prefix1, prefix2, stringMetricEntry.getKey()), stringMetricEntry.getValue());
                }
            }});
        }

        @Override
        public Map<String, Metric> getMetrics() {
            return metricMap;
        }
    }

    public static MetricRegistry getRegistry() {
        return registry;
    }

    public static Meter meter(Class kls, String... names) {
        return getRegistry().meter(MetricRegistry.name(kls, names));
    }

    public static Timer timer(Class kls, String... names) {
        return getRegistry().timer(MetricRegistry.name(kls, names));
    }

    public static Histogram histogram(Class kls, String... names) {
        return getRegistry().histogram(MetricRegistry.name(kls, names));
    }

    public static Counter counter(Class kls, String... names) {
        return getRegistry().counter(MetricRegistry.name(kls, names));
    }
}
