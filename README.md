Metrics Cluster Aggregator
==========================

Combines aggregate values from multiple TSD Aggregators into an aggregate.  Simply, this means combining the values from your fleet.


Setup
-----

### Building ###

Prerequisites:
* [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

Building:

    metrics-cluster-aggregator> ./mvnw install

### Installing ###

The artifacts from the build are in *metrics-cluster-aggregator/target/appassembler* and should be copied to an appropriate directory on your application host(s).

### Execution ###

In the installation's *bin* directory there are scripts to start Metrics Cluster Aggregator: *metrics-cluster-aggregator* (Linux) and *metrics-cluster-aggregator.bat* (Windows).  One of these should be executed on system start with appropriate parameters; for example:

    /usr/local/lib/metrics-cluster-aggregator/bin/metrics-cluster-aggregator /usr/local/lib/metrics-cluster-aggregator/config/config.json

### Configuration ###

#### Logging ####

To customize logging you may provide a [LogBack](http://logback.qos.ch/) configuration file.  To use a custom logging configuration you need to define and export an environment variable before executing *cluster-aggregator*:

    export JAVA_OPTS="-Dlogback.configurationFile=/usr/local/lib/metrics-cluster-aggregator/config/logger.xml"

Where */usr/local/lib/metrics-cluster-aggregator/config/logger.xml* is the path to your logging configuration file.

#### Daemon ####

The Metrics Cluster Aggregator daemon configuration is specified in a JSON file.  The location of the configuration file is passed to *metrics-cluster-aggregator* as a command line argument:

    /usr/local/etc/metrics-cluster-aggregator/config/prod.conf

The configuration specifies:

* logDirectory - The location of additional logs.  This is independent of the logging configuration.
* pipelineConfiguration - The location of configuration file for the metrics pipeline.
* httpHost - The ip address to bind the http server to.
* httpPort - The port to bind the http server to.
* aggregationHost - The ip address to bind the tcp aggregation server to.
* aggregationPort - The port to bind the tcp aggregation server to.
* maxConnectionTimeout - The maximum aggregation server client connection timeout in ISO-8601 period notation.
* minConnectionTimeout - The minimum aggregation server client connection timeout in ISO-8601 period notation.
* jvmMetricsCollectionInterval - The JVM metrics collection interval in ISO-8601 period notation.
* rebalanceConfiguration - Configuration for aggregator shard rebalancing.
* akkaConfiguration - Configuration of Akka.

For example:

```json
{
  "logDirectory": "/usr/local/lib/metrics-cluster-aggregator/logs",
  "clusterPipelineConfiguration": "/usr/local/lib/metrics-cluster-aggregator/config/cluster-pipeline.json",
  "hostPipelineConfiguration": "/usr/local/lib/metrics-cluster-aggregator/config/host-pipeline.json",
  "httpPort": 7066,
  "httpHost": "0.0.0.0",
  "aggregationHost": "0.0.0.0",
  "aggregationPort": 7065,
  "maxConnectionTimeout": "PT2M",
  "minConnectionTimeout": "PT1M",
  "jvmMetricsCollectionInterval": "PT.5S",
  "rebalanceConfiguration": {
    "maxParallel": 100,
    "threshold": 500
  },
  "akkaConfiguration": {
    "akka": {
      "loggers": ["akka.event.slf4j.Slf4jLogger"],
      "loglevel": "DEBUG",
      "stdout-loglevel": "DEBUG",
      "logging-filter": "akka.event.slf4j.Slf4jLoggingFilter",
      "actor": {
        "debug": {
          "unhandled": "on"
        },
        "provider": "akka.cluster.ClusterActorRefProvider",
        "serializers": {
          "loggingJava": "com.arpnetworking.akka.LoggingSerializer"
        },
        "serialization-bindings" : {
          "\"com.arpnetworking.tsdcore.model.AggregatedData\"": "loggingJava"
        }
      },
      "cluster": {
        "seed-nodes": [
          "akka.tcp://Metrics@127.0.0.1:2551"
        ],
        "auto-down-unreachable-after": "300s"
      },
      "remote": {
        "log-remote-lifecycle-events": "on",
        "netty": {
          "tcp": {
            "hostname": "127.0.0.1",
            "port": 2551
          }
        }
      },
      "contrib": {
        "cluster": {
          "sharding": {
            "guardian-name": "sharding",
            "role": "",
            "retry-interval": "2 s",
            "buffer-size": 100000,
            "handoff-timeout": "60 s",
            "rebalance-interval": "10 s",
            "snapshot-interval": "720 s",
            "least-shard-allocation-strategy": {
              "rebalance-threshold": 10,
              "max-simultaneous-rebalance": 3
            }
          }
        }
      }
    }
  }
}
```

#### Pipeline ####

Metrics Cluster Aggregator supports a two pipelines.  The first is the host pipeline which handles publication of all statistics received from Metrics Aggregator Daemon instances.  The second is the cluster pipeline which handles all statistics (re)aggregated by cluster across host statistics from Metrics Aggregator Daemon instances.  In both cases the pipeline defines one more destinations or sinks for the statistics.

For example:

```json
{
    "sinks":
    [
        {
            "type": "com.arpnetworking.tsdcore.sinks.CarbonSink",
            "name": "my_application_carbon_sink",
            "serverAddress": "192.168.0.1"
        }
    ]
}
```

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Groupon Inc., 2014
