/sbin/chkconfig --add cluster-aggregator
mkdir /opt/cluster-aggregator/logs
chown cagg:cagg /opt/cluster-aggregator/logs
mkdir /var/run/cluster-aggregator
chown cagg:cagg /var/run/cluster-aggregator
mkdir /opt/cluster-aggregator/data
chown cagg:cagg /opt/cluster-aggregator/data
