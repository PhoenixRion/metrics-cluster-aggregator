getent group cagg >/dev/null || groupadd -r cagg
getent passwd cagg >/dev/null || \
    useradd -r -g cagg -d /opt/cluster-aggregator -s /sbin/nologin \
    -c "Account used for isolation of metrics cluster aggregator" cagg
