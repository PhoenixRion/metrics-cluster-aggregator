if [ "$1" = 0 ]; then
  /sbin/service cluster-aggregator stop > /dev/null 2>&1
  /sbin/chkconfig --del cluster-aggregator
fi
exit 0
