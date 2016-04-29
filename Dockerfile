# Copyright 2016 Smartsheet.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM java:8u77-jre-alpine
RUN apk -U add libstdc++
MAINTAINER arpnetworking
EXPOSE 7065
EXPOSE 7066
WORKDIR /opt/cluster-aggregator
ENV CONFIG_FILE /opt/cluster-aggregator/config/config.json
ENV PARAMS $CONFIG_FILE
ENV LOGGING_CONFIG -Dlogback.configurationFile=/opt/cluster-aggregator/config/logback.xml
ENV JAVA_OPTS $LOGGING_CONFIG
RUN mkdir -p /opt/cluster-aggregator/logs
RUN mkdir -p /opt/cluster-aggregator/data
ADD config /opt/cluster-aggregator/config
CMD /opt/cluster-aggregator/bin/cluster-aggregator $PARAMS
ADD target/appassembler /opt/cluster-aggregator
