/**
 * Copyright 2014 Groupon.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.clusteraggregator.http;

import akka.actor.ActorSystem;
import akka.cluster.Member;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.CacheControl;
import akka.http.javadsl.model.headers.CacheDirectives;
import akka.japi.function.Function;
import akka.pattern.PatternsCS;
import akka.util.ByteString;
import akka.util.Timeout;
import com.arpnetworking.clusteraggregator.Status;
import com.arpnetworking.clusteraggregator.models.StatusResponse;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.configuration.jackson.akka.AkkaModule;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Timer;
import com.arpnetworking.steno.LogBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Http server routes.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class Routes implements Function<HttpRequest, CompletionStage<HttpResponse>> {

    /**
     * Public constructor.
     *
     * @param actorSystem Instance of <code>ActorSystem</code>.
     * @param metricsFactory Instance of <code>MetricsFactory</code>.
     * @param healthCheckPath The path for the health check.
     * @param statusPath The path for the status.
     */
    public Routes(
            final ActorSystem actorSystem,
            final MetricsFactory metricsFactory,
            final String healthCheckPath,
            final String statusPath) {
        _actorSystem = actorSystem;
        _metricsFactory = metricsFactory;
        _healthCheckPath = healthCheckPath;
        _statusPath = statusPath;

        _objectMapper = ObjectMapperFactory.createInstance();
        _objectMapper.registerModule(new SimpleModule().addSerializer(Member.class, new MemberSerializer()));
        _objectMapper.registerModule(new AkkaModule(actorSystem));
    }

    @Override
    public CompletionStage<HttpResponse> apply(final HttpRequest request) {
        final Metrics metrics = _metricsFactory.create();
        final Timer timer = metrics.createTimer(createTimerName(request));
        LOGGER.trace()
                .setEvent("http.in.start")
                .addData("method", request.method())
                .addData("url", request.getUri())
                .addData("headers", request.getHeaders())
                .log();
        return process(request).<HttpResponse>whenComplete(
                (response, failure) -> {
                    timer.close();
                    metrics.close();
                    final LogBuilder log = LOGGER.trace()
                            .setEvent("http.in")
                            .addData("method", request.method())
                            .addData("url", request.getUri())
                            .addData("status", response.status().intValue())
                            .addData("headers", request.getHeaders());
                    if (failure != null) {
                        log.setEvent("http.in.error").addData("exception", failure);
                    }
                    log.log();
                });
    }

    private CompletionStage<HttpResponse> process(final HttpRequest request) {
        if (HttpMethods.GET.equals(request.method())) {
            if (_healthCheckPath.equals(request.getUri().path())) {
                return ask("/user/status", new Status.HealthRequest(), Boolean.FALSE)
                        .thenApply(
                                isHealthy -> HttpResponse.create()
                                        .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                        .addHeader(PING_CACHE_CONTROL_HEADER)
                                        .withEntity(
                                                JSON_CONTENT_TYPE,
                                                ByteString.fromString(
                                                        "{\"status\":\""
                                                                + (isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE)
                                                                + "\"}")));
            } else if (_statusPath.equals(request.getUri().path())) {
                return ask("/user/status", new Status.StatusRequest(), (StatusResponse) null)
                        .thenApply(
                                status -> {
                                    try {
                                        return HttpResponse.create()
                                                .withEntity(
                                                        JSON_CONTENT_TYPE,
                                                        ByteString.fromString(_objectMapper.writeValueAsString(status)));
                                    } catch (final IOException e) {
                                        LOGGER.error()
                                                .setMessage("Failed to serialize status")
                                                .setThrowable(e)
                                                .log();
                                        return HttpResponse.create().withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
                                    }
                                });
            }
        }
        return CompletableFuture.completedFuture(HttpResponse.create().withStatus(404));
    }

    @SuppressWarnings("unchecked")
    private <T> CompletionStage<T> ask(final String actorPath, final Object request, final T defaultValue) {
        return
                PatternsCS.ask(
                        _actorSystem.actorSelection(actorPath),
                        request,
                        Timeout.apply(5, TimeUnit.SECONDS))
                .thenApply(o -> (T) o)
                .exceptionally(throwable -> defaultValue);
    }

    private String createTimerName(final HttpRequest request) {
        final StringBuilder nameBuilder = new StringBuilder()
                .append("rest_service/")
                .append(request.method().value());
        if (!request.getUri().path().startsWith("/")) {
            nameBuilder.append("/");
        }
        nameBuilder.append(request.getUri().path());
        return nameBuilder.toString();
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ActorSystem _actorSystem;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final MetricsFactory _metricsFactory;
    private final String _healthCheckPath;
    private final String _statusPath;
    private final ObjectMapper _objectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(Routes.class);

    // Ping
    private static final HttpHeader PING_CACHE_CONTROL_HEADER = CacheControl.create(
            CacheDirectives.PRIVATE(),
            CacheDirectives.NO_CACHE,
            CacheDirectives.NO_STORE,
            CacheDirectives.MUST_REVALIDATE);
    private static final String UNHEALTHY_STATE = "UNHEALTHY";
    private static final String HEALTHY_STATE = "HEALTHY";

    private static final ContentType JSON_CONTENT_TYPE = ContentTypes.APPLICATION_JSON;

    private static final long serialVersionUID = -1573473630801540757L;

    private static class MemberSerializer extends JsonSerializer<Member> {
        @Override
        public void serialize(
                final Member value,
                final JsonGenerator jgen,
                final SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeObjectField("address", value.address().toString());
            jgen.writeObjectField("roles", value.getRoles().toArray());
            jgen.writeEndObject();
        }
    }
}
