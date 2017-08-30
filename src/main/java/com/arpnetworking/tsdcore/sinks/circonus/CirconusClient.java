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
package com.arpnetworking.tsdcore.sinks.circonus;

import akka.http.javadsl.model.HttpMethods;
import akka.stream.Materializer;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.sinks.circonus.api.BrokerListResponse;
import com.arpnetworking.tsdcore.sinks.circonus.api.CheckBundle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.oval.constraint.NotNull;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import play.libs.ws.WSRequest;
import play.libs.ws.WSResponse;
import play.libs.ws.ahc.AhcWSClient;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.xml.ws.WebServiceException;

/**
 * Async Circonus API client.  Hides the implementation of the HTTP calls.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class CirconusClient {

    /**
     * Gets the list of brokers from the Circonus API.
     *
     * @return Future with the results.
     */
    public CompletionStage<BrokerListResponse> getBrokers() {
        final WSRequest request = _client
                .url(_uri + BROKERS_URL)
                .setMethod(HttpMethods.GET.value());
        LOGGER.trace()
                .setMessage("Sending get broker request")
                .log();
        return fireRequest(request)
                .thenApply(response -> handleBrokerListResponse(request, response));
    }

    /**
     * Sends a request to store data to an http trap monitor.
     * NOTE: This method is a bit odd to have on the client.  The URI of the httptrap is likely not on the
     * API broker host at all.  However, it doesn't make sense to move this method to another utility
     * class at this time.
     *
     * TODO(vkoskela): This method needs to be renamed since it is used only to send metrics. [ISSUE-?]
     * Furthermore, the method should probably return the deserialized response instead of the ws response.
     *
     * @param data Json data to store.
     * @param httptrapURI Url of the httptrap.
     * @return Future response.
     */
    public CompletionStage<WSResponse> sendToHttpTrap(final Map<String, Object> data, final URI httptrapURI) {
        try {
            LOGGER.trace()
                    .setMessage("Sending data to httptrap")
                    .log();
            final WSRequest request = _client
                    .url(httptrapURI.toString())
                    .setMethod("POST")
                    .setBody(OBJECT_MAPPER.writeValueAsString(data));
            return fireRequest(
                    request)
                    .thenApply(response -> handleMetricResponse(request, response));
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets or creates a check bundle in Circonus.
     *
     * @param request Request details.
     * @return Future with the results.
     */
    public CompletionStage<CheckBundle> getOrCreateCheckBundle(final CheckBundle request) {
        final String responseBody;
        try {
            responseBody = OBJECT_MAPPER.writeValueAsString(request);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        final WSRequest requestHolder = _client
                .url(_uri + CHECK_BUNDLE_URL)
                .setQueryParameter("dedupe_params", "display_name,target")
                .setMethod("POST")
                .setBody(responseBody);
        LOGGER.trace()
                .setMessage("Looking up check bundle")
                .addData("cid", request.getCid())
                .log();
        return fireRequest(requestHolder)
                .thenApply(response -> handleCheckBundleResponse(requestHolder, response, "create"));
    }

    /**
     * Gets a check bundle in Circonus.
     *
     * @param cid The check bundle's cid.
     * @return Future with the results.
     */
    public CompletionStage<CheckBundle> getCheckBundle(final String cid) {
        final WSRequest requestHolder = _client
                .url(_uri + cid)
                .setMethod("GET")
                .setQueryParameter("query_broker", "1");
        LOGGER.trace()
                .setMessage("Getting check bundle")
                .addData("cid", cid)
                .log();
        return fireRequest(requestHolder)
                .thenApply(response -> handleCheckBundleResponse(requestHolder, response, "get"));
    }


    /**
     * Updates a check bundle in Circonus.
     *
     * @param request Request details.
     * @return Future with the results.
     */
    public CompletionStage<CheckBundle> updateCheckBundle(final CheckBundle request) {
        final String responseBody;
        try {
            responseBody = OBJECT_MAPPER.writeValueAsString(request);
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        final WSRequest requestHolder = _client
                .url(_uri + request.getCid())
                .setMethod("PUT")
                .setBody(responseBody);
        LOGGER.trace()
                .setMessage("Updating check bundle")
                .addData("cid", request.getCid())
                .log();
        return fireRequest(requestHolder)
                .thenApply(response -> handleCheckBundleResponse(requestHolder, response, "updating"));
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("uri", _uri)
                .put("appName", _appName)
                .put("authToken", _authToken)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private static BrokerListResponse handleBrokerListResponse(
            final WSRequest request,
            final WSResponse response) {
        final String body = response.getBody();
        LOGGER.trace()
                .setMessage("Response from get brokers")
                .addData("response", response)
                .addData("body", body)
                .log();
        if (response.getStatus() / 100 == 2) {
            final List<BrokerListResponse.Broker> brokers;
            try {
                brokers = OBJECT_MAPPER.readValue(
                        body,
                        BROKER_LIST_TYPE_REFERENCE);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return new BrokerListResponse(brokers);
        }
        throw new WebServiceException(
                String.format(
                        "Received non 200 response getting broker list; request=%s, response=%s, responseBody=%s",
                        request,
                        response,
                        response.getBody()));
    }

    private static WSResponse handleMetricResponse(
            final WSRequest request,
            final WSResponse response) {
        final String body = response.getBody();
        LOGGER.trace()
                .setMessage("Response from posting metric data")
                .addData("response", response)
                .addData("body", body)
                .log();
        if (response.getStatus() / 100 == 2) {
            return response;
        }
        throw new WebServiceException(
                String.format(
                        "Received non 200 response posting metric data; request=%s, response=%s, responseBody=%s",
                        request,
                        response,
                        response.getBody()));
    }

    private static CheckBundle handleCheckBundleResponse(
            final WSRequest request,
            final WSResponse response,
            final String operation) {
        final String body = response.getBody();
        LOGGER.trace()
                .setMessage("Response from " + operation + " checkbundle")
                .addData("response", response)
                .addData("body", body)
                .log();
        if (response.getStatus() / 100 == 2) {
            try {
                return OBJECT_MAPPER.readValue(body, CheckBundle.class);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        throw new WebServiceException(
                String.format(
                        "Received non 200 response " + operation + " checkbundle; request=%s, response=%s, responseBody=%s",
                        request,
                        response,
                        response.getBody()));
    }

    private CompletionStage<WSResponse> fireRequest(final WSRequest request) {
        return request
                .setHeader("X-Circonus-Auth-Token", _authToken)
                .setHeader("X-Circonus-App-Name", _appName)
                .setHeader("Accept", "application/json")
                .execute();
    }

    private CirconusClient(final Builder builder) {
        _uri = builder._uri;
        _appName = builder._appName;
        _authToken = builder._authToken;
        _materializer = builder._materializer;

        final DefaultAsyncHttpClientConfig.Builder clientBuilder = new DefaultAsyncHttpClientConfig.Builder();
        if (!builder._safeHttps.booleanValue()) {
            clientBuilder.setAcceptAnyCertificate(true);
        }
        _client = new AhcWSClient(clientBuilder.build(), _materializer);
    }

    private final AhcWSClient _client;
    private final URI _uri;
    private final String _appName;
    private final String _authToken;
    private final Materializer _materializer;

    private static final String BROKERS_URL = "/v2/broker";
    private static final String CHECK_BUNDLE_URL = "/v2/check_bundle";
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
    private static final TypeReference<List<BrokerListResponse.Broker>> BROKER_LIST_TYPE_REFERENCE = new ListBrokerTypeReference();
    private static final HostnameVerifier HOST_NAME_VERIFIER = new UnsafeHostnameVerifier();
    private static final Logger LOGGER = LoggerFactory.getLogger(CirconusClient.class);

    /**
     * Implementation of the builder pattern for a {@link com.arpnetworking.tsdcore.sinks.circonus.CirconusClient}.
     */
    public static final class Builder extends OvalBuilder<CirconusClient> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(CirconusClient::new);
        }

        /**
         * Sets the base URI.
         *
         * @param value the base URI
         * @return this Builder
         */
        public Builder setUri(final URI value) {
            _uri = value;
            return this;
        }

        /**
         * Sets the application name.
         *
         * @param value the name of the application
         * @return this Builder
         */
        public Builder setAppName(final String value) {
            _appName = value;
            return this;
        }

        /**
         * Sets the authentication token.
         *
         * @param value the authentication token
         * @return this Builder
         */
        public Builder setAuthToken(final String value) {
            _authToken = value;
            return this;
        }

        /**
         * Sets the materializer for callbacks.
         *
         * @param value the execution context for callbacks
         * @return this Builder
         */
        public Builder setMaterializer(final Materializer value) {
            _materializer = value;
            return this;
        }

        /**
         * Sets the safety of HTTPS. Optional. Default is true. Setting this to false
         * will accept any certificate and disables the hostname verifier. You may also
         * need to supply the "-Djsse.enableSNIExtension=false" JVM argument to disable
         * SNI.
         *
         * @param value the authentication token
         * @return this Builder
         */
        public Builder setSafeHttps(final Boolean value) {
            _safeHttps = value;
            return this;
        }

        @NotNull
        private URI _uri;
        @NotNull
        private String _appName;
        @NotNull
        private String _authToken;
        @NotNull
        private Materializer _materializer;
        @NotNull
        private Boolean _safeHttps = true;
    }

    private static final class ListBrokerTypeReference extends TypeReference<List<BrokerListResponse.Broker>> { }

    private static final class UnsafeHostnameVerifier implements HostnameVerifier {
        @Override
        public boolean verify(final String s, final SSLSession sslSession) {
            return true;
        }
    }
}
