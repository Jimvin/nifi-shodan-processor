/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.nifi.processors;

import com.cloudera.nifi.auth.NullAuth;
import com.cloudera.nifi.endpoints.ShodanStreamingBannerEndpoint;
import com.cloudera.nifi.endpoints.ShodanStreamingAlertEndpoint;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.LineStringProcessor;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"shodan", "json"})
@CapabilityDescription("Reads from Shodan streaming API")
@WritesAttributes({@WritesAttribute(attribute="mime.type", description="Sets mime type to application/json")})
public class ShodanStreamingProcessor extends AbstractProcessor {

//    static final AllowableValue ENDPOINT_REST = new AllowableValue("REST Endpoint", "REST Endpoint (WIP)", "The Shodan REST API endpoint");
    static final AllowableValue ENDPOINT_STREAMING_ALERT = new AllowableValue("Streaming Alert Endpoint", "Streaming Alert Endpoint", "The Shodan streaming alert endpoint");
    static final AllowableValue ENDPOINT_STREAMING_BANNER = new AllowableValue("Streaming Banner Endpoint", "Streaming Banner Endpoint", "The Shodan streaming banner (firehose) endpoint");

    public static final PropertyDescriptor SHODAN_API_KEY = new PropertyDescriptor.Builder()
            .name("Shodan API Key")
            .description("The API Key to access the Shodan REST API")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
            .name("Shodan Endpoint")
            .description("Specifies which endpoint data should be pulled from")
            .required(true)
            .allowableValues(ENDPOINT_STREAMING_ALERT, ENDPOINT_STREAMING_BANNER)
            .defaultValue(ENDPOINT_STREAMING_BANNER.getValue())
            .build();
    public static final PropertyDescriptor MAX_CLIENT_ERROR_RETRIES = new PropertyDescriptor.Builder()
            .name("max-client-error-retries")
            .displayName("Max Client Error Retries")
            .description("The maximum number of retries to attempt when client experience retryable connection errors."
                    + " Client continues attempting to reconnect using an exponential back-off pattern until it successfully reconnects"
                    + " or until it reaches the retry limit."
                    +"  It is recommended to raise this value when client is getting rate limited by Shodan API. Default value is 5.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();
    public static final PropertyDescriptor ALERT_ID = new PropertyDescriptor.Builder()
            .name("Shodan Alert ID")
            .description("The ID for the registered Shodan alert. Only relevant for the Shodan alert endpoint")
            .required(false)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Shodan JSON records will be sent ot this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

    private volatile ClientBuilder clientBuilder;
    private volatile Client client;
    private volatile BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(5000);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SHODAN_API_KEY);
        descriptors.add(ENDPOINT);
        descriptors.add(MAX_CLIENT_ERROR_RETRIES);
        descriptors.add(ALERT_ID);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // if any property is modified, the results are no longer valid. Destroy all messages in the queue.
        messageQueue.clear();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String endpointName = context.getProperty(ENDPOINT).getValue();
        final int maxRetries = context.getProperty(MAX_CLIENT_ERROR_RETRIES).asInteger();
        final String apiKey = context.getProperty(SHODAN_API_KEY).getValue();
        final String alertId = context.getProperty(ALERT_ID).getValue();

        final ClientBuilder clientBuilder = new ClientBuilder();
        clientBuilder.name("GetShodanAlerts[id=" + getIdentifier() + "]")
                .authentication(new NullAuth())
                .eventMessageQueue(eventQueue)
                .processor(new LineStringProcessor(messageQueue));

        final String host;
        final StreamingEndpoint streamingEndpoint;
        if (ENDPOINT_STREAMING_ALERT.getValue().equals(endpointName)) {
            host = "https://stream.shodan.io";
            final ShodanStreamingAlertEndpoint sse = new ShodanStreamingAlertEndpoint(alertId);
            sse.addQueryParameter("key", apiKey);
            streamingEndpoint = sse;
        } else if (ENDPOINT_STREAMING_BANNER.getValue().equals(endpointName)) {
            host = "https://stream.shodan.io";
            final ShodanStreamingBannerEndpoint sse = new ShodanStreamingBannerEndpoint();
            sse.addQueryParameter("key", apiKey);
            streamingEndpoint = sse;
        } else {
            throw new AssertionError("Endpoint was invalid value: " + endpointName);
        }

        clientBuilder.hosts(host).endpoint(streamingEndpoint);
        clientBuilder.retries(maxRetries);
        this.clientBuilder = clientBuilder;
    }

    public synchronized void connectNewClient() {
        if (client == null || client.isDone()) {
            client = clientBuilder.build();
            try {
                client.connect();
            } catch (Exception e) {
                client.stop();
            }
        }
    }

    @OnStopped
    public void shutdownClient() {
        if (client != null) {
            client.stop();
        }
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        if (newState == PrimaryNodeState.PRIMARY_NODE_REVOKED) {
            shutdownClient();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (client == null || client.isDone()) {
            connectNewClient();
            if (client.isDone()) {
                context.yield();
                return;
            }
        }

        final Event event = eventQueue.poll();
        if (event != null) {
            switch (event.getEventType()) {
                case STOPPED_BY_ERROR:
                    getLogger().error("Received error {}: {} due to {}. Will not attempt to reconnect", new Object[]{event.getEventType(), event.getMessage(), event.getUnderlyingException()});
                    break;
                case CONNECTION_ERROR:
                case HTTP_ERROR:
                    getLogger().error("Received error {}: {}. Will attempt to reconnect", new Object[]{event.getEventType(), event.getMessage()});
                    client.reconnect();
                    break;
                default:
                    break;
            }
        }

        final String alert = messageQueue.poll();
        if (alert == null) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, out -> out.write(alert.getBytes(StandardCharsets.UTF_8)));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
//        session.getProvenanceReporter().receive(flowFile, Constants.STREAM_HOST + client.getEndpoint().getURI());
    }
}
