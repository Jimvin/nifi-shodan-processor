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

import com.fooock.shodan.ShodanRestApi;
import com.fooock.shodan.model.banner.Banner;
import com.fooock.shodan.model.banner.BannerReport;
import com.fooock.shodan.model.host.HostReport;
import io.reactivex.Observable;
import io.reactivex.observers.DisposableObserver;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"shodan", "json"})
@CapabilityDescription("Reads from Shodan REST API")
@WritesAttributes({@WritesAttribute(attribute="mime.type", description="Sets mime type to application/json")})
public class ShodanRestProcessor extends AbstractProcessor {

    public static final PropertyDescriptor SHODAN_API_KEY = new PropertyDescriptor.Builder()
            .name("Shodan API Key")
            .description("The API Key to access the Shodan REST API")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Shodan JSON records will be sent ot this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile ShodanRestApi client;
    private volatile Observable<BannerReport> reporter;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SHODAN_API_KEY);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
//        final String apiKey = context.getProperty(SHODAN_API_KEY).getValue();
//        ShodanRestApi client = new ShodanRestApi(apiKey);
//        this.client = client;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        // ToDo: Get the query string and facets
        // ToDo: add page number to search to collect all of the available data
        String query = "port:2181";
        final String apiKey = context.getProperty(SHODAN_API_KEY).getValue();

        new ShodanRestApi(apiKey).hostSearch(query)
            .subscribe(new DisposableObserver<HostReport>() {
            @Override
            public void onNext(HostReport hostReport) {
                int total = hostReport.getTotal();
                // ToDo: set record count output attribute
                getLogger().info("Found " + total + " records\n");
                if (total > 0) {
                    FlowFile flowFile = session.get();

                    for (Banner banner : hostReport.getBanners()) {
                        flowFile = session.write(flowFile, out -> out.write(banner.toString().getBytes(StandardCharsets.UTF_8)));
                    }

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                    attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.transfer(flowFile, REL_SUCCESS);
                    //session.getProvenanceReporter().receive(flowFile, Constants.STREAM_HOST + client.getEndpoint().getURI());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                getLogger().error("Processor error: " + throwable);
            }

            @Override
            public void onComplete() {
                getLogger().info("onTrigger subscribe complete");
            }
        });

    }
}
