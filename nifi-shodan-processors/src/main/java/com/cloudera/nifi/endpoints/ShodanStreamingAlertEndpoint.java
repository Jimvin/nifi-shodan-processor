package com.cloudera.nifi.endpoints;

import com.google.common.base.Preconditions;
import com.twitter.hbc.core.HttpConstants;
import com.twitter.hbc.core.endpoint.BaseEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ShodanStreamingAlertEndpoint extends BaseEndpoint implements StreamingEndpoint {
    protected final String alertId;
    protected final ConcurrentMap<String, String> queryParameters;

    public ShodanStreamingAlertEndpoint(String alertId) {
        super("/shodan/alert/" + alertId, HttpConstants.HTTP_GET);
        this.alertId = Preconditions.checkNotNull(alertId);
        this.queryParameters = new ConcurrentHashMap<>();
    }

    @Override
    public String getPath(String apiVersion) {
        return this.path;
    }

    @Override
    public void setBackfillCount(int count) { }

}
