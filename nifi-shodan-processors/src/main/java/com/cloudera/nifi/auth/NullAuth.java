package com.cloudera.nifi.auth;

import com.twitter.hbc.httpclient.auth.Authentication;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.AbstractHttpClient;

// This class implements the twitter Authentication interface but does nothing
// Useful where the API endpoint does authentication by API key only
public class NullAuth implements Authentication {

    @Override
    public void setupConnection(AbstractHttpClient client) { }

    @Override
    public void signRequest(HttpUriRequest request, String postContent) { }
}
