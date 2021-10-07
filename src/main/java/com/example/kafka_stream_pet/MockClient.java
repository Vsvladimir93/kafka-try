package com.example.kafka_stream_pet;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.StatsReporter;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;

public class MockClient implements Client {

    @Override
    public void connect() {

    }

    @Override
    public void reconnect() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void stop(int i) {

    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public StreamingEndpoint getEndpoint() {
        return null;
    }

    @Override
    public StatsReporter.StatsTracker getStatsTracker() {
        return null;
    }
}
