package com.boundary.config;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.google.common.collect.Lists;
import com.netflix.config.WatchedUpdateListener;
import com.netflix.config.WatchedUpdateResult;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.BaseEncoding.base64;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsulWatchedConfigurationSourceTest extends RandomizedTest {


    private KeyValueClient client = mock(KeyValueClient.class);
    private String rootPath = "my-app/config";
    private int watchInterval = 10;
    private ConsulWatchedConfigurationSource configSource;

    @Before
    public void setUp() {
        configSource = new ConsulWatchedConfigurationSource(rootPath, client, watchInterval);

    }

    @Test
    @Repeat(iterations = 5)
    public void testInitialRun() throws Exception {


        final Response<List<GetValue>> expected = randomListGetValueResponse();

        when(client.getKVValues(eq(rootPath), any(QueryParams.class))).thenReturn(expected);

        assertThat(configSource.getCurrentData(), nullValue());

        UpdateListener listener = new UpdateListener();
        configSource.addUpdateListener(listener);


        configSource.runOnce();

        Map<String, Object> currentData = configSource.getCurrentData();

        for(GetValue gv: expected.getValue()) {
            assertThat(decodeVal(gv.getValue()), endsWith((String) currentData.get(decodeKey(gv.getKey()))));
        }

        assertThat(configSource.getLatestIndex(), is(expected.getConsulIndex()));

        assertThat(listener.events.get(), is(1));
        assertThat(listener.results.get(0).getComplete(), is(currentData));
        assertThat(listener.results.get(0).isIncremental(), is(false));

    }

    @Test
    @Repeat(iterations = 5)
    public void testAdded() throws Exception {

        final Response<List<GetValue>> initialResponse = randomListGetValueResponse();
        final GetValue added = randomGetVal();
        final List<GetValue> updatedList = Lists.newArrayList(initialResponse.getValue());
        updatedList.add(added);
        final Response<List<GetValue>> initialResponsePlus = randomResponse(updatedList);
        //noinspection unchecked
        when(client.getKVValues(eq(rootPath), any(QueryParams.class))).thenReturn(initialResponse, initialResponsePlus);

        UpdateListener listener = new UpdateListener();
        configSource.addUpdateListener(listener);

        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponse.getConsulIndex()));

        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponsePlus.getConsulIndex()));
        assertThat(listener.events.get(), is(2));
        assertThat(configSource.getCurrentData().values(), hasItem(decodeVal(added.getValue())));

        WatchedUpdateResult result = listener.results.get(1);
        assertThat(result.getAdded().values(), hasItem(decodeVal(added.getValue())));
        assertThat(result.getDeleted().isEmpty(), is(true));
        assertThat(result.getChanged().isEmpty(), is(true));
        assertThat(result.isIncremental(), is(true));


    }


    @Test
    @Repeat(iterations = 5)
    public void testRemoved() throws Exception {

        final Response<List<GetValue>> initialResponse = randomListGetValueResponse();
        final GetValue removed = initialResponse.getValue().get(randomInt(initialResponse.getValue().size() -1));
        final List<GetValue> updatedList = Lists.newArrayList(initialResponse.getValue());
        updatedList.remove(removed);
        final Response<List<GetValue>> initialResponseMinus = randomResponse(updatedList);
        //noinspection unchecked
        when(client.getKVValues(eq(rootPath), any(QueryParams.class))).thenReturn(initialResponse, initialResponseMinus);

        UpdateListener listener = new UpdateListener();
        configSource.addUpdateListener(listener);


        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponse.getConsulIndex()));

        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponseMinus.getConsulIndex()));
        assertThat(listener.events.get(), is(2));
        assertThat(configSource.getCurrentData().values(), not(hasItem(decodeVal(removed.getValue()))));

        WatchedUpdateResult result = listener.results.get(1);
        assertThat(result.getDeleted().values(), hasItem(decodeVal(removed.getValue())));
        assertThat(result.getAdded().isEmpty(), is(true));
        assertThat(result.getChanged().isEmpty(), is(true));
        assertThat(result.isIncremental(), is(true));


    }


    @Test
    @Repeat(iterations = 5)
    public void testChanged() throws Exception {

        final Response<List<GetValue>> initialResponse = randomListGetValueResponse();

        final List<GetValue> updatedList = new ArrayList<>(initialResponse.getValue());
        final GetValue toChange = updatedList.remove(randomInt(updatedList.size() - 1));

        final GetValue changed = randomGetVal();
        changed.setKey(toChange.getKey());

        updatedList.add(changed);

        final Response<List<GetValue>> initialResponseChanged = randomResponse(updatedList);
        //noinspection unchecked
        when(client.getKVValues(eq(rootPath), any(QueryParams.class))).thenReturn(initialResponse, initialResponseChanged);

        UpdateListener listener = new UpdateListener();
        configSource.addUpdateListener(listener);


        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponse.getConsulIndex()));

        configSource.runOnce();

        assertThat(configSource.getLatestIndex(), is(initialResponseChanged.getConsulIndex()));
        assertThat(listener.events.get(), is(2));
        assertThat(configSource.getCurrentData().values(), hasItem(decodeVal(changed.getValue())));
        WatchedUpdateResult result = listener.results.get(1);

        assertThat(result.getChanged().values(), hasItem(decodeVal(changed.getValue())));
        assertThat(result.getAdded().isEmpty(), is(true));
        assertThat(result.getDeleted().isEmpty(), is(true));
        assertThat(result.isIncremental(), is(true));

    }
    private String decodeKey(String key) {
        return key.substring(rootPath.length() + 1);
    }

    private String decodeVal(String value) {
        return new String(base64().decode(value)).trim();
    }


    private Response<List<GetValue>> randomListGetValueResponse() {
        return randomResponse(randomListGetValue());
    }

    private <T> Response<T> randomResponse(T val) {
        return new Response<>(val, randomLong(), true, System.currentTimeMillis());
    }

    private List<GetValue> randomListGetValue() {
        List<GetValue> list = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            list.add(randomGetVal());
        }
        return list;
    }

    private GetValue randomGetVal() {
        final GetValue gv = new GetValue();
        gv.setCreateIndex(randomInt(100));
        gv.setKey( rootPath + "/" + randomAsciiOfLength(10));
        gv.setValue( randomAsciiOfLength(10));
        gv.setModifyIndex(randomInt(100));
        return gv;
    }

    private static class UpdateListener implements WatchedUpdateListener {

        CopyOnWriteArrayList<WatchedUpdateResult> results = new CopyOnWriteArrayList<>();
        AtomicInteger events = new AtomicInteger(0);

        @Override
        public void updateConfiguration(WatchedUpdateResult result) {
            results.add(result);
            events.incrementAndGet();
        }
    }

}