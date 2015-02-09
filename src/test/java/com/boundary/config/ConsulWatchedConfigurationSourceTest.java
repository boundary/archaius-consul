package com.boundary.config;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.io.BaseEncoding.base64;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
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
    public void testStartup() throws Exception {


        Response<List<GetValue>> expected = randomListGetValueResponse();

        when(client.getKVValues(rootPath, QueryParams.DEFAULT)).thenReturn(expected);
        assertThat(configSource.getCurrentData().size(), is(0));

        configSource.startAsync();
        configSource.awaitRunning();

        Map<String, Object> currentData = configSource.getCurrentData();

        for(GetValue gv: expected.getValue()) {
            assertThat(decodeVal(gv.getValue()), endsWith((String) currentData.get(decodeKey(gv.getKey()))));
        }


    }

    private String decodeKey(String key) {
        return key.substring(rootPath.length() + 1);
    }

    private String decodeVal(String value) {
        return new String(base64().decode(value)).trim();
    }


    @After
    public void tearDown() {
        configSource.stopAsync();
        configSource.awaitTerminated();
    }


    private Response<List<GetValue>> randomListGetValueResponse() {
        return new Response<>(randomListGetValue(), randomLong(), true, System.currentTimeMillis());
    }

    private List<GetValue> randomListGetValue() {

        List<GetValue> list = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
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

}