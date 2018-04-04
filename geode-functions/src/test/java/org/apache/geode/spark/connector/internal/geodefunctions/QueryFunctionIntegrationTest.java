package org.apache.geode.spark.connector.internal.geodefunctions;

import org.apache.geode.cache.*;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryFunctionIntegrationTest {

    @Before
    public void setUp() throws Exception {
        setupGemFireEmbeddedServer();
        createData();
    }

    @After
    public void closeCacheAfterThisTest() {
        CacheFactory.getAnyInstance().close();
    }

    @Test
    public void queryFunctionShouldReturnNotNull() {
        String[] args = new String[2];
        args[0] = "select count(*) from /test";
        args[1] = "bucketSet";
        ResultCollector rc = executeFunction(args);
        Assert.assertNotNull(rc);
    }

    @Test
    public void queryFunctionShouldReturnEmptyWhenResultIsEmpty() {
        byte[] bytes = null;
        String[] args = new String[2];
        args[0] = "select * from /test.entries where key='bill'"; //object dne in cache
        args[1] = "bucketSet";
        ResultCollector rc = executeFunction(args);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            bytes = (byte[]) l.get(i);
        }
        Assert.assertEquals("Return length error", 0, bytes.length);
    }

    @Test
    public void queryFunctionShouldReturnEntireRow() {
        String result = null;
        String[] args = new String[2];
        String name = "john";
        String age = "8";
        args[0] = "select value from /test.entries where key='john'";
        args[1] = "bucketSet";
        ResultCollector rc = executeFunction(args);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            byte[] bytes = (byte[]) l.get(i);
            result = new String(bytes);
            System.out.println(result);
        }
        Assert.assertThat("Key Result error", result, CoreMatchers.containsString(name));
        Assert.assertThat("Key Result error", result, CoreMatchers.containsString(age));

    }

    @Test
    public void queryFunctionShouldReturnMultipleRow() {
        String result = null;
        String[] args = new String[2];
        args[0] = "select * from /test";
        args[1] = "bucketSet";
        ResultCollector rc = executeFunction(args);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            byte[] bytes = (byte[]) l.get(i);
            result = new String(bytes);
            System.out.println(result);
        }
        Assert.assertThat("Key Result error", result, CoreMatchers.containsString("john"));
        Assert.assertThat("Key Result error", result, CoreMatchers.containsString("george"));

    }

    @Test
    public void queryFunctionShouldReturnField() {
        String key = null;
        String expected = "john";
        String[] args = new String[2];
        args[0] = "select key from /test.entries where key='john'";
        args[1] = "bucketSet";
        ResultCollector rc = executeFunction(args);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            byte[] result = (byte[]) l.get(i);
            key = new String(result);
            System.out.println(key);
        }
        Assert.assertThat("Key Result error", key, CoreMatchers.containsString(expected));
    }

    public ResultCollector executeFunction(String[] args) {
        Region testRegion = CacheFactory.getAnyInstance().getRegion("/test");
        QueryFunction queryFunction = new QueryFunction();
        Execution execution = FunctionService.onRegion(testRegion).setArguments(args);
        return execution.execute(queryFunction);
    }

    private void createData() {
        Region testRegion = CacheFactory.getAnyInstance().getRegion("/test");
        Map customer = new HashMap();
        customer.put("name", "john");
        customer.put("age", "8");
        testRegion.put("john", customer);

        customer = new HashMap();
        customer.put("name", "george");
        customer.put("age", "9");
        testRegion.put("george", customer);
    }

    public void setupGemFireEmbeddedServer() throws IOException {
        CacheFactory cf = new CacheFactory();
        cf.setPdxReadSerialized(true);
        Cache c = cf.create();
        CacheServer cs = c.addCacheServer();
        cs.start();

        RegionFactory<?, ?> rf = c.createRegionFactory(RegionShortcut.PARTITION);
        rf.create("test");

        QueryFunction queryFunction = new QueryFunction();
        FunctionService.registerFunction(queryFunction);
    }
}