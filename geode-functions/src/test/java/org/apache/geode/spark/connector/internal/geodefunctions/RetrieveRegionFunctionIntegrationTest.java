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

public class RetrieveRegionFunctionIntegrationTest {

    @Before
    public void setUp() throws Exception {
        setupGemFireEmbeddedServer();
        createData();
    }

    @Test
    public void retrieveRegionFunctionShouldRetrieveFullRegion() {
        String result = null;
        String[] args = new String[2];
        args[0] = "";
        args[1] = "";
        Region partitionRegion = getTestRegion();
        ResultCollector rc = executeFunction(args, partitionRegion);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            byte[] bytes = (byte[]) l.get(i);
            result = new String(bytes);
        }
        Assert.assertThat("Region Result error", result, CoreMatchers.containsString("john"));
        Assert.assertThat("Region Result error", result, CoreMatchers.containsString("bill"));
    }

    @Test
    public void retrieveRegionFunctionShouldRetrieveWhereClauseRegion() {
        String result = null;
        String[] args = new String[2];
        args[0] = "key='1'";
        args[1] = "";
        Region partitionRegion = getTestRegion();
        ResultCollector rc = executeFunction(args, partitionRegion);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            byte[] bytes = (byte[]) l.get(i);
            result = new String(bytes);
        }
        Assert.assertThat("Where Result error", result, CoreMatchers.containsString("john"));
        Assert.assertThat("Where Result error", result, CoreMatchers.not(CoreMatchers.containsString("bill")));
    }

    @Test
    public void retrieveRegionFunctionShouldRetrieveEmptyWhereClauseUnsatisfied() {
        String result = null;
        byte[] bytes = null;
        String[] args = new String[2];
        args[0] = "key='100'";
        args[1] = "";
        Region partitionRegion = getTestRegion();
        ResultCollector rc = executeFunction(args, partitionRegion);
        List l = (ArrayList) rc.getResult();
        for (int i = 0; i < l.size(); i++) {
            bytes = (byte[]) l.get(i);
            result = new String(bytes);
        }
        Assert.assertEquals("Size Result error", 0, bytes.length);
        Assert.assertThat("Where Result error", result, CoreMatchers.not(CoreMatchers.containsString("john")));
        Assert.assertThat("Where Result error", result, CoreMatchers.not(CoreMatchers.containsString("bill")));
    }

    @After
    public void closeCacheAfterThisTest() {
        CacheFactory.getAnyInstance().close();
    }

    public void setupGemFireEmbeddedServer() throws IOException {
        CacheFactory cf = new CacheFactory();
        cf.setPdxReadSerialized(true);
        Cache c = cf.create();
        CacheServer cs = c.addCacheServer();
        cs.start();

        RegionFactory<?, ?> prf = c.createRegionFactory(RegionShortcut.PARTITION);
        prf.create("test");

        RetrieveRegionFunction retrieveRegionFunction = new RetrieveRegionFunction();
        FunctionService.registerFunction(retrieveRegionFunction);
    }

    private void createData() {
        Region partitionTestRegion = getTestRegion();

        Map customer = new HashMap();
        customer.put("id", "1");
        customer.put("name", "john");
        customer.put("age", "8");
        partitionTestRegion.put("1", customer);

        customer = new HashMap();
        customer.put("id", "2");
        customer.put("name", "bill");
        customer.put("age", "10");
        partitionTestRegion.put("2", customer);

    }

    private ResultCollector executeFunction(String[] args, Region region) {
        RetrieveRegionFunction retrieveRegionFunction = new RetrieveRegionFunction();
        Execution execution = FunctionService.onRegion(region).setArguments(args);
        return execution.execute(retrieveRegionFunction);
    }

    private Region getTestRegion() {
        return CacheFactory.getAnyInstance().getRegion("/test");
    }
}