package org.apache.geode.spark.connector.internal.geodefunctions;


import org.apache.geode.cache.*;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.spark.connector.internal.RegionMetadata;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RetrieveRegionMetadataFunctionIntegrationTest {
    @Before
    public void setUp() throws Exception {
        setupGemFireEmbeddedServer();
        createData();
    }

    /*Still should test bucket mapping*/

    @Test
    public void retrieveRegionMetaFunctionShouldRetrieveForPartition() {
        Region partitionRegion = CacheFactory.getAnyInstance().getRegion("/partitionRegion");
        RegionMetadata regionMetadata = executeFunction(partitionRegion);
        System.out.println(regionMetadata);
        Assert.assertEquals("Bucket Number Error", 113, regionMetadata.getTotalBuckets());
        Assert.assertTrue("Region Type Error", regionMetadata.isPartitioned());
        Assert.assertEquals("Key Type Error", "java.lang.String", regionMetadata.getKeyTypeName());
        Assert.assertEquals("Value Type Error", "java.util.Map", regionMetadata.getValueTypeName());
        Assert.assertEquals("Bucket Map Error", new HashMap<>(), regionMetadata.getServerBucketMap());
    }

    @Test
    public void retrieveRegionMetaFunctionShouldRetrieveForReplicate() {
        Region replicateRegion = CacheFactory.getAnyInstance().getRegion("/replicateRegion");
        RegionMetadata regionMetadata = executeFunction(replicateRegion);
        Assert.assertEquals("Bucket Number is Not 0", 0, regionMetadata.getTotalBuckets());
        Assert.assertFalse("Region Type Error", regionMetadata.isPartitioned());
        Assert.assertEquals("Key Type Error", null, regionMetadata.getKeyTypeName());
        Assert.assertEquals("Value Type Error", null, regionMetadata.getValueTypeName());
        Assert.assertNull("Bucket Map Error", regionMetadata.getServerBucketMap());
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
        Class constraint = String.class;
        Class mconstraint = Map.class;

        RegionFactory<String, Map> prf = c.createRegionFactory(RegionShortcut.PARTITION);
        prf.setKeyConstraint(constraint);
        prf.setValueConstraint(mconstraint);
        prf.create("partitionRegion");

        RegionFactory<?, ?> rrf = c.createRegionFactory(RegionShortcut.REPLICATE);
        rrf.create("replicateRegion");

        RetrieveRegionMetadataFunction retrieveRegionMetadataFunction = new RetrieveRegionMetadataFunction();
        FunctionService.registerFunction(retrieveRegionMetadataFunction);
    }

    private void createData() {
        Region partitionTestRegion = CacheFactory.getAnyInstance().getRegion("/partitionRegion");
        Region replicateTestRegion = CacheFactory.getAnyInstance().getRegion("/replicateRegion");

        Map customer = new HashMap();
        customer.put("id", "1");
        customer.put("name", "john");
        customer.put("age", "8");
        partitionTestRegion.put("1", customer);
        replicateTestRegion.put("1", customer);

        customer = new HashMap();
        customer.put("id", "2");
        customer.put("name", "bill");
        customer.put("age", "10");
        partitionTestRegion.put("2", customer);
        replicateTestRegion.put("2", customer);

    }

    private RegionMetadata executeFunction(Region region) {
        RegionMetadata regionMetadata = null;
        RetrieveRegionMetadataFunction retrieveRegionMetadataFunction = new RetrieveRegionMetadataFunction();
        Execution execution = FunctionService.onRegion(region);
        ResultCollector rc = execution.execute(retrieveRegionMetadataFunction);
        List result = (ArrayList) rc.getResult();
        for (int i = 0; i < result.size(); i++) {
            regionMetadata = (RegionMetadata) result.get(i);

        }
        return regionMetadata;
    }


}