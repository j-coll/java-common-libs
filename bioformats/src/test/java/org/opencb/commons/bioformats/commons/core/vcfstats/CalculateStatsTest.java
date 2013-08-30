package org.opencb.commons.bioformats.commons.core.vcfstats;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.opencb.commons.bioformats.commons.core.connectors.variant.*;
import org.opencb.commons.bioformats.commons.core.connectors.variant.VcfFileStatsDataWriter;
import org.opencb.commons.bioformats.commons.core.connectors.variant.VcfStatsDataWriter;
import org.opencb.commons.bioformats.commons.core.variant.io.Vcf4Reader;

/**
 * Created with IntelliJ IDEA.
 * User: aaleman
 * Date: 8/26/13
 * Time: 1:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class CalculateStatsTest {

    private Long start, end;
    private Vcf4Reader vcf;
    private String vcfFileName = "/home/aaleman/tmp/small.vcf";
    private String pedFileName = "/home/aaleman/tmp/file.ped";
    private String pathStats   = "/home/aaleman/tmp/jstats/";


    @Rule
    public TestName name = new TestName();


    @Before
    public void setUp() throws Exception {
        start = System.currentTimeMillis();


    }

    @After
    public void tearDown() throws Exception {
        end = System.currentTimeMillis();
        System.out.println("Time " + name.getMethodName() + ": " + (end - start));

    }

    @Test
    public void testCalculateStatsList() throws Exception {

        VcfDataReader vcfReader = new VcfFileDataReader(vcfFileName);
        VcfStatsDataWriter vcfWriter = new VcfFileStatsDataWriter(pathStats);

        CalculateStats.runner(vcfReader, vcfWriter, pedFileName, pathStats);

    }



}
