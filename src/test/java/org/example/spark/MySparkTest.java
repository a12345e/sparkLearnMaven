package org.example.spark;


import org.junit.Assert;
import org.junit.Test;

public class MySparkTest extends SetupTest {

    @Test
    public void firstTest(){
        new MySparkApp().run(spark);
        Assert.assertEquals(2, 2);

    }

}
