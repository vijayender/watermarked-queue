package zz.vj;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ArrayBlockingQueue;

@Test
public class TestWMBQ {
    public void testInsertToQueue() throws InterruptedException {
        WaterMarkedBlockingQueue<Integer> wmb = new WaterMarkedBlockingQueue<Integer>(new ArrayBlockingQueue<Integer>(5), 3);
        wmb.put(1);
        wmb.put(2);
        wmb.put(3);
        wmb.put(4);
        wmb.put(5);
        Assert.assertTrue(!wmb.offer(6));
        Assert.assertTrue(!wmb.offer(6));
        Assert.assertTrue(wmb.take() == 1);
        Assert.assertTrue(!wmb.offer(6));
        Assert.assertTrue(wmb.take() == 2);
        Assert.assertTrue(wmb.offer(6));
    }
}