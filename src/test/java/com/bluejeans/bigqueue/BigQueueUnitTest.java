package com.bluejeans.bigqueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;


public class BigQueueUnitTest {

    private final String testDir = TestUtil.TEST_BASE_DIR + "bigqueue/unit";
    private BigQueue bigQueue;

    @Test
    public void simpleTest() throws IOException {
        for (int i = 1; i <= 2; i++) {

            bigQueue = new BigQueue(testDir, "simple_test");
            assertNotNull(bigQueue);

            for (int j = 1; j <= 3; j++) {
                assertTrue(bigQueue.size() == 0L);
                assertTrue(bigQueue.isEmpty());

                assertNull(bigQueue.dequeue());
                assertNull(bigQueue.peek());

                bigQueue.enqueue("hello".getBytes());
                assertTrue(bigQueue.size() == 1L);
                assertTrue(!bigQueue.isEmpty());
                assertEquals("hello", new String(bigQueue.peek()));
                assertEquals("hello", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

                bigQueue.enqueue("world".getBytes());
                bigQueue.flush();
                assertTrue(bigQueue.size() == 1L);
                assertTrue(!bigQueue.isEmpty());
                assertEquals("world", new String(bigQueue.dequeue()));
                assertNull(bigQueue.dequeue());

            }

            bigQueue.close();

        }
    }

    @Test
    public void bigLoopTest() throws IOException {
        bigQueue = new BigQueue(testDir, "big_loop_test");
        assertNotNull(bigQueue);

        final int loop = 1000000;
        for (int i = 0; i < loop; i++) {
            bigQueue.enqueue(("" + i).getBytes());
            assertTrue(bigQueue.size() == i + 1L);
            assertTrue(!bigQueue.isEmpty());
            final byte[] data = bigQueue.peek();
            assertEquals("0", new String(data));
        }

        assertTrue(bigQueue.size() == loop);
        assertTrue(!bigQueue.isEmpty());
        assertEquals("0", new String(bigQueue.peek()));

        bigQueue.close();

        // create a new instance on exiting queue
        bigQueue = new BigQueue(testDir, "big_loop_test");
        assertTrue(bigQueue.size() == loop);
        assertTrue(!bigQueue.isEmpty());

        for (int i = 0; i < loop; i++) {
            final byte[] data = bigQueue.dequeue();
            assertEquals("" + i, new String(data));
            assertTrue(bigQueue.size() == loop - i - 1);
        }

        assertTrue(bigQueue.isEmpty());

        bigQueue.gc();

        bigQueue.close();
    }

    @Test
    public void loopTimingTest() {
        bigQueue = new BigQueue(testDir, "loop_timing_test");
        assertNotNull(bigQueue);

        final int loop = 1000000;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            bigQueue.enqueue(("" + i).getBytes());
        long end = System.currentTimeMillis();
        int timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to enqueue " + loop + " items : " + timeInSeconds + " seconds.");

        begin = System.currentTimeMillis();
        for (int i = 0; i < loop; i++)
            assertEquals("" + i, new String(bigQueue.dequeue()));
        end = System.currentTimeMillis();
        timeInSeconds = (int) ((end - begin) / 1000L);
        System.out.println("Time used to dequeue " + loop + " items : " + timeInSeconds + " seconds.");
    }

    @Test
    public void testInvalidDataPageSize() {
        try {
            bigQueue = new BigQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE - 1);
            fail("should throw invalid page size exception");
        }
        catch (final IllegalArgumentException iae) {
            // ecpected
        }
        // ok
        bigQueue = new BigQueue(testDir, "testInvalidDataPageSize", BigArray.MINIMUM_DATA_PAGE_SIZE);
    }

    @Test
    public void testApplyForEachDoNotChangeTheQueue() throws Exception {
        bigQueue = new BigQueue(testDir, "testApplyForEachDoNotChangeTheQueue", BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue("1".getBytes());
        bigQueue.enqueue("2".getBytes());
        bigQueue.enqueue("3".getBytes());

        final DefaultItemIterator dii = new DefaultItemIterator();
        bigQueue.applyForEach(dii);
        System.out.println("[" + dii.getCount() + "] " + dii.toString());

        assertEquals(3, bigQueue.size());
        assertEquals(bigQueue.size(), dii.getCount());

        assertArrayEquals("1".getBytes(), bigQueue.dequeue());
        assertArrayEquals("2".getBytes(), bigQueue.dequeue());
        assertArrayEquals("3".getBytes(), bigQueue.dequeue());

        assertEquals(0, bigQueue.size());
    }

    @Test
    public void concurrentApplyForEachTest() throws Exception {
        bigQueue = new BigQueue(testDir, "concurrentApplyForEachTest", BigArray.MINIMUM_DATA_PAGE_SIZE);

        final long N = 100000;

        final Thread publisher = new Thread(new Runnable() {
            private Long item = 1l;

            @Override
            public void run() {
                for (long i = 0; i < N; i++)
                    try {
                        bigQueue.enqueue(item.toString().getBytes());
                        item++;
                        Thread.yield();
                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        final Thread subscriber = new Thread(new Runnable() {
            private long item = 0l;

            @Override
            public void run() {
                for (long i = 0; i < N; i++)
                    try {
                        if (bigQueue.size() > 0) {
                            final byte[] bytes = bigQueue.dequeue();
                            final String str = new String(bytes);
                            final long curr = Long.parseLong(str);
                            assertEquals(item + 1, curr);
                            item = curr;
                        }

                        Thread.yield();
                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
            }
        });

        subscriber.start();
        publisher.start();

        for (long i = 0; i < N; i += N / 100) {
            final DefaultItemIterator dii = new DefaultItemIterator();
            bigQueue.applyForEach(dii);
            System.out.println("[" + dii.getCount() + "] " + dii.toString());
            Thread.sleep(2);
        }

        publisher.join();
        subscriber.join();
    }

    @After
    public void clean() {
        if (bigQueue != null)
            bigQueue.removeAll();
    }

    private class DefaultItemIterator implements ItemIterator {
        private long count = 0;
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void forEach(final byte[] item) {
            if (count < 20) {
                sb.append(new String(item));
                sb.append(", ");

            }
            count++;
        }

        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
