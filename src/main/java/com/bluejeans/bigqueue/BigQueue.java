package com.bluejeans.bigqueue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A big, fast and persistent queue implementation.
 *
 * Main features: 1. FAST : close to the speed of direct memory access, both
 * enqueue and dequeue are close to O(1) memory access. 2. MEMORY-EFFICIENT :
 * automatic paging and swapping algorithm, only most-recently accessed data is
 * kept in memory. 3. THREAD-SAFE : multiple threads can concurrently enqueue
 * and dequeue without data corruption. 4. PERSISTENT - all data in queue is
 * persisted on disk, and is crash resistant. 5. BIG(HUGE) - the total size of
 * the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueue implements Closeable {

    final BigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    MappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();

    /**
     * A big, fast and persistent queue implementation, use default back data
     * page size, see {@link BigArray#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir
     *            the directory to store queue data
     * @param queueName
     *            the name of the queue, will be appended as last part of the
     *            queue directory
     */
    public BigQueue(final String queueDir, final String queueName) {
        this(queueDir, queueName, BigArray.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir
     *            the directory to store queue data
     * @param queueName
     *            the name of the queue, will be appended as last part of the
     *            queue directory
     * @param pageSize
     *            the back data file size per page in bytes, see minimum allowed
     *            {@link BigArray#MINIMUM_DATA_PAGE_SIZE}
     */
    public BigQueue(final String queueDir, final String queueName, final int pageSize) {
        innerArray = new BigArray(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always
        // cached
        queueFrontIndexPageFactory = new MappedPageFactory(QUEUE_FRONT_INDEX_PAGE_SIZE, innerArray.getArrayDirectory()
                + QUEUE_FRONT_INDEX_PAGE_FOLDER, 10 * 1000/* does not matter */);
        final MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        final long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    /**
     * Determines whether a queue is empty
     *
     * @return ture if empty, false otherwise
     */

    public boolean isEmpty() {
        return queueFrontIndex.get() == innerArray.getHeadIndex();
    }

    /**
     * Adds an item at the back of a queue
     *
     * @param data
     *            to be enqueued data
     */

    public void enqueue(final byte[] data) {
        innerArray.append(data);
    }

    /**
     * Retrieves and removes the front of a queue
     *
     * @return data at the front of a queue
     */

    public byte[] dequeue() {
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return null;
            }
            queueFrontIndex = this.queueFrontIndex.get();
            final byte[] data = innerArray.get(queueFrontIndex);
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex == Long.MAX_VALUE) {
                nextQueueFrontIndex = 0L; // wrap
            } else {
                nextQueueFrontIndex++;
            }
            this.queueFrontIndex.set(nextQueueFrontIndex);
            // persist the queue front
            final MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);
            return data;
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    /**
     * Retrieves and removes the fronts of a queue upto given total number /
     * total size whichever is smaller
     *
     * @param max
     *            the maximum to dequque
     * @return data at the fronts of a queue
     */

    public List<byte[]> dequeueMulti(final int max) {
        long queueFrontIndex = -1L;
        final List<byte[]> dataList = new ArrayList<byte[]>();
        try {
            queueFrontWriteLock.lock();
            final long size = size();
            for (int i = 0; i < max && i < size; i++) {
                if (!this.isEmpty()) {
                    queueFrontIndex = this.queueFrontIndex.get();
                    final byte[] data = innerArray.get(queueFrontIndex);
                    dataList.add(data);
                    long nextQueueFrontIndex = queueFrontIndex;
                    if (nextQueueFrontIndex == Long.MAX_VALUE) {
                        nextQueueFrontIndex = 0L; // wrap
                    } else {
                        nextQueueFrontIndex++;
                    }
                    this.queueFrontIndex.set(nextQueueFrontIndex);
                    // persist the queue front
                    final MappedPage queueFrontIndexPage = queueFrontIndexPageFactory
                            .acquirePage(QUEUE_FRONT_PAGE_INDEX);
                    final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
                    queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
                    queueFrontIndexPage.setDirty(true);
                }
            }
            return dataList;
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    /**
     * Removes all items of a queue, this will empty the queue and delete all
     * back data files.
     */

    public void removeAll() {
        try {
            queueFrontWriteLock.lock();
            innerArray.removeAll();
            queueFrontIndex.set(0L);
            final MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            final ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    /**
     * Retrieves the item at the front of a queue
     *
     * @return data at the front of a queue
     */

    public byte[] peek() {
        if (this.isEmpty()) {
            return null;
        }
        final byte[] data = innerArray.get(queueFrontIndex.get());
        return data;
    }

    /**
     * Retrieves the items at the front of a queue
     *
     * @param max
     *            the maximum elements to peek
     * @return data at the front of a queue
     */

    public List<byte[]> peekMulti(final int max) {
        final List<byte[]> dataList = new ArrayList<byte[]>();
        final long size = size();
        final long queueFront = queueFrontIndex.get();
        byte[] data = null;
        try {
            for (int i = 0; i < max && i < size; i++) {
                data = innerArray.get(queueFront + i);
                if (data == null) {
                    break;
                } else {
                    dataList.add(data);
                }
            }
        } catch (final RuntimeException rex) {
            // do nothing
        }
        return dataList;
    }

    public void applyForEach(final ItemIterator iterator) {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return;
            }

            final long index = queueFrontIndex.get();
            for (long i = index; i < innerArray.size(); i++) {
                iterator.forEach(innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (queueFrontIndexPageFactory != null) {
            queueFrontIndexPageFactory.releaseCachedPages();
        }

        innerArray.close();
    }

    /**
     * Delete all used data files to free disk space.
     *
     * BigQueue will persist enqueued data in disk files, these data files will
     * remain even after the data in them has been dequeued later, so your
     * application is responsible to periodically call this method to delete all
     * used data files and free disk space.
     */

    public void gc() {
        long beforeIndex = queueFrontIndex.get();
        if (beforeIndex == 0L) {
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            innerArray.removeBeforeIndex(beforeIndex);
        } catch (final IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    /**
     * Force to persist current state of the queue,
     *
     * normally, you don't need to flush explicitly since: 1.) BigQueue will
     * automatically flush a cached page when it is replaced out, 2.) BigQueue
     * uses memory mapped file technology internally, and the OS will flush the
     * changes even your process crashes,
     *
     * call this periodically only if you need transactional reliability and you
     * are aware of the cost to performance.
     */

    public void flush() {
        try {
            queueFrontWriteLock.lock();
            queueFrontIndexPageFactory.flush();
            innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    /**
     * Total number of items available in the queue.
     *
     * @return total number
     */

    public long size() {
        final long qFront = queueFrontIndex.get();
        final long qRear = innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return qRear - qFront;
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }

}
