package zz.vj;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WaterMarkedBlockingQueue
 * HighWaterMark is the actual queue capacity
 * LowWaterMark is to be provided by the user
 *
 * When a queue goes full, all further inserts are blocked/dropped until there is space in queue.
 *
 */
public class WaterMarkedBlockingQueue<B> implements BlockingQueue<B>
{
    private final BlockingQueue<B> queue;
    private final int capacityThreshold;
    private volatile boolean waterMarkReached = false;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();

    /**
     *
     * @param queue Provide a backing blocking queue with appropriate capacity constraints. Ensure queue is empty and no
     *             external accesses are made.
     * @param lowWaterMark
     */
    public WaterMarkedBlockingQueue(BlockingQueue<B> queue, int lowWaterMark) {
        this.queue = queue;
        if(lowWaterMark > queue.remainingCapacity()) {
            throw new IllegalArgumentException("capacityThreshold cannot be greater than queue capacity");
        }
        this.capacityThreshold = queue.remainingCapacity() - lowWaterMark;
    }

    // We have to capture the event when the queue goes full (or in other words has hit the highWaterMark)
    // Once captured we save it into waterMarkReached flag. And all further inserts will be dropped untill
    // the remainingCapacity is larger than or equal to capacityThreshold computed.
    //
    // We add the above logic into {@link #offer(B) offer(B b)}
    //
    // The other insertion functions [ put, offer(with time limit), add ] leverage the {@link #offer(B) offer(B b)}
    //
    // Functions that retrieve elements from the queue - take, remove, removeAll, retainAll, clear etc.,
    // notify using notifyIfNotFull, however these functions are suboptimal compared to the once defined
    // in the various BlockingQueue implementations (ArrayBlockingQueue etc.,) as:
    //
    //   1) These functions check on the remainingCapacity before firing notFull.signal()
    //   2) Some of the drainTo, clear etc., variants in the java.util.concurrent implementations raise
    //      only as many signals as they have cleared elements. We resort to signalAll. This can be improved
    //      if there is a valid use case.

    private boolean isNotFull() {
        return queue.remainingCapacity() >= this.capacityThreshold;
    }

    public void put(B b) throws InterruptedException {
        if(offer(b)) {
            return;
        }
        try {
            lock.lockInterruptibly();
            while (!offer(b)) {
                notFull.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(B b, long l, TimeUnit timeUnit) throws InterruptedException {
        if(offer(b)) {
            return true;
        }
        long nanos = timeUnit.toNanos(l);
        try {
            lock.lockInterruptibly();
            while (!offer(b)) {
                if (nanos < 0L) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
        } finally {
            lock.unlock();
        }
        return true;
    }

    public boolean add(B b) {
        if(offer(b)) {
            return true;
        } else {
            throw new IllegalStateException("Capacity exceeded");
        }
    }

    public boolean offer(B b) {
        if(!waterMarkReached) {
            boolean result = queue.offer(b);
            if(!result) {
                waterMarkReached = true;
            }
            return result;
        } else {
            if (isNotFull()) {
                if(queue.offer(b)) {
                    waterMarkReached = false;
                    return true;
                }
            }
            return false;
        }
    }

    public boolean addAll(Collection<? extends B> collection) {
        for (B b : collection) {
            add(b);
        }
        return true;
    }

    private void notifyIfNotFull(boolean all) {
        try {
            lock.lock();
            if(all) {
                notFull.signalAll();
            } else {
                notFull.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public int drainTo(Collection<? super B> collection) {
        int result = queue.drainTo(collection);
        notifyIfNotFull(true);
        return result;
    }

    public int drainTo(Collection<? super B> collection, int i) {
        int result = queue.drainTo(collection, i);
        notifyIfNotFull(true);
        return result;
    }

    public B take() throws InterruptedException {
        B result = queue.take();
        notifyIfNotFull(false);
        return result;
    }

    public B poll(long l, TimeUnit timeUnit) throws InterruptedException {
        B result = queue.poll(l, timeUnit);
        if(result != null)
            notifyIfNotFull(false);
        return result;
    }

    public B remove() {
        B result = queue.remove();
        notifyIfNotFull(false);
        return result;
    }

    public B poll() {
        B result = queue.poll();
        notifyIfNotFull(false);
        return result;
    }

    public boolean removeAll(Collection<?> collection) {
        boolean result = queue.removeAll(collection);
        notifyIfNotFull(true);
        return result;
    }

    public boolean retainAll(Collection<?> collection) {
        boolean result = queue.retainAll(collection);
        notifyIfNotFull(true);
        return result;
    }

    public void clear() {
        queue.clear();
        notifyIfNotFull(true);
    }
    // ** Delegated functions - As is **

    public B element() {
        return queue.element();
    }

    public B peek() {
        return queue.peek();
    }

    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    public boolean remove(Object o) {
        return queue.remove(o);
    }

    public boolean containsAll(Collection<?> collection) {
        return queue.containsAll(collection);
    }

    public int size() {
        return queue.size();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public boolean contains(Object o) {
        return queue.contains(o);
    }

    public Iterator<B> iterator() {
        return queue.iterator();
    }

    public Object[] toArray() {
        return queue.toArray();
    }

    public <T> T[] toArray(T[] ts) {
        return queue.toArray(ts);
    }

}
