package com.mn.cdc.util;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/**
 * @program:cdc-master
 * @description 只是一个锁，还线程不安全，没看出多大的用~~~只是加快点速度
 * @author:miaoneng
 * @create:2021-09-11 11:59
 **/
public class FunctionalReadWriteLock {
    /**
     *ReentrantReadWriteLock，它表示两个锁，一个是读操作相关的锁，称为共享锁；一个是写相关的锁，称为排他锁
     * Create a read-write lock that supports reentrancy.
     * @return the functional read-write lock; never null
     */
    public static FunctionalReadWriteLock reentrant() {
        return create(new ReentrantReadWriteLock());
    }

    /**
     * Create a read-write lock around the given standard {@link ReadWriteLock}.
     * @param lock the standard lock; may not be null
     * @return the functional read-write lock; never null
     */
    public static FunctionalReadWriteLock create(ReadWriteLock lock) {
        assert lock != null;
        return new FunctionalReadWriteLock(lock);
    }

    private final ReadWriteLock lock;

    protected FunctionalReadWriteLock(ReadWriteLock lock) {
        this.lock = lock;
    }

    /**
     * Obtain a read lock, perform the operation, and release the read lock.
     *
     * @param operation the operation to perform while the read lock is held; may not be null
     * @return the result of the operation
     */
    public <T> T read(Supplier<T> operation) {
        try {
            lock.readLock().lock();
            return operation.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Obtain a read lock, perform the operation, and release the lock.
     *
     * @param operation the operation to perform while the read lock is held; may not be null
     */
    public void read(Runnable operation) {
        try {
            lock.readLock().lock();
            operation.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Obtain an exclusive write lock, perform the operation, and release the lock.
     *
     * @param operation the operation to perform while the write lock is held; may not be null
     * @return the result of the operation
     */
    public <T> T write(Supplier<T> operation) {
        try {
            lock.writeLock().lock();
            return operation.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Obtain an exclusive write lock, perform the operation, and release the lock.
     *
     * @param operation the operation to perform while the write lock is held; may not be null
     */
    public void write(Runnable operation) {
        try {
            lock.writeLock().lock();
            operation.run();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
