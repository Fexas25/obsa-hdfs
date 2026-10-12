package org.apache.hadoop.fs.obs;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java 8 compatible fallback for abandoned iterators. Does not fetch pages or hold the iterator.
 */
final class ListingCleanup extends PhantomReference<Object> {
  private static final ReferenceQueue<Object> QUEUE = new ReferenceQueue<>();
  private static final Set<ListingCleanup> LIVE =
      Collections.newSetFromMap(new ConcurrentHashMap<ListingCleanup, Boolean>());

  static {
    Thread worker =
        new Thread(
            () -> {
              while (true) {
                try {
                  ((ListingCleanup) QUEUE.remove()).release();
                } catch (InterruptedException ignored) {
                  // This daemon belongs to the class loader, not an individual filesystem.
                }
              }
            },
            "obs-listing-cleanup");
    worker.setDaemon(true);
    worker.start();
  }

  private final AtomicLong handle;

  ListingCleanup(Object owner, long handle) {
    super(owner, QUEUE);
    this.handle = new AtomicLong(handle);
    LIVE.add(this);
  }

  void release() {
    long value = handle.getAndSet(0);
    if (value != 0) NativeObs.nativeReleaseListing(value);
    LIVE.remove(this);
    clear();
  }
}
