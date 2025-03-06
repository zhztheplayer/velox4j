package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.exception.VeloxException;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
  }

  public static DownIterator fromQueue(Queue<RowVector> queue) {
    if (queue instanceof BlockingQueue) {
      return new FromBlockingQueue((BlockingQueue<RowVector>) queue);
    }
    return new FromQueue(queue);
  }

  private static class FromJavaIterator implements DownIterator {
    private final Iterator<RowVector> itr;

    private FromJavaIterator(Iterator<RowVector> itr) {
      this.itr = itr;
    }

    @Override
    public State advance0() {
      if (!itr.hasNext()) {
        return State.FINISHED;
      }
      return State.AVAILABLE;
    }

    @Override
    public void waitFor() {
    }

    @Override
    public long get() {
      return itr.next().id();
    }

    @Override
    public void close() {

    }
  }

  private static class FromQueue implements DownIterator {
    private final Queue<RowVector> queue;

    public FromQueue(Queue<RowVector> queue) {
      this.queue = queue;
    }

    @Override
    public State advance0() {
      if (queue.isEmpty()) {
        return State.BLOCKED;
      }
      return State.AVAILABLE;
    }

    @Override
    public void waitFor() {
    }

    @Override
    public long get() {
      return queue.remove().id();
    }

    @Override
    public void close() {

    }
  }

  private static class FromBlockingQueue implements DownIterator {
    private final BlockingQueue<RowVector> queue;
    private RowVector pending = null;

    public FromBlockingQueue(BlockingQueue<RowVector> queue) {
      this.queue = queue;
    }

    @Override
    public State advance0() {
      if (pending != null) {
        return State.AVAILABLE;
      }
      if (queue.isEmpty()) {
        return State.BLOCKED;
      }
      return State.AVAILABLE;
    }

    @Override
    public void waitFor() throws InterruptedException {
      pending = queue.take();
    }

    @Override
    public long get() {
      if (pending != null) {
        final RowVector out = pending;
        pending = null;
        return out.id();
      }
      return queue.remove().id();
    }

    @Override
    public void close() {

    }
  }
}
