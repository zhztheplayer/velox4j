package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;
import java.util.Queue;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
  }

  public static DownIterator fromQueue(Queue<RowVector> queue) {
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
    public long get() {
      return itr.next().id();
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
    public long get() {
      return queue.remove().id();
    }
  }
}
