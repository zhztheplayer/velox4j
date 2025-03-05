package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new FromJavaIterator(itr);
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
}
