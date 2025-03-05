package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;

import java.util.Iterator;

public final class DownIterators {
  public static DownIterator fromJavaIterator(Iterator<RowVector> itr) {
    return new JavaIteratorAdapter(itr);
  }

  private static class JavaIteratorAdapter implements DownIterator {
    private final Iterator<RowVector> itr;

    private JavaIteratorAdapter(Iterator<RowVector> itr) {
      this.itr = itr;
    }

    @Override
    public State advance() {
      if (!itr.hasNext()) {
        return State.FINISHED;
      }
      return State.AVAILABLE;
    }

    @Override
    public long next() {
      return itr.next().id();
    }
  }
}
