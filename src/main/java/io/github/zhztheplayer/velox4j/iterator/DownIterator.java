package io.github.zhztheplayer.velox4j.iterator;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.jni.CalledFromNative;
import io.github.zhztheplayer.velox4j.jni.JniApi;
import io.github.zhztheplayer.velox4j.jni.CppObject;

import java.util.Iterator;

public interface DownIterator {
  enum State {
    AVAILABLE(0),
    BLOCKED(1),
    FINISHED(2);

    private final int value;

    State(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  @CalledFromNative
  State advance();

  @CalledFromNative
  long next();
}
