package io.github.zhztheplayer.velox4j.data;

import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.type.Type;
import org.junit.Assert;

import java.util.List;

public final class BaseVectorTests {
  private BaseVectorTests() {

  }

  public static void assertEquals(BaseVector expected, BaseVector actual) {
    final Type typeExpected = expected.getType();
    final Type typeActual = actual.getType();
    Assert.assertEquals(Serde.toPrettyJson(typeExpected), Serde.toPrettyJson(typeActual));
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  public static void assertEquals(List<? extends BaseVector> expected, List<? extends BaseVector> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }
}
