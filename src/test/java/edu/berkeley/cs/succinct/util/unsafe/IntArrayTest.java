package edu.berkeley.cs.succinct.util.unsafe;

import junit.framework.TestCase;

public class IntArrayTest extends TestCase {
  public void testSetAndGet() throws Exception {
    IntArray array = new IntArray(126976);
    for (int i = 0; i < 126976; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 126976; i++) {
      assertEquals(i, array.get(i));
    }
    array.destroy();
  }

  public void testUpdate() throws Exception {
    IntArray array = new IntArray(126976);
    for (int i = 0; i < 126976; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 126976; i++) {
      int val = array.update(i, 126976 - i);
      assertEquals(126976, val);
      assertEquals(126976, array.get(i));
    }
    array.destroy();
  }

  public void testLength() throws Exception {
    IntArray array = new IntArray(126976);
    assertEquals(126976, array.length());
    array.destroy();
  }
}
