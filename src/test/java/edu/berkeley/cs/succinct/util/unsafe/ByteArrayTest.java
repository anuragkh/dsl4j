package edu.berkeley.cs.succinct.util.unsafe;

import junit.framework.TestCase;

public class ByteArrayTest extends TestCase {

  public void testSetAndGet() throws Exception {
    ByteArray array = new ByteArray(127);
    for (int i = 0; i < 127; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 127; i++) {
      assertEquals(i, array.get(i));
    }
    array.destroy();
  }

  public void testUpdate() throws Exception {
    ByteArray array = new ByteArray(127);
    for (int i = 0; i < 127; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 127; i++) {
      int val = array.update(i, 127 - i);
      assertEquals(127, val);
      assertEquals(127, array.get(i));
    }
    array.destroy();
  }

  public void testLength() throws Exception {
    ByteArray array = new ByteArray(127);
    assertEquals(127, array.length());
    array.destroy();
  }
}
