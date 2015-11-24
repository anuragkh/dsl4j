package edu.berkeley.cs.succinct.util.unsafe;

import junit.framework.TestCase;

public class CharArrayTest extends TestCase {

  public void testSetAndGet() throws Exception {
    CharArray array = new CharArray(65535);
    for (int i = 0; i < 65535; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 65535; i++) {
      assertEquals(i, array.get(i));
    }
    array.destroy();
  }

  public void testUpdate() throws Exception {
    CharArray array = new CharArray(65535);
    for (int i = 0; i < 65535; i++) {
      array.set(i, i);
    }

    for (int i = 0; i < 65535; i++) {
      int val = array.update(i, 65535 - i);
      assertEquals(65535, val);
      assertEquals(65535, array.get(i));
    }
    array.destroy();
  }

  public void testLength() throws Exception {
    CharArray array = new CharArray(65535);
    assertEquals(65535, array.length());
    array.destroy();
  }
}
