package edu.berkeley.cs.succinct.util.unsafe;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeMemory {
  protected static final Unsafe UNSAFE;
  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
