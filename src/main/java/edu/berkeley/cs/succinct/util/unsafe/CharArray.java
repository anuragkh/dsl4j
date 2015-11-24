package edu.berkeley.cs.succinct.util.unsafe;

import edu.berkeley.cs.succinct.util.container.BasicArray;

public class CharArray extends UnsafeMemory implements BasicArray {
  private final static long BLOCK_SIZE = 2;
  private final long startIndex;
  private final int size;

  public CharArray(char[] array) {
    this.startIndex = UNSAFE.allocateMemory(array.length * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, array.length * BLOCK_SIZE, (byte) 0);
    this.size = array.length;
    for (int i = 0; i < array.length; i++) {
      set(i, array[i]);
    }
  }

  public CharArray(int size) {
    this.startIndex = UNSAFE.allocateMemory(size * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, size * BLOCK_SIZE, (byte) 0);
    this.size = size;
  }

  @Override public void set(int index, int value) {
    UNSAFE.putChar(startIndex + BLOCK_SIZE * index, (char) (value & 0xffff));
  }

  @Override public int update(int index, int val) {
    int tmp = get(index) + (val & 0xffff);
    set(index, tmp);
    return tmp;
  }

  @Override public int length() {
    return size;
  }

  @Override public int get(int index) {
    return UNSAFE.getChar(startIndex + BLOCK_SIZE * index);
  }

  @Override public void destroy() {
    UNSAFE.freeMemory(startIndex);
  }
}
