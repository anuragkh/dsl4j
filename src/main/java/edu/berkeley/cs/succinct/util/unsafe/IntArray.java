package edu.berkeley.cs.succinct.util.unsafe;

import edu.berkeley.cs.succinct.util.container.BasicArray;

public class IntArray extends UnsafeMemory implements BasicArray {
  private final static long BLOCK_SIZE = 4;
  private final long startIndex;
  private final int size;
  private final boolean isPrimary;

  public IntArray(int[] array) {
    this.isPrimary = true;
    this.startIndex = UNSAFE.allocateMemory(array.length * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, array.length * BLOCK_SIZE, (byte) 0);
    this.size = array.length;
    for (int i = 0; i < array.length; i++) {
      set(i, array[i]);
    }
  }

  public IntArray(int size) {
    this.isPrimary = true;
    this.startIndex = UNSAFE.allocateMemory(size * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, size * BLOCK_SIZE, (byte) 0);
    this.size = size;
  }

  public IntArray(IntArray other, int offset) {
    this.isPrimary = false;
    this.size = other.size - offset;
    this.startIndex = other.startIndex + offset * BLOCK_SIZE;
  }

  @Override public void set(int index, int value) {
    UNSAFE.putInt(startIndex + BLOCK_SIZE * index, value);
  }

  @Override public int update(int index, int val) {
    int tmp = get(index) + val;
    set(index, tmp);
    return tmp;
  }

  @Override public int length() {
    return size;
  }

  @Override public int get(int index) {
    return UNSAFE.getInt(startIndex + BLOCK_SIZE * index);
  }

  @Override public void destroy() {
    if (isPrimary)
      UNSAFE.freeMemory(startIndex);
  }
}
