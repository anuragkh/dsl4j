package edu.berkeley.cs.succinct.util.unsafe;

import edu.berkeley.cs.succinct.util.container.BasicArray;

public class ByteArray extends UnsafeMemory implements BasicArray {
  private final static long BLOCK_SIZE = 1;
  private final long startIndex;
  private final int size;

  public ByteArray(byte[] array) {
    this.startIndex = UNSAFE.allocateMemory(array.length * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, array.length * BLOCK_SIZE, (byte) 0);
    this.size = array.length;
    for (int i = 0; i < array.length; i++) {
      set(i, array[i]);
    }
  }

  public ByteArray(int size) {
    this.startIndex = UNSAFE.allocateMemory(size * BLOCK_SIZE);
    UNSAFE.setMemory(startIndex, size * BLOCK_SIZE, (byte) 0);
    this.size = size;
  }

  @Override public void set(int index, int value) {
    UNSAFE.putByte(startIndex + BLOCK_SIZE * index, (byte) (value & 0xff));
  }

  @Override public int update(int index, int val) {
    int tmp = get(index) + (val & 0xff);
    set(index, tmp);
    return tmp;
  }

  @Override public int length() {
    return size;
  }

  @Override public int get(int index) {
    return UNSAFE.getByte(startIndex + BLOCK_SIZE * index);
  }

  @Override public void destroy() {
    UNSAFE.freeMemory(startIndex);
  }
}
