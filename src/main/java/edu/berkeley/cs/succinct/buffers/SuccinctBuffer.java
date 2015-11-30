package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctCore;
import edu.berkeley.cs.succinct.util.BitUtils;
import edu.berkeley.cs.succinct.util.CommonUtils;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeByteBuffer;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeIntBuffer;
import edu.berkeley.cs.succinct.util.buffer.ThreadSafeLongBuffer;
import edu.berkeley.cs.succinct.util.container.Pair;
import edu.berkeley.cs.succinct.util.suffixarray.QSufSort;
import edu.berkeley.cs.succinct.util.serops.ArrayOps;
import edu.berkeley.cs.succinct.util.serops.DeltaEncodedIntVectorOps;
import edu.berkeley.cs.succinct.util.serops.IntVectorOps;
import edu.berkeley.cs.succinct.util.vector.DeltaEncodedIntVector;
import edu.berkeley.cs.succinct.util.vector.IntVector;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.HashMap;

public class SuccinctBuffer extends SuccinctCore {

  // To maintain versioning
  private static final long serialVersionUID = 1382615274437547247L;

  // Serialized data structures
  protected transient ThreadSafeByteBuffer metadata;
  protected transient ThreadSafeByteBuffer alphabetmap;
  protected transient ThreadSafeByteBuffer alphabet;
  protected transient ThreadSafeLongBuffer sa;
  protected transient ThreadSafeLongBuffer isa;
  protected transient ThreadSafeIntBuffer columnoffsets;
  protected transient ThreadSafeByteBuffer[] columns;
  protected transient StorageMode storageMode;

  /**
   * Default constructor.
   */
  public SuccinctBuffer() {
    super();
  }

  /**
   * Constructor to initialize SuccinctCore from input byte array.
   *
   * @param input      Input byte array.
   */
  public SuccinctBuffer(byte[] input) {
    // Append the EOF byte
    int end = input.length;
    input = Arrays.copyOf(input, input.length + 1);
    input[end] = EOF;

    // Construct Succinct data-structures
    construct(input);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctBuffer(String path, StorageMode storageMode) {
    this.storageMode = storageMode;
    try {
      if (storageMode == StorageMode.MEMORY_ONLY) {
        readFromFile(path);
      } else if (storageMode == StorageMode.MEMORY_MAPPED) {
        memoryMap(path);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a DataInputStream.
   *
   * @param is Input stream to load the data from
   */
  public SuccinctBuffer(DataInputStream is) {
    try {
      readFromStream(is);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctBuffer(ByteBuffer buf) {
    mapFromBuffer(buf);
  }


  /**
   * Lookup NPA at specified index.
   *
   * @param i Index into NPA.
   * @return Value of NPA at specified index.
   */
  @Override public long lookupNPA(long i) {
    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "NPA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    int colId = ArrayOps.getRank1(columnoffsets.buffer(), 0, getAlphabetSize(), (int) i) - 1;

    assert colId < getAlphabetSize();
    assert columnoffsets.get(colId) <= i;

    return (long) DeltaEncodedIntVectorOps.get(columns[colId].buffer(),
      (int) (i - columnoffsets.get(colId)));
  }

  /**
   * Lookup SA at specified index.
   *
   * @param i Index into SA.
   * @return Value of SA at specified index.
   */
  @Override public long lookupSA(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "SA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    int j = 0;
    while (i % getSamplingRate() != 0) {
      i = lookupNPA(i);
      j++;
    }
    long saVal = IntVectorOps.get(sa.buffer(), (int) (i / getSamplingRate()), getSampleBitWidth());

    if (saVal < j)
      return getOriginalSize() - (j - saVal);
    return saVal - j;
  }

  /**
   * Lookup ISA at specified index.
   *
   * @param i Index into ISA.
   * @return Value of ISA at specified index.
   */
  @Override public long lookupISA(long i) {

    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "ISA index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    int sampleIdx = (int) (i / getSamplingRate());
    int pos = IntVectorOps.get(isa.buffer(), sampleIdx, getSampleBitWidth());
    i -= (sampleIdx * getSamplingRate());
    while (i-- != 0) {
      pos = (int) lookupNPA(pos);
    }
    return pos;
  }

  /**
   * Lookup up the inverted alphabet map at specified index.
   *
   * @param i Index into inverted alphabet map
   * @return Value of inverted alphabet map at specified index.
   */
  @Override public byte lookupC(long i) {
    if (i > getOriginalSize() - 1 || i < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "C index out of bounds: i = " + i + " originalSize = " + getOriginalSize());
    }

    int idx = ArrayOps.getRank1(columnoffsets.buffer(), 0, getAlphabetSize(), (int) i) - 1;
    return alphabet.get(idx);
  }

  /**
   * Binary Search for a value withing NPA.
   *
   * @param val      Value to be searched.
   * @param startIdx Starting index into NPA.
   * @param endIdx   Ending index into NPA.
   * @param flag     Whether to search for left or the right boundary.
   * @return Search result as an index into the NPA.
   */
  @Override public long binSearchNPA(long val, long startIdx, long endIdx, boolean flag) {

    long sp = startIdx;
    long ep = endIdx;
    long m;

    while (sp <= ep) {
      m = (sp + ep) / 2;

      long psi_val;
      psi_val = lookupNPA(m);

      if (psi_val == val) {
        return m;
      } else if (val < psi_val) {
        ep = m - 1;
      } else {
        sp = m + 1;
      }
    }

    return flag ? ep : sp;
  }

  /**
   * Construct Succinct data structures from input byte array.
   *
   * @param input Input byte array.
   */
  private void construct(byte[] input) {

    QSufSort suffixSorter = new QSufSort();
    suffixSorter.buildSuffixArray(input);

    // Get SA, ISA
    int[] SA, ISA;
    SA = suffixSorter.getSA();
    ISA = suffixSorter.getISA();

    System.out.println("Constructed SA, ISA");

    // Set metadata
    setOriginalSize(input.length);
    setSamplingRate(32);                // FIXME: Hard-coded
    setSampleBitWidth(BitUtils.bitWidth(getOriginalSize()));
    setAlphabetSize(suffixSorter.getAlphabetSize());
    metadata = ThreadSafeByteBuffer.allocate(16);
    metadata.putInt(getOriginalSize());
    metadata.putInt(getSamplingRate());
    metadata.putInt(getSampleBitWidth());
    metadata.putInt(getAlphabetSize());
    metadata.rewind();

    // Populate alphabet
    alphabet = ThreadSafeByteBuffer.wrap(suffixSorter.getAlphabet());
    alphabet.rewind();

    // Populate columnoffsets and alphabetMap
    int pos = 0;
    alphabetMap = new HashMap<>();
    alphabetMap.put(input[SA[0]], new Pair<>(0, pos));
    columnoffsets = ThreadSafeIntBuffer.allocate(getAlphabetSize());
    columnoffsets.put(pos, 0);
    pos++;
    for (int i = 1; i < getOriginalSize(); ++i) {
      if (input[SA[i]] != input[SA[i - 1]]) {
        alphabetMap.put(input[SA[i]], new Pair<>(i, pos));
        columnoffsets.put(pos, i);
        pos++;
      }
    }
    alphabetMap.put(SuccinctCore.EOA, new Pair<>(getOriginalSize(), getAlphabetSize()));
    columnoffsets.rewind();

    // Serialize alphabetmap
    alphabetmap = ThreadSafeByteBuffer.allocate(alphabetMap.size() * (1 + 4 + 4));
    for (Byte c : alphabetMap.keySet()) {
      Pair<Integer, Integer> cval = alphabetMap.get(c);
      alphabetmap.put(c);
      alphabetmap.putInt(cval.first);
      alphabetmap.putInt(cval.second);
    }
    alphabetmap.rewind();

    System.out.println("Computed Alphabet, AlphabetMap, ColumnOffsets");

    // Construct NPA
    int[] NPA = new int[getOriginalSize()];
    for (int i = 1; i < getOriginalSize(); i++) {
      NPA[ISA[i - 1]] = ISA[i];
    }
    NPA[ISA[getOriginalSize() - 1]] = ISA[0];

    System.out.println("Computed uncompressed NPA");

    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int startOffset = columnoffsets.get(i);
      int endOffset = (i < getAlphabetSize() - 1) ? columnoffsets.get(i + 1) : getOriginalSize();
      int length = endOffset - startOffset;
      DeltaEncodedIntVector columnVector = new DeltaEncodedIntVector(NPA, startOffset, length, 128);
      int columnSizeInBytes = columnVector.serializedSize();
      columns[i] = ThreadSafeByteBuffer.allocate(columnSizeInBytes);
      columnVector.writeToBuffer(columns[i].buffer());
      columns[i].rewind();
    }

    System.out.println("Constructed compressed NPA");

    // Sample SA, ISA
    IntVector sampledSA, sampledISA;
    int numSampledElements = CommonUtils.numBlocks(getOriginalSize(), getSamplingRate());
    int sampleBitWidth = BitUtils.bitWidth(getOriginalSize());
    sampledSA = new IntVector(numSampledElements, sampleBitWidth);
    sampledISA = new IntVector(numSampledElements, sampleBitWidth);
    for (int i = 0; i < getOriginalSize(); i++) {
      int saVal = SA[i];
      if (i % getSamplingRate() == 0) {
        sampledSA.add(i / getSamplingRate(), saVal);
      }
      if (saVal % getSamplingRate() == 0) {
        sampledISA.add(saVal / getSamplingRate(), i);
      }
    }
    sa = ThreadSafeLongBuffer.wrap(sampledSA.getData());
    sa.rewind();
    isa = ThreadSafeLongBuffer.wrap(sampledISA.getData());
    isa.rewind();

    System.out.println("Constructed sampled SA, ISA");
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  public void writeToStream(DataOutputStream os) throws IOException {
    WritableByteChannel dataChannel = Channels.newChannel(os);

    dataChannel.write(metadata.order(ByteOrder.BIG_ENDIAN));
    metadata.rewind();

    dataChannel.write(alphabetmap.order(ByteOrder.BIG_ENDIAN));
    alphabetmap.rewind();

    dataChannel.write(alphabet.order(ByteOrder.BIG_ENDIAN));
    alphabet.rewind();

    ByteBuffer bufSA = ByteBuffer.allocate(sa.limit() * 8);
    bufSA.asLongBuffer().put(sa.buffer());
    dataChannel.write(bufSA.order(ByteOrder.BIG_ENDIAN));
    sa.rewind();

    ByteBuffer bufISA = ByteBuffer.allocate(isa.limit() * 8);
    bufISA.asLongBuffer().put(isa.buffer());
    dataChannel.write(bufISA.order(ByteOrder.BIG_ENDIAN));
    isa.rewind();

    ByteBuffer bufColOff = ByteBuffer.allocate(getAlphabetSize() * 4);
    bufColOff.asIntBuffer().put(columnoffsets.buffer());
    dataChannel.write(bufColOff.order(ByteOrder.BIG_ENDIAN));
    columnoffsets.rewind();

    for (int i = 0; i < columns.length; i++) {
      os.writeInt(columns[i].limit());
      dataChannel.write(columns[i].order(ByteOrder.BIG_ENDIAN));
      columns[i].rewind();
    }
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  public void readFromStream(DataInputStream is) throws IOException {
    ReadableByteChannel dataChannel = Channels.newChannel(is);
    setOriginalSize(is.readInt());
    setSamplingRate(is.readInt());
    setSampleBitWidth(is.readInt());
    setAlphabetSize(is.readInt());

    metadata = ThreadSafeByteBuffer.allocate(16);
    metadata.putInt(getOriginalSize());
    metadata.putInt(getSamplingRate());
    metadata.putInt(getSampleBitWidth());
    metadata.putInt(getAlphabetSize());
    metadata.rewind();

    alphabetmap = ThreadSafeByteBuffer.allocate((getAlphabetSize() + 1) * (1 + 4 + 4));
    dataChannel.read(alphabetmap.buffer());
    alphabetmap.rewind();

    // Deserialize alphabetmap
    alphabetMap = new HashMap<>();
    for (int i = 0; i < getAlphabetSize() + 1; i++) {
      byte c = alphabetmap.get();
      int v1 = alphabetmap.getInt();
      int v2 = alphabetmap.getInt();
      alphabetMap.put(c, new Pair<>(v1, v2));
    }

    // Read alphabet
    alphabet = ThreadSafeByteBuffer.allocate(getAlphabetSize());
    dataChannel.read(alphabet.buffer());
    alphabet.rewind();

    // Compute number of sampled elements
    int totalSampledBits = CommonUtils.numBlocks(getOriginalSize(), getSamplingRate()) * getSampleBitWidth();

    // Read sa
    ByteBuffer saBuf = ByteBuffer.allocate(BitUtils.bitsToBlocks64(totalSampledBits) * 8);
    dataChannel.read(saBuf);
    saBuf.rewind();
    sa = ThreadSafeLongBuffer.fromLongBuffer(saBuf.asLongBuffer());

    // Read sainv
    ByteBuffer isaBuf = ByteBuffer.allocate(BitUtils.bitsToBlocks64(totalSampledBits) * 8);
    dataChannel.read(isaBuf);
    isaBuf.rewind();
    isa = ThreadSafeLongBuffer.fromLongBuffer(isaBuf.asLongBuffer());

    // Read columnoffsets
    ByteBuffer coloffsetsBuf = ByteBuffer.allocate(getAlphabetSize() * 4);
    dataChannel.read(coloffsetsBuf);
    coloffsetsBuf.rewind();
    columnoffsets = ThreadSafeIntBuffer.fromIntBuffer(coloffsetsBuf.asIntBuffer());

    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = is.readInt();
      ByteBuffer columnBuf = ByteBuffer.allocate(columnSize);
      dataChannel.read(columnBuf);
      columns[i] = ThreadSafeByteBuffer.fromByteBuffer(((ByteBuffer) columnBuf.rewind()));
    }
  }

  /**
   * Slices, orders and limits ByteBuffer.
   *
   * @param buf  Buffer to slice, order and limit.
   * @param size Size to which buffer should be limited.
   * @return Sliced, ordered and limited buffer.
   */
  private ByteBuffer sliceOrderLimit(ByteBuffer buf, int size) {
    ByteBuffer ret = (ByteBuffer) buf.slice().order(ByteOrder.BIG_ENDIAN).limit(size);
    buf.position(buf.position() + size);
    return ret;
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  public void mapFromBuffer(ByteBuffer buf) {
    buf.rewind();

    metadata = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, 16));

    // Deserialize metadata
    setOriginalSize(metadata.getInt());
    setSamplingRate(metadata.getInt());
    setSampleBitWidth(metadata.getInt());
    setAlphabetSize(metadata.getInt());
    metadata.rewind();

    int alphabetmapSize = (getAlphabetSize() + 1) * (1 + 4 + 4);
    alphabetmap = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, alphabetmapSize));

    // Deserialize alphabetmap
    alphabetMap = new HashMap<>();
    for (int i = 0; i < getAlphabetSize() + 1; i++) {
      byte c = alphabetmap.get();
      int v1 = alphabetmap.getInt();
      int v2 = alphabetmap.getInt();
      alphabetMap.put(c, new Pair<>(v1, v2));
    }
    alphabetmap.rewind();

    // Read alphabet
    alphabet = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, getAlphabetSize()));

    // Compute number of sampled elements
    int totalSampledBits = CommonUtils.numBlocks(getOriginalSize(), getSamplingRate()) * getSampleBitWidth();

    // Read sa
    int saSize = BitUtils.bitsToBlocks64(totalSampledBits) * 8;
    sa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, saSize).asLongBuffer());

    // Read isa
    int isaSize = BitUtils.bitsToBlocks64(totalSampledBits) * 8;
    isa = ThreadSafeLongBuffer.fromLongBuffer(sliceOrderLimit(buf, isaSize).asLongBuffer());

    // Read columnoffsets
    int coloffsetsSize = getAlphabetSize() * 4;
    columnoffsets = ThreadSafeIntBuffer.fromIntBuffer(sliceOrderLimit(buf, coloffsetsSize).asIntBuffer());

    columns = new ThreadSafeByteBuffer[getAlphabetSize()];
    for (int i = 0; i < getAlphabetSize(); i++) {
      int columnSize = buf.getInt();
      columns[i] = ThreadSafeByteBuffer.fromByteBuffer(sliceOrderLimit(buf, columnSize));
      columns[i].rewind();
    }
  }

  /**
   * Write Succinct data structures to file.
   *
   * @param path Path to file where Succinct data structures should be written.
   * @throws IOException
   */
  public void writeToFile(String path) throws IOException {
    FileOutputStream fos = new FileOutputStream(path);
    DataOutputStream os = new DataOutputStream(fos);
    writeToStream(os);
  }

  /**
   * Read Succinct data structures into memory from file.
   *
   * @param path Path to serialized Succinct data structures.
   * @throws IOException
   */
  public void readFromFile(String path) throws IOException {
    FileInputStream fis = new FileInputStream(path);
    DataInputStream is = new DataInputStream(fis);
    readFromStream(is);
  }

  /**
   * Memory maps serialized Succinct data structures.
   *
   * @param path Path to serialized Succinct data structures.
   * @throws IOException
   */
  public void memoryMap(String path) throws IOException {
    File file = new File(path);
    long size = file.length();
    FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

    ByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
    mapFromBuffer(buf);
  }

  /**
   * Convert Succinct data-structures to a byte array.
   *
   * @return Byte array containing serialzied Succinct data structures.
   * @throws IOException
   */
  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    writeToStream(new DataOutputStream(bos));
    return bos.toByteArray();
  }

  /**
   * Read Succinct data structures from byte array.
   *
   * @param data Byte array to read data from.
   * @throws IOException
   */
  public void fromByteArray(byte[] data) throws IOException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    readFromStream(new DataInputStream(bis));
  }

  /**
   * Serialize SuccinctBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    writeToStream(new DataOutputStream(oos));
  }

  /**
   * Deserialize SuccinctBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    readFromStream(new DataInputStream(ois));
  }

}
