package edu.berkeley.cs.succinct;

import edu.berkeley.cs.succinct.util.container.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class SuccinctCore implements Serializable {

  // End of File marker
  public transient static final byte EOF = -127;

  // END of Alphabet marker
  public transient static final byte EOA = -126;

  // End of Line marker
  public transient static final byte EOL = '\n';

  // Deserialized data-structures
  protected transient HashMap<Byte, Pair<Integer, Integer>> alphabetMap;
  protected transient Map<Long, Long> contextMap;

  // Metadata
  private transient int originalSize;
  private transient int alphabetSize;
  private transient int samplingRate;
  private transient int sampleBitWidth;

  public SuccinctCore() {
  }

  /**
   * Get the original size.
   *
   * @return The originalSize.
   */
  public int getOriginalSize() {
    return originalSize;
  }

  /**
   * Set the original size.
   *
   * @param originalSize The originalSize to set.
   */
  public void setOriginalSize(int originalSize) {
    this.originalSize = originalSize;
  }

  /**
   * Get the alpha size.
   *
   * @return The alphabetSize.
   */
  public int getAlphabetSize() {
    return alphabetSize;
  }

  /**
   * Set the alpha size.
   *
   * @param alphabetSize The alphabetSize to set.
   */
  public void setAlphabetSize(int alphabetSize) {
    this.alphabetSize = alphabetSize;
  }

  /**
   * Get the sampling rate.
   *
   * @return The samplingRate.
   */
  public int getSamplingRate() {
    return samplingRate;
  }

  /**
   * Set the sampling rate.
   *
   * @param samplingRate The samplingRate to set.
   */
  public void setSamplingRate(int samplingRate) {
    this.samplingRate = samplingRate;
  }

  /**
   * Get the sample bit width.
   *
   * @return The sample bit width.
   */
  public int getSampleBitWidth() {
    return sampleBitWidth;
  }

  /**
   * Set the sample bit width.
   *
   * @param sampleBitWidth The sample bit width to set.
   */
  public void setSampleBitWidth(int sampleBitWidth) {
    this.sampleBitWidth = sampleBitWidth;
  }

  /**
   * Lookup NPA at specified index.
   *
   * @param i Index into NPA.
   * @return Value of NPA at specified index.
   */
  public abstract long lookupNPA(long i);

  /**
   * Lookup SA at specified index.
   *
   * @param i Index into SA.
   * @return Value of SA at specified index.
   */
  public abstract long lookupSA(long i);

  /**
   * Lookup ISA at specified index.
   *
   * @param i Index into ISA.
   * @return Value of ISA at specified index.
   */
  public abstract long lookupISA(long i);

  /**
   * Lookup up the inverted alphabet map at specified index.
   *
   * @param i Index into inverted alphabet map
   * @return Value of inverted alphabet map at specified index.
   */
  public abstract byte lookupC(long i);

  /**
   * Binary Search for a value withing NPA.
   *
   * @param val      Value to be searched.
   * @param startIdx Starting index into NPA.
   * @param endIdx   Ending index into NPA.
   * @param flag     Whether to search for left or the right boundary.
   * @return Search result as an index into the NPA.
   */
  public abstract long binSearchNPA(long val, long startIdx, long endIdx, boolean flag);
}
