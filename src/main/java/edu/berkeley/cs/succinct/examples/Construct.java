package edu.berkeley.cs.succinct.examples;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctFileBuffer;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

public class Construct {
  public static void main(String[] args) throws IOException {
    if (args.length < 2 || args.length > 3) {
      System.err.println("Parameters: [input-path] [output-path] <[type]>");
      System.exit(-1);
    }

    SuccinctFileBuffer succinctFileBuffer;

    String type = "file";
    if (args.length == 3) {
      type = args[2];
    }

    long start = System.currentTimeMillis();
    if (type.equals("file")) {
      if (args[0].endsWith(".succinct")) {
        succinctFileBuffer = new SuccinctFileBuffer(args[0], StorageMode.MEMORY_ONLY);
      } else {
        File file = new File(args[0]);
        if (file.length() > 1L << 31) {
          System.err.println("Cant handle files > 2GB");
          System.exit(-1);
        }
        byte[] fileData = new byte[(int) file.length()];
        System.out.println("File size: " + fileData.length + " bytes");
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        dis.readFully(fileData, 0, (int) file.length());

        succinctFileBuffer = new SuccinctFileBuffer(fileData);
      }
    } else if (type.equals("indexed-file")) {
      if (args[0].endsWith(".succinct")) {
        succinctFileBuffer = new SuccinctIndexedFileBuffer(args[0], StorageMode.MEMORY_ONLY);
      } else {
        File file = new File(args[0]);
        if (file.length() > 1L << 31) {
          System.err.println("Cant handle files > 2GB");
          System.exit(-1);
        }
        byte[] fileData = new byte[(int) file.length()];
        System.out.println("File size: " + fileData.length + " bytes");
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        dis.readFully(fileData, 0, (int) file.length());

        ArrayList<Integer> positions = new ArrayList<Integer>();
        positions.add(0);
        for (int i = 0; i < fileData.length; i++) {
          if (fileData[i] == '\n') {
            positions.add(i + 1);
          }
        }
        int[] offsets = new int[positions.size()];
        for (int i = 0; i < offsets.length; i++) {
          offsets[i] = positions.get(i);
        }
        succinctFileBuffer = new SuccinctIndexedFileBuffer(fileData, offsets);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported mode: " + type);
    }
    long end = System.currentTimeMillis();
    System.out.println("Time to construct: " + (end - start) / 1000 + "s");

    succinctFileBuffer.writeToFile(args[1]);

    try {
      String memoryUsage = new String();
      List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean pool : pools) {
        MemoryUsage peak = pool.getPeakUsage();
        memoryUsage += String.format("Peak %s memory used: %,d%n", pool.getName(), peak.getUsed());
        memoryUsage += String.format("Peak %s memory reserved: %,d%n", pool.getName(), peak.getCommitted());
      }

      // we print the result in the console
      System.out.println(memoryUsage);

    } catch (Throwable t) {
      System.err.println("Exception in agent: " + t);
    }
  }
}
