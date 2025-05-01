/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2009, 2010, 2011, 2013, 2015, 2018, 2019, 2020, 2021, 2022, 2024, 2025  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-io-filesystems-posix.
 *
 * ao-io-filesystems-posix is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-io-filesystems-posix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-io-filesystems-posix.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.io.filesystems.posix;

import com.aoapps.hodgepodge.io.FilesystemIterator;
import com.aoapps.hodgepodge.io.FilesystemIteratorRule;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.io.posix.PosixFile;
import com.aoapps.io.posix.Stat;
import com.aoapps.lang.util.BufferManager;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPOutputStream;

/**
 * Our backup directories contain parallel directories with many hard links.
 * rsync and tar both use extreme amounts of RAM to manipulate these
 * directories and often fail or become extremely slow due to excessive
 * swapping.
 *
 * <p>To work around this problem and be able to move directory trees from host to
 * host, this tool will combine the set of directories and write them to
 * <code>System.out</code>.  This is similar to tar.  The output is then
 * unpacked using <code>ParallelUnpack</code>, which could be a direct pipe,
 * through <code>ssh</code>, <code>nc</code>, or any other mechanism.</p>
 *
 * <p>For efficiency, direct TCP communication is supported with the <code>-h</code> option.</p>
 *
 * <p>It assumes that the file system is not changing, results of use on a changing
 * filesystem is not defined.</p>
 *
 * @see  ParallelUnpack
 *
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class ParallelPack {

  /** Make no instances. */
  private ParallelPack() {
    throw new AssertionError();
  }

  /**
   * The size of the verbose output queue.
   */
  private static final int VERBOSE_QUEUE_SIZE = 1000;

  /**
   * Packs multiple directories in parallel (but not concurrently).
   */
  @SuppressWarnings("AssignmentToForLoopParameter")
  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("Usage: " + ParallelPack.class.getName() + " [-d root] [-h host] [-p port] [-v] [--] path {path}");
      System.err.println("\t-d\tRead from a deduplicated filesystem at the given root, paths are relative to this root");
      System.err.println("\t-h\tWill connect to host instead of writing to standard out");
      System.err.println("\t-p\tWill connect to port instead of port " + PackProtocol.DEFAULT_PORT);
      System.err.println("\t-v\tWrite the full path to standard error as each file is packed");
      System.err.println("\t-z\tCompress the output");
      System.err.println("\t--\tEnd options, all additional arguments will be interpreted as paths");
      System.exit(1);
    } else {
      List<PosixFile> directories = new ArrayList<>(args.length);
      PrintStream verboseOutput = null;
      boolean compress = false;
      String host = null;
      int port = PackProtocol.DEFAULT_PORT;
      boolean optionsEnded = false;
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (!optionsEnded && arg.equals("-v")) {
          verboseOutput = System.err;
        } else if (!optionsEnded && arg.equals("-h")) {
          i++;
          if (i < args.length) {
            host = args[i];
          } else {
            throw new IllegalArgumentException("Expecting host after -h");
          }
        } else if (!optionsEnded && arg.equals("-p")) {
          i++;
          if (i < args.length) {
            port = Integer.parseInt(args[i]);
          } else {
            throw new IllegalArgumentException("Expecting port after -p");
          }
        } else if (!optionsEnded && arg.equals("--")) {
          optionsEnded = true;
        } else if (!optionsEnded && arg.equals("-z")) {
          compress = true;
        } else {
          directories.add(new PosixFile(arg));
        }
      }
      try {
        if (host != null) {
          try (
              Socket socket = new Socket(host, port);
              OutputStream out = socket.getOutputStream();
              InputStream in = socket.getInputStream()
              ) {
            parallelPack(directories, out, verboseOutput, compress);
            int resp = in.read();
            if (resp == -1) {
              throw new EOFException("End of file while reading completion confirmation");
            }
            if (resp != PackProtocol.END) {
              throw new IOException("Unexpected value while reading completion confirmation");
            }
          }
        } else {
          // System.out
          parallelPack(directories, System.out, verboseOutput, compress);
        }
      } catch (IOException err) {
        err.printStackTrace(System.err);
        System.err.flush();
        System.exit(2);
      }
    }
  }

  private static class LinkAndCount {
    final long linkId;
    int linkCount;

    LinkAndCount(long linkId, int linkCount) {
      this.linkId = linkId;
      this.linkCount = linkCount;
    }
  }

  static class FilesystemIteratorAndSlot {
    final FilesystemIterator iterator;
    final int slot;

    FilesystemIteratorAndSlot(FilesystemIterator iterator, int slot) {
      this.iterator = iterator;
      this.slot = slot;
    }
  }

  /**
   * Packs to the provided output stream.  The stream is flushed and closed.
   */
  public static void parallelPack(List<PosixFile> directories, OutputStream out, final PrintStream verboseOutput, boolean compress) throws IOException {
    // Reused throughout method
    final int numDirectories = directories.size();

    // The set of next files is kept in key order so that it can scale with O(n*log(n)) for larger numbers of directories
    // as opposed to O(n^2) for a list.  This is similar to the fix for AWStats logresolvemerge provided by Dan Armstrong
    // a couple of years ago.
    final Map<String, List<FilesystemIteratorAndSlot>> nextFiles = new TreeMap<>(
        (String s1, String s2) -> {
          // Make sure directories are sorted after their directory contents
          int diff = s1.compareTo(s2);
          if (diff == 0) {
            return 0;
          }
          if (s2.startsWith(s1)) {
            return 1;
          }
          if (s1.startsWith(s2)) {
            return -1;
          }
          return diff;
        }
    );
    {
      int nextSlot = 0;
      final Map<String, FilesystemIteratorRule> prefixRules = Collections.emptyMap();
      for (PosixFile directory : directories) {
        Stat stat = directory.getStat();
        if (!stat.exists()) {
          throw new IOException("Directory not found: " + directory.getPath());
        }
        if (!stat.isDirectory()) {
          throw new IOException("Not a directory: " + directory.getPath());
        }
        String path = directory.getFile().getCanonicalPath();
        Map<String, FilesystemIteratorRule> rules = Collections.singletonMap(path, FilesystemIteratorRule.OK);
        FilesystemIterator iterator = new FilesystemIterator(rules, prefixRules, path, true, true);
        File nextFile = iterator.getNextFile();
        if (nextFile != null) {
          String relPath = getRelativePath(nextFile, iterator);
          List<FilesystemIteratorAndSlot> list = nextFiles.get(relPath);
          if (list == null) {
            list = new ArrayList<>(numDirectories);
            nextFiles.put(relPath, list);
          }
          list.add(new FilesystemIteratorAndSlot(iterator, nextSlot++));
          if (nextSlot > 62) {
            nextSlot = 0;
          }
        }
      }
    }

    final BlockingQueue<String> verboseQueue;
    final boolean[] verboseThreadRun;
    Thread verboseThread;
    if (verboseOutput == null) {
      verboseQueue = null;
      verboseThreadRun = null;
      verboseThread = null;
    } else {
      verboseQueue = new ArrayBlockingQueue<>(VERBOSE_QUEUE_SIZE);
      verboseThreadRun = new boolean[]{true};
      verboseThread = new Thread("ParallelPack - Verbose Thread") {
        @Override
        public void run() {
          while (!Thread.currentThread().isInterrupted()) {
            synchronized (verboseThreadRun) {
              if (!verboseThreadRun[0] && verboseQueue.isEmpty()) {
                break;
              }
            }
            try {
              verboseOutput.println(verboseQueue.take());
              if (verboseQueue.isEmpty()) {
                verboseOutput.flush();
              }
            } catch (InterruptedException err) {
              err.printStackTrace(System.err);
              // Restore the interrupted status
              Thread.currentThread().interrupt();
            }
          }
        }
      };

      verboseThread.start();
    }
    try {
      // Hard link management
      long nextLinkId = 1; // LinkID of 0 is reserved for no link
      // This is a mapping from device->inode->linkId
      Map<Long, Map<Long, LinkAndCount>> deviceInodeIdMap = new HashMap<>();

      StreamableOutput streamOut = new StreamableOutput(out);
      try {
        // Header
        for (int c = 0, len = PackProtocol.HEADER.length(); c < len; c++) {
          streamOut.write(PackProtocol.HEADER.charAt(c));
        }
        // Version
        streamOut.writeInt(PackProtocol.VERSION);
        streamOut.writeBoolean(compress);
        if (compress) {
          streamOut = new StreamableOutput(new GZIPOutputStream(out, PackProtocol.BUFFER_SIZE));
        }
        // Reused in main loop
        final StringBuilder sb = new StringBuilder();
        final byte[] buffer = PackProtocol.BUFFER_SIZE == BufferManager.BUFFER_SIZE ? BufferManager.getBytes() : new byte[PackProtocol.BUFFER_SIZE];
        try {
          // Main loop, continue until nextFiles is empty
          while (true) {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedIOException();
            }
            Iterator<String> iter = nextFiles.keySet().iterator();
            if (!iter.hasNext()) {
              break;
            }
            String relPath = iter.next();
            for (FilesystemIteratorAndSlot iteratorAndSlot : nextFiles.remove(relPath)) {
              FilesystemIterator iterator = iteratorAndSlot.iterator;
              // Get the full path on this machine
              sb.setLength(0);
              String startPath = iterator.getStartPath();
              sb.append(startPath);
              sb.append(relPath);
              String fullPath = sb.toString();
              final PosixFile uf = new PosixFile(fullPath);
              // Get the pack path
              sb.setLength(0);
              int lastSlashPos = startPath.lastIndexOf(File.separatorChar);
              if (lastSlashPos == -1) {
                sb.append(startPath);
              } else {
                sb.append(startPath, lastSlashPos, startPath.length());
              }
              sb.append(relPath);
              String packPath = sb.toString();
              // Verbose output
              if (verboseQueue != null) {
                try {
                  verboseQueue.put(packPath);
                } catch (InterruptedException err) {
                  // Restore the interrupted status
                  Thread.currentThread().interrupt();
                  InterruptedIOException ioErr = new InterruptedIOException();
                  ioErr.initCause(err);
                  throw ioErr;
                }
              }

              // Handle this file
              Stat stat = uf.getStat();
              if (stat.isRegularFile()) {
                streamOut.writeByte(PackProtocol.REGULAR_FILE);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                int numLinks = stat.getNumberLinks();
                if (numLinks == 1) {
                  // No hard links
                  streamOut.writeLong(0);
                  streamOut.writeInt(stat.getUid());
                  streamOut.writeInt(stat.getGid());
                  streamOut.writeLong(stat.getMode());
                  streamOut.writeLong(stat.getModifyTime());
                  writeFile(uf, streamOut, buffer);
                } else if (numLinks > 1) {
                  // Has hard links
                  // Look for already found
                  Long device = stat.getDevice();
                  Long inode = stat.getInode();
                  Map<Long, LinkAndCount> inodeMap = deviceInodeIdMap.get(device);
                  if (inodeMap == null) {
                    deviceInodeIdMap.put(device, inodeMap = new HashMap<>());
                  }
                  LinkAndCount linkAndCount = inodeMap.get(inode);
                  if (linkAndCount != null) {
                    // Already sent, send the link ID and decrement our count
                    streamOut.writeLong(linkAndCount.linkId);
                    if (--linkAndCount.linkCount <= 0) {
                      inodeMap.remove(inode);
                      // This keeps memory tighter but can increase overhead by making many new maps:
                      // if (inodeMap.isEmpty()) {
                      //   deviceInodeIdMap.remove(device);
                      // }
                    }
                  } else {
                    // New file, send file data
                    long linkId = nextLinkId++;
                    streamOut.writeLong(linkId);
                    streamOut.writeInt(stat.getUid());
                    streamOut.writeInt(stat.getGid());
                    streamOut.writeLong(stat.getMode());
                    streamOut.writeLong(stat.getModifyTime());
                    streamOut.writeInt(numLinks);
                    writeFile(uf, streamOut, buffer);
                    inodeMap.put(inode, new LinkAndCount(linkId, numLinks - 1));
                  }
                } else {
                  throw new IOException("Invalid link count: " + numLinks);
                }
              } else if (stat.isDirectory()) {
                streamOut.writeByte(PackProtocol.DIRECTORY);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                streamOut.writeInt(stat.getUid());
                streamOut.writeInt(stat.getGid());
                streamOut.writeLong(stat.getMode());
                streamOut.writeLong(stat.getModifyTime());
              } else if (stat.isSymLink()) {
                streamOut.writeByte(PackProtocol.SYMLINK);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                streamOut.writeInt(stat.getUid());
                streamOut.writeInt(stat.getGid());
                streamOut.writeCompressedUTF(uf.readLink(), 63);
              } else if (stat.isBlockDevice()) {
                streamOut.writeByte(PackProtocol.BLOCK_DEVICE);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                streamOut.writeInt(stat.getUid());
                streamOut.writeInt(stat.getGid());
                streamOut.writeLong(stat.getMode());
                streamOut.writeLong(stat.getDeviceIdentifier());
              } else if (stat.isCharacterDevice()) {
                streamOut.writeByte(PackProtocol.CHARACTER_DEVICE);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                streamOut.writeInt(stat.getUid());
                streamOut.writeInt(stat.getGid());
                streamOut.writeLong(stat.getMode());
                streamOut.writeLong(stat.getDeviceIdentifier());
              } else if (stat.isFifo()) {
                streamOut.writeByte(PackProtocol.FIFO);
                streamOut.writeCompressedUTF(packPath, iteratorAndSlot.slot);
                streamOut.writeInt(stat.getUid());
                streamOut.writeInt(stat.getGid());
                streamOut.writeLong(stat.getMode());
              } else if (stat.isSocket()) {
                throw new IOException("Unable to pack socket: " + uf.getPath());
              }
              // Get the next file
              File nextFile = iterator.getNextFile();
              if (nextFile != null) {
                String newRelPath = getRelativePath(nextFile, iterator);
                List<FilesystemIteratorAndSlot> list = nextFiles.get(newRelPath);
                if (list == null) {
                  list = new ArrayList<>(numDirectories);
                  nextFiles.put(newRelPath, list);
                }
                list.add(iteratorAndSlot);
              }
            }
          }
        } finally {
          if (PackProtocol.BUFFER_SIZE == BufferManager.BUFFER_SIZE) {
            BufferManager.release(buffer, false);
          }
        }
        streamOut.writeByte(PackProtocol.END);
      } finally {
        streamOut.flush();
        streamOut.close();
      }
      // TODO: If verbose, warn for any hard links that didn't all get packed
    } finally {
      // Wait for verbose queue to be empty
      if (verboseThread != null) {
        synchronized (verboseThreadRun) {
          verboseThreadRun[0] = false;
        }
        try {
          verboseThread.join();
        } catch (InterruptedException err) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          InterruptedIOException ioErr = new InterruptedIOException();
          ioErr.initCause(err);
          throw ioErr;
        }
      }
    }
  }

  private static void writeFile(PosixFile uf, DataOutput out, byte[] buffer) throws IOException {
    try (InputStream in = new FileInputStream(uf.getFile())) {
      int ret;
      while ((ret = in.read(buffer, 0, PackProtocol.BUFFER_SIZE)) != -1) {
        if (ret < 0 || ret > Short.MAX_VALUE) {
          throw new IOException("ret out of range: " + ret);
        }
        out.writeShort(ret);
        out.write(buffer, 0, ret);
      }
      out.writeShort(-1);
    }
  }

  /**
   * Gets the relative path for the provided file from the provided iterator.
   */
  private static String getRelativePath(File file, FilesystemIterator iterator) throws IOException {
    String path = file.getPath();
    String prefix = iterator.getStartPath();
    if (!path.startsWith(prefix)) {
      throw new IOException("path doesn't start with prefix: path=\"" + path + "\", prefix=\"" + prefix + "\"");
    }
    return path.substring(prefix.length());
  }
}
