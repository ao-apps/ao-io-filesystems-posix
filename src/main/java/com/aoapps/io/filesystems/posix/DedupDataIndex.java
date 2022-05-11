/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.io.filesystems.FileLock;
import com.aoapps.io.filesystems.FileSystem;
import com.aoapps.io.filesystems.Path;
import com.aoapps.io.filesystems.PathIterator;
import com.aoapps.io.posix.Stat;
import com.aoapps.lang.Strings;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>
 * Each backup partition may optionally use a central index of data chunks for
 * all non-empty regular files.  Files are split into chunks of
 * <code>FAILOVER_FILE_REPLICATION_CHUNK_SIZE</code> bytes.  Each chunk may be
 * reused via hard links in order to achieve space savings.  Each chunk is also
 * optionally gzip compressed for additional space savings.  File tails (partial
 * chunks at the end less than <code>FAILOVER_FILE_REPLICATION_CHUNK_SIZE</code>
 * in length) are also added to the index.  A zero length chunk may never be
 * added.
 * </p>
 * <p>
 * The central index is a single layer directory hash, based on the first four
 * characters of the content's MD5 hash.  Because of this single layer hash, an
 * individual hash directory can easily have enough entries to require a
 * file system with support for many files in one directory, such as ext4 or xfs.
 * </p>
 * <p>
 * Files are named based on the lowercase hex-coded MD5 hash of the uncompressed
 * chunk contents.  However, due to both MD5 hash collisions and the per-file
 * hard link limits, there may be more than one file per MD5 hash value.  The
 * files are named as follows:
 * </p>
 * <pre>(/backup_partition)/DATA-INDEX/(directory_hash)/(remaining_hash)-(uncompressed_length)-(collision#)-(link#)[.gz][.corrupt]</pre>
 * <p>
 * The <code>directory_hash</code> is the first four characters of the MD5 sum.
 * </p>
 * <p>
 * The <code>remaining_hash</code> is the remaining 28 characters of the MD5 sum.
 * </p>
 * <p>
 * The <code>uncompressed_length</code> is the hex-coded length of the
 * uncompressed chunk contents.  When the length is a multiple of 0x100000
 * (1 MiB), it is represented with an "M" following the number of mebibytes in
 * hex.  When the length is a multiple of 0x400 (1 kiB), it is represented with
 * a "k" following the number of kibibytes in hex.
 * </p>
 * <p>
 * The uncompressed length is added to the filename to allow the efficient
 * location of candidate contents in the event of an MD5 collision.  Chunks with
 * a different size can be immediately excluded by filename without any
 * additional <code>stat</code> (for uncompressed) or full decompression.  Note
 * that if the file does not end with ".gz", this length will always equal the
 * actual file length.
 * </p>
 * <p>
 * The <code>collision#</code> is a zero-based hex counter for each unique set
 * of data resulting in this MD5 hash.  When locating new data from the index,
 * matches are not done by MD5 alone, the contents will be verified byte-by-byte.
 * When a second set of content must be added for a given MD5 hash, it will be
 * <code>(remaining_hash)-(uncompressed_length)-<em>1</em>-(link#)[.gz]</code>.
 * </p>
 * <p>
 * The <code>link#</code> is a zero-based hex counter to workaround the
 * file system limits on the number of hard links allowed to one file (65000 for
 * ext4).  Once the first file is "full", a second copy of the content is stored.
 * The second link file will be named
 * <code>(remaining_hash)-(uncompressed_length)-(collision#)-<em>1</em>[.gz]</code>
 * </p>
 * <p>
 * The <code>.gz</code> extension is added to chunks that have been gzip
 * compressed.  Chunks smaller than <code>FILE_SYSTEM_BLOCK_SIZE</code> are
 * never compressed as the space reduction will not yield any savings.
 * For larger files, the chunk is compressed, then the compressed version is
 * only used if it is sufficiently smaller to cross a
 * <code>FILE_SYSTEM_BLOCK_SIZE</code> block boundary in size.  This avoids
 * further compression overhead when the space reduction does not yield any
 * savings.
 * </p>
 * <p>
 * The <code>.corrupt</code> extension indicates that the background verifier
 * detected this chunk to no longer match the expected MD5 sum or chunk length
 * and the chunk could not be restored from another copy (see <code>link#</code>).
 * TODO: Can we restore this from backup and recover in-place and remove .corrupted from the filename?
 * This file will no longer be used for any new links, and links pointing to it
 * will be migrated to another copy of the data (see <code>link#</code>).  If
 * there is no other copy of the link, then the client will be asked to re-upload
 * the chunk.  During restore, an attempt will be made to locate an alternate
 * copy of the chunk.  Once all links are migrated, this corrupt chunk will be
 * deleted as normal when link count reaches one.
 * </p>
 * <p>
 * Both <code>collision#</code> and <code>link#</code> are maintained in sequential
 * order starting at <code>0</code>.  The system renumbers files as-needed as
 * things are removed in order to maintain no gaps in the sequence.  During routine
 * operations, searches are done one-past the end to detect and correct any gaps
 * in the sequence caused by any unclean shutdowns.
 * </p>
 * <p>
 * Once <code>DUPLICATE_LINK_COUNT</code> or more links are created to a data chunk,
 * a second copy of the data is created.  Once two copies exist, links will be
 * distributed between them evenly.  Only when both of the first two copies have
 * hit <code>FILE_SYSTEM_MAX_LINK_COUNT</code> links will a third copy be created.
 * </p>
 * <p>
 * Once the number of links in the first two copies reaches <code>COALESCE_LINK_COUNT</code>,
 * all new links are all put into the first and the second copy will be eventually
 * be removed once no longer referenced.
 * </p>
 * <p>
 * Files are normally removed from the index immediately as they are removed from
 * the backup directory trees.  However, in the event of an unclean shutdown or
 * manual administrative action, there may be orphaned index files (with a link
 * count of 1).  A cleanup job is ran at startup as well as once per day to find
 * and delete any orphaned index files.  This cleanup job can also be
 * accomplished manually on the shell:
 * </p>
 * <pre>
 * /etc/init.d/aoserv-daemon stop
 * find (/backup-partition)/DATA-INDEX -mindepth 2 -maxdepth 2 -type f -links 1 -print # -delete
 * find (/backup-partition)/DATA-INDEX -mindepth 1 -maxdepth 1 -type d -empty -print # -delete
 * # Note: -delete commented for safety, uncomment to actually delete the orphans.
 * /etc/init.d/aoserv-daemon start
 * </pre>
 * <p>
 * The backup process recreates missing index files from existing hard linked chunks,
 * so the entire index may be discarded and it will be recreated with minimal loss
 * of drive space.  Some links might not be created from new data to old (if not
 * yet put back in the index), but the system will still function and eventually
 * settle to an optimal state once again as backup directories are recycled.
 * </p>
 * <p>
 * The background verifier uses the chunk's modified time to keep track of the
 * last time the chunk was verified.  The chunk will be re-verified
 * approximately once every <code>VERIFICATION_INTERVAL</code> milliseconds.
 * </p>
 * <p>
 * Security: Client-provided MD5 values must never be trusted for what goes into
 * the index.  They can be used to link to existing data within the client's
 * backup, but anything being added to the index must have server-side MD5
 * computed.
 * </p>
 * <p>
 * Locks are maintained on a per-hash-directory basis, so the I/O can be
 * dispatched with up to 2^16 concurrency.  Locks are done within the JVM
 * using synchronized blocks, as well as between processes using file locking.
 * It is safe for multiple processes to use the same directory index concurrently.
 * All locks are exclusive for simplicity as concurrency is still obtained by the
 * sheer number of locks.
 * </p>
 * <p>
 * TODO: Keep track of number of in-JVM locks, only release file lock when all JVM locks released.
 * </p>
 *
 * @see  com.aoindustries.aoserv.daemon.client.AoservDaemonProtocol#FAILOVER_FILE_REPLICATION_CHUNK_SIZE
 *
 * @author  AO Industries, Inc.
 */
public class DedupDataIndex {

  private static final Logger logger = Logger.getLogger(DedupDataIndex.class.getName());

  /**
   * The maximum link count before creating a new copy of the data.
   * ext4 has a maximum of 65000, so this leaves some unused link count for
   * other administrative purposes.
   */
  private static final int FILE_SYSTEM_MAX_LINK_COUNT = 60000;

  /**
   * The number of links at which a second copy of the data is automatically created.
   */
  private static final int DUPLICATE_LINK_COUNT = 100;

  /**
   * The number of links when switching from duplicate storage back to single
   * storage.
   */
  private static final int COALESCE_LINK_COUNT = 50;

  /**
   * The page size assumed for the underlying file system.  This affects when
   * gzip compressed may be attempted.
   */
  private static final int FILE_SYSTEM_BLOCK_SIZE = 4096;

  /**
   * The number of bits in an MD5 sum.
   */
  private static final int MD5_SUM_BITS = 128;

  /**
   * The number of bits per hex character.
   */
  private static final int HEX_BITS = 4;

  /**
   * The number of bits of the MD5 sum used for the directory hash.
   * This must be a multiple of 4 for the hex encoding of filenames.
   */
  private static final int DIRECTORY_HASH_BITS = 16;

  static {
    assert DIRECTORY_HASH_BITS >= HEX_BITS && DIRECTORY_HASH_BITS <= (MD5_SUM_BITS - HEX_BITS);
    assert (DIRECTORY_HASH_BITS & (HEX_BITS - 1)) == 0 : "This must be a multiple of " + HEX_BITS + " for the hex encoding of filenames.";
  }

  /**
   * The number of hex characters in the directory hash filename.
   */
  private static final int DIRECTORY_HASH_CHARACTERS = DIRECTORY_HASH_BITS / HEX_BITS;

  static {
    assert DIRECTORY_HASH_CHARACTERS >= 1;
  }

  /**
   * The filename used to lock directories.
   */
  private static final String LOCK_FILE_NAME = "lock";

  /**
   * The index directory permissions.
   */
  private static final int DIRECTORY_MODE = 0700;

  /**
   * The index file permissions.
   */
  private static final int FILE_MODE = 0600;

  /**
   * The number of milliseconds between file verifications.
   */
  private static final long VERIFICATION_INTERVAL = 7L * 24L * 60L * 60L * 1000L; // 7 Days


  // <editor-fold defaultstate="collapsed" desc="Obtaining instances">
  private static class InstanceKey {

    private final PosixFileSystem fileSystem;
    private final Path dataIndexDir;

    private InstanceKey(PosixFileSystem fileSystem, Path dataIndexDir) {
      if (fileSystem != dataIndexDir.getFileSystem()) {
        throw new IllegalArgumentException("fileSystem and path.fileSystem do not match");
      }
      this.fileSystem = fileSystem;
      this.dataIndexDir = dataIndexDir;
    }

    @Override
    public int hashCode() {
      return dataIndexDir.getFileSystem().hashCode() * 31 + dataIndexDir.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof InstanceKey)) {
        return false;
      }
      InstanceKey other = (InstanceKey) obj;
      return
          fileSystem == other.fileSystem
              && dataIndexDir.equals(other.dataIndexDir);
    }
  }

  private static final Map<InstanceKey, DedupDataIndex> instances = new HashMap<>();

  /**
   * Gets the index for the given index directory.
   * Only one instance is created per file system instance and unique path.
   */
  public static DedupDataIndex getInstance(PosixFileSystem fileSystem, Path dataIndexDir) throws IOException {
    synchronized (instances) {
      InstanceKey key = new InstanceKey(fileSystem, dataIndexDir);
      DedupDataIndex instance = instances.get(key);
      if (instance == null) {
        instance = new DedupDataIndex(fileSystem, dataIndexDir);
        instances.put(key, instance);
      }
      return instance;
    }
  }
  // </editor-fold>

  private final PosixFileSystem fileSystem;
  private final Path dataIndexDir;

  private DedupDataIndex(PosixFileSystem fileSystem, Path dataIndexDir) throws IOException {
    this.fileSystem = fileSystem;
    this.dataIndexDir = dataIndexDir;

    // Create the index directory if missing
    Stat stat = fileSystem.stat(dataIndexDir);
    if (!stat.exists()) {
      fileSystem.createDirectory(dataIndexDir, DIRECTORY_MODE);
    } else if (!stat.isDirectory()) {
      throw new IOException("Not a directory: " + this.dataIndexDir);
    }
  }

  /**
   * The file system containing this index.
   */
  public PosixFileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Returns the path (within the file system) containing this index.
   */
  public Path getDataIndexDir() {
    return dataIndexDir;
  }

  /**
   * Parses a hash directory name.
   */
  private static int parseHashDir(String hex) throws NumberFormatException {
    if (hex.length() != DIRECTORY_HASH_CHARACTERS) {
      throw new NumberFormatException("Hash directory must be " + DIRECTORY_HASH_CHARACTERS + " characters long: " + hex);
    }
    int total = 0;
    int shift = HEX_BITS * DIRECTORY_HASH_CHARACTERS;
    int pos = 0;
    do {
      shift -= HEX_BITS;
      @SuppressWarnings("deprecation")
      int hexVal = Strings.getHex(hex.charAt(pos++));
      total |= hexVal << shift;
    } while (shift > 0);
    return total;
  }

  // <editor-fold defaultstate="collapsed" desc="Hash directory locking">
  /**
   * Obtains a file lock when created.
   */
  private class HashDirectoryLock {

    private final String lockDirName;
    private final Path lockPath;

    private HashDirectoryLock(int hashDir) throws IOException {
      //this.hashDir = hashDir;
      StringBuilder name = new StringBuilder(DIRECTORY_HASH_CHARACTERS);
      int shift = HEX_BITS * DIRECTORY_HASH_CHARACTERS;
      do {
        shift -= HEX_BITS;
        @SuppressWarnings("deprecation")
        char hexChar = Strings.getHexChar(hashDir >>> shift);
        name.append(hexChar);
      } while (shift > 0);
      this.lockDirName = name.toString();
      this.lockPath = new Path(
          new Path(dataIndexDir, lockDirName),
          LOCK_FILE_NAME
      );
      Stat stat = fileSystem.stat(lockPath);
      if (!stat.exists()) {
        try {
          fileSystem.createFile(lockPath, FILE_MODE);
        } catch (IOException e) {
          // Check race condition: OK if some other process created the file
          stat = fileSystem.stat(lockPath);
          if (!stat.exists() || !stat.isRegularFile()) {
            throw e;
          }
        }
      } else if (!stat.isRegularFile()) {
        throw new FileSystemException("Not a regular file: " + lockPath);
      }
    }

    @Override
    public String toString() {
      return
          DedupDataIndex.class.getName()
              + '('
              + DedupDataIndex.this.dataIndexDir
              + ").hashLock("
              + lockDirName
              + ')';
    }

    /**
     * The currently opened lock.
     */
    private FileLock lock;

    /**
     * The thread currently holding the lock.
     */
    private Thread thread;

    /**
     * Gets an exclusive lock on this hash directory.
     *
     * @see  FileSystem#lock(com.aoapps.io.filesystems.Path)
     */
    private FileLock lock() throws IOException {
      return fileSystem.lock(lockPath);
    }
  }

  /**
   * Per hash locks (one for each hash sub directory).
   */
  private final HashDirectoryLock[] hashLocks = new HashDirectoryLock[2 ^ DIRECTORY_HASH_BITS];

  /**
   * Gets the lock for a specific hash directory, never removed once created.
   */
  private HashDirectoryLock getHashLock(int hashDir) throws IOException {
    synchronized (hashLocks) {
      HashDirectoryLock hashLock = hashLocks[hashDir];
      if (hashLock == null) {
        hashLock = new HashDirectoryLock(hashDir);
        hashLocks[hashDir] = hashLock;
      }
      return hashLock;
    }
  }

  // </editor-fold>

  // <editor-fold defaultstate="collapsed" desc="Background verification">
  /**
   * Cleans all orphaned index files.  The lock is only held briefly one file
   * at a time, so other I/O can be interleaved with this cleanup process.
   * It is possible that new orphans created during the cleanup will not be
   * cleaned-up on this pass.
   * <p>
   * For long-lived data indexes, it is good to run this once per day during low usage times.
   * </p>
   *
   * @param  quick  When true, performs a quick pass to clean orphaned data
   *                only, but does not verify MD5 sums.
   */
  @SuppressWarnings("CallToThreadYield")
  public void verify(boolean quick) throws IOException {
    try (PathIterator dataIndexIter = fileSystem.list(dataIndexDir)) {
      while (dataIndexIter.hasNext()) {
        Path hashDirPath = dataIndexIter.next();
        String hashDirFilename = hashDirPath.getName();
        // Skip lock files
        if (!LOCK_FILE_NAME.equals(hashDirFilename)) {
          int hashDir;
          try {
            hashDir = parseHashDir(hashDirFilename);
          } catch (NumberFormatException e) {
            logger.log(Level.WARNING, "Skipping non-hash directory: " + hashDirPath, e);
            continue;
          }
          final HashDirectoryLock hashDirLock = getHashLock(hashDir);
          PathIterator list = null;
          try {
            try (FileLock lock = hashDirLock.lock()) {
              assert lock != null; // Java 9: fix: Avoid warning: "auto-closeable resource lock is never referenced in body of corresponding try statement"
              list = fileSystem.list(hashDirPath);
            } catch (NoSuchFileException | NotDirectoryException e) {
              // These are OK since we're working on a live file system
              // list remains null
            }
            if (list != null) {
              while (list.hasNext()) {
                Path file = list.next();
                String filename = file.getName();
                // Skip lock files
                if (!LOCK_FILE_NAME.equals(filename)) {
                  try (FileLock lock = hashDirLock.lock()) {
                    assert lock != null; // Java 9: fix: Avoid warning: "auto-closeable resource lock is never referenced in body of corresponding try statement"
                    Stat stat = fileSystem.stat(file);
                    // Must still exist
                    if (stat.exists()) {
                      if (
                          // Must be a regular file
                          stat.isRegularFile()
                              // Must have a link count of one
                              && stat.getNumberLinks() == 1
                      ) {
                        logger.log(Level.WARNING, "Removing orphan: " + file);
                        fileSystem.delete(file);
                        // TODO: Renumber any files after this one by both collision# and link#
                        //       (or move highest number into first empty slot)
                      }
                    }
                  }
                  // We'll play extra nice by letting others grab the lock before
                  // going on to the next file.
                  Thread.yield();
                }
              }
            }
          } finally {
            if (list != null) {
              list.close();
            }
          }
        }
      }
    }
  }
  // </editor-fold>
}
