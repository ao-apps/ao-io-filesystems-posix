/*
 * Copyright 2015 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.io.filesystems.unix;

import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.io.unix.UnixFile;
import com.aoindustries.util.StringUtility;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
 * @see  AOServDaemonProtocol#FAILOVER_FILE_REPLICATION_CHUNK_SIZE
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
		assert DIRECTORY_HASH_CHARACTERS >=1;
	}

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

	/**
	 * The time that orphans will be cleaned.
	 */
	private static final int
		CLEAN_ORPHANS_HOUR = 1,
		CLEAN_ORPHANS_MINUTE = 49
	;

	/**
	 * Only one instance is created per canonical index directory.
	 */
	private static final Map<File,DedupDataIndex> instances = new HashMap<>();

	/**
	 * Gets the index for the given index directory.
	 * Only one instance is created per canonical index directory.
	 */
	public static DedupDataIndex getInstance(File indexDirectory) throws IOException {
		File canonicalDirectory = indexDirectory.getCanonicalFile();
		synchronized(instances) {
			DedupDataIndex instance = instances.get(canonicalDirectory);
			if(instance == null) {
				instance = new DedupDataIndex(canonicalDirectory);
				instances.put(canonicalDirectory, instance);
			}
			return instance;
		}
	}

	private final UnixFile canonicalDirectory;

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
				name.append(StringUtility.getHexChar(hashDir >>> shift));
			} while(shift > 0);
			this.lockDirName = name.toString();
			UnixFile lockUF = new UnixFile(canonicalDirectory, lockDirName, false);
			File lockFile = lockUF.getFile();
			Stat stat = lockUF.getStat();
			if(!stat.exists()) {
				new FileOutputStream(lockFile).close();
				lockUF.setMode(FILE_MODE);
			} else if(!stat.isRegularFile()) {
				throw new FileSystemException("Not a regular file: " + lockUF.toString());
			}
			lockPath = lockFile.toPath();
		}

		@Override
		public String toString() {
			return
				DedupDataIndex.class.getName()
				+ '('
				+ canonicalDirectory
				+ ").hashLock("
				+ lockDirName
				+ ')'
			;
		}

		/**
		 * The channel for the currently held lock.
		 */
		private FileChannel channel;

		/**
		 * The currently opened lock.
		 */
		private FileLock lock;

		/**
		 * The thread currently holding the lock.
		 */
		private Thread thread;

		/**
		 * Gets an exclusive lock on this hash directory.  The lock must be
		 * released when done manipulating the directory.
		 * Must be used in a try/finally block.
		 * The locks are not reentrant.
		 * The obtained lock must not be given to another thread.
		 *
		 * @see  #unlock()
		 */
		private void lock() throws IOException {
			Thread currentThread = Thread.currentThread();
			synchronized(this) {
				// Avoid obvious deadlock scenario, these locks are not reentrant
				if(currentThread == thread) {
					throw new IllegalStateException("Thread already has the lock, locks are not reentrant: " + lockDirName);
				}
				// Wait for existing lock to be unlocked
				while(lock != null && lock.isValid()) {
					try {
						this.wait();
					} catch(InterruptedException e) {
						// Interruption is OK
					}
				}
				// Close old channel here, just to be extra safe
				if(channel != null) {
					channel.close();
					channel = null;
				}
				// Obtain lock
				channel = FileChannel.open(lockPath, StandardOpenOption.READ);
				lock = channel.lock();
				thread = currentThread;
			}
		}

		/**
		 * Unlocks this directory.  Must be called in a try/finally block.
		 * 
		 * @see  #lock()
		 */
		private void unlock() throws IOException {
			Thread currentThread = Thread.currentThread();
			synchronized(this) {
				if(thread != null) {
					if(thread != currentThread) {
						throw new IllegalStateException("Lock was not obtained by this thread: " + lockDirName);
					}
					thread = null;
				}
				if(lock != null) {
					this.notify();
					lock.close();
					lock = null;
				}
				if(channel != null) {
					channel.close();
					channel = null;
				}
			}
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
		synchronized(hashLocks) {
			HashDirectoryLock hashLock = hashLocks[hashDir];
			if(hashLock == null) {
				hashLock = new HashDirectoryLock(hashDir);
				hashLocks[hashDir] = hashLock;
			}
			return hashLock;
		}
	}
	// </editor-fold>

	private DedupDataIndex(File canonicalDirectory) throws IOException {
		this.canonicalDirectory = new UnixFile(canonicalDirectory);

		// Create the index directory if missing
		Stat stat = this.canonicalDirectory.getStat();
		if(!stat.exists()) {
			this.canonicalDirectory.mkdir(false, DIRECTORY_MODE);
		} else if(!stat.isDirectory()) {
			throw new IOException("Not a directory: " + this.canonicalDirectory);
		}

		/**
		 * Add the CronJob that cleans orphaned data in the background.
		 */
		CronJob cleanupJob = new CronJob() {
			@Override
			public Schedule getCronJobSchedule() {
				return
					(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year)
					-> minute==CLEAN_ORPHANS_MINUTE && hour==CLEAN_ORPHANS_HOUR
				;
			}
			@Override
			public CronJobScheduleMode getCronJobScheduleMode() {
				return CronJobScheduleMode.SKIP;
			}
			@Override
			public String getCronJobName() {
				return DedupDataIndex.class.getName()+".cleanOrphans()";
			}
			@Override
			public int getCronJobThreadPriority() {
				return Thread.NORM_PRIORITY;
			}
			@Override
			public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
				try {
					cleanOrphans();
				} catch(IOException e) {
					logger.log(Level.SEVERE, "clean orphans failed", e);
				}
			}
		};
		CronDaemon.addCronJob(cleanupJob, logger);
		// Clean once on startup
		CronDaemon.runImmediately(cleanupJob);
	}

	/**
	 * The directory containing this index.
	 */
	public File getCanonicalDirectory() {
		return canonicalDirectory.getFile();
	}

	/**
	 * Parses a hash directory name.
	 */
	private static int parseHashDir(String hex) throws NumberFormatException {
		if(hex.length() != DIRECTORY_HASH_CHARACTERS) throw new NumberFormatException("Hash directory must be " + DIRECTORY_HASH_CHARACTERS + " characters long: " + hex);
		int total = 0;
		int shift = HEX_BITS * DIRECTORY_HASH_CHARACTERS;
		int pos = 0;
		do {
			shift -= HEX_BITS;
			total |= StringUtility.getHex(hex.charAt(pos++)) << shift;
		} while(shift > 0);
		return total;
	}

	/**
	 * Cleans all orphaned index files.  The lock is only held briefly one file
	 * at a time, so other I/O can be interleaved with this cleanup process.
	 * It is possible that new orphans created during the cleanup will not be
	 * cleaned-up on this pass.
	 */
	public void cleanOrphans() throws IOException {
		String[] hashDirs = canonicalDirectory.list();
		if(hashDirs != null) {
			for(String hashDir : hashDirs) {
				try {
					final HashDirectoryLock hashDirLock = getHashLock(parseHashDir(hashDir));
					final UnixFile hashDirUF = new UnixFile(canonicalDirectory, hashDir, false);
					String[] list;
					hashDirLock.lock();
					try {
						list = hashDirUF.list();
					} finally {
						hashDirLock.unlock();
					}
					if(list != null) {
						boolean hasKeptFile = false;
						final Stat stat = new Stat();
						for(String filename : list) {
							UnixFile uf = new UnixFile(hashDirUF, filename, false);
							hashDirLock.lock();
							try {
								uf.getStat(stat);
								// Must still exist
								if(stat.exists()) {
									if(
										// Must be a regular file
										stat.isRegularFile()
										// Must have a link count of one
										&& stat.getNumberLinks() == 1
									) {
										logger.log(Level.WARNING, "Removing orphan: " + uf);
										uf.delete();
										// TODO: Renumber any files after this one by both collision# and link#
									} else {
										hasKeptFile = true;
									}
								}
							} finally {
								hashDirLock.unlock();
							}
							// We'll play extra nice by letting others grab the lock before
							// going on to the next file.
							Thread.yield();
						}
						list = null; // Done with this potentially long array

						// Remove the hash directory itself if now empty
						if(!hasKeptFile) {
							boolean logSkippedNonDirectory = false;
							hashDirLock.lock();
							try {
								hashDirUF.getStat(stat);
								if(stat.exists()) {
									if(stat.isDirectory()) {
										list = hashDirUF.list();
										if(list==null || list.length == 0) {
											logger.log(Level.WARNING, "Removing empty hash directory: " + hashDirUF);
											hashDirUF.delete();
										}
									} else {
										logSkippedNonDirectory = true;
									}
								}
							} finally {
								hashDirLock.unlock();
							}
							if(logSkippedNonDirectory) {
								logger.log(Level.WARNING, "Skipping non-directory: " + hashDir);
							}
						}
					}
				} catch(NumberFormatException e) {
					logger.log(Level.WARNING, "Skipping non-hash directory: " + hashDir, e);
				}
			}
		}
	}
}
