/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2009, 2010, 2011, 2013, 2015, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.io.posix.PosixFile;
import com.aoapps.io.posix.Stat;
import com.aoapps.lang.util.BufferManager;
import java.io.DataInput;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPInputStream;

/**
 * <p>
 * Unpacks the files that have been packed by ParallelPack.
 * </p>
 * <p>
 * For efficiency, direct TCP communication is supported with the <code>-l</code> option.
 * </p>
 * <p>
 * It assumes that the file system is not changing, results of use on a changing
 * filesystem is not defined.
 * </p>
 *
 * @see  ParallelPack
 *
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class ParallelUnpack {

	/** Make no instances. */
	private ParallelUnpack() {throw new AssertionError();}

	/**
	 * The size of the verbose output queue.
	 */
	private static final int VERBOSE_QUEUE_SIZE = 1000;

	/**
	 * Unpacks multiple directories in parallel (but not concurrently).
	 */
	@SuppressWarnings("AssignmentToForLoopParameter")
	public static void main(String[] args) {
		if(args.length == 0) {
			System.err.println("Usage: "+ParallelUnpack.class.getName()+" [-d root] [-l] [-h host] [-p port] [-n] [-v] [--] path");
			System.err.println("\t-d\tWrite to a deduplicated filesystem at the given root, paths are relative to this root");
			System.err.println("\t-l\tWill listen for an incoming connection instead of reading from standard in");
			System.err.println("\t-h\tWill listen on the interface matching host");
			System.err.println("\t-p\tWill listen on port instead of port "+PackProtocol.DEFAULT_PORT);
			System.err.println("\t-n\tPerform dry run, do not modify the filesystem");
			System.err.println("\t-f\tOverwrite existing files");
			System.err.println("\t-v\tWrite the full path to standard error as each file is unpacked");
			System.err.println("\t--\tEnd options, any additional argument will be interpreted as a path");
			System.exit(1);
		} else {
			String path = null;
			PrintStream verboseOutput = null;
			boolean listen = false;
			String host = null;
			int port = PackProtocol.DEFAULT_PORT;
			boolean dryRun = false;
			boolean force = false;
			boolean optionsEnded = false;
			for(int i=0; i<args.length; i++) {
				String arg = args[i];
				if(!optionsEnded && arg.equals("-v")) verboseOutput = System.err;
				else if(!optionsEnded && arg.equals("-l")) listen = true;
				else if(!optionsEnded && arg.equals("-h")) {
					i++;
					if(i<args.length) host = args[i];
					else throw new IllegalArgumentException("Expecting host after -h");
				} else if(!optionsEnded && arg.equals("-p")) {
					i++;
					if(i<args.length) port = Integer.parseInt(args[i]);
					else throw new IllegalArgumentException("Expecting port after -p");
				} else if(!optionsEnded && arg.equals("-n")) dryRun = true;
				else if(!optionsEnded && arg.equals("-f")) force = true;
				else if(!optionsEnded && arg.equals("--")) optionsEnded = true;
				else {
					if(path != null) throw new IllegalArgumentException("More than one path in arguments");
					path = arg;
				}
			}
			try {
				if(listen) {
					Socket socket = null;
					try {
						// Accept only one TCP connection
						try (
							ServerSocket ss = host == null ? new ServerSocket(port, 1) : new ServerSocket(port, 1, InetAddress.getByName(host))
						) {
							socket = ss.accept();
						}
						try (
							OutputStream out = socket.getOutputStream();
							InputStream in = socket.getInputStream()) {
							parallelUnpack(path, in, verboseOutput, dryRun, force);
							out.write(PackProtocol.END);
							out.flush();
						}
					} finally {
						if(socket != null) socket.close();
					}
				} else {
					// System.in
					parallelUnpack(path, System.in, verboseOutput, dryRun, force);
				}
			} catch(IOException err) {
				err.printStackTrace(System.err);
				System.err.flush();
				System.exit(2);
			}
		}
	}

	private static class PathAndCount {
		final String path;
		int linkCount;
		PathAndCount(String path, int linkCount) {
			this.path = path;
			this.linkCount = linkCount;
		}
	}

	static class PathAndModifyTime {
		final String path;
		final long modifyTime;
		PathAndModifyTime(String path, long modifyTime) {
			this.path = path;
			this.modifyTime = modifyTime;
		}
	}

	/**
	 * Unpacks from the provided output stream.  The stream is flushed but not closed.
	 */
	public static void parallelUnpack(String path, InputStream in, final PrintStream verboseOutput, boolean dryRun, boolean force) throws IOException {
		final PosixFile destination = new PosixFile(path);
		Stat destinationStat = destination.getStat();
		if(!destinationStat.exists()) throw new IOException("Directory not found: "+destination.getPath());
		if(!destinationStat.isDirectory()) throw new IOException("Not a directory: "+destination.getPath());

		final BlockingQueue<String> verboseQueue;
		final boolean[] verboseThreadRun;
		Thread verboseThread;
		if(verboseOutput == null) {
			verboseQueue = null;
			verboseThreadRun = null;
			verboseThread = null;
		} else {
			verboseQueue = new ArrayBlockingQueue<>(VERBOSE_QUEUE_SIZE);
			verboseThreadRun = new boolean[] {true};
			verboseThread = new Thread("ParallelUnpack - Verbose Thread") {
				@Override
				public void run() {
					while(!Thread.currentThread().isInterrupted()) {
						synchronized(verboseThreadRun) {
							if(!verboseThreadRun[0] && verboseQueue.isEmpty()) break;
						}
						try {
							verboseOutput.println(verboseQueue.take());
							if(verboseQueue.isEmpty()) verboseOutput.flush();
						} catch(InterruptedException err) {
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
			StreamableInput streamIn = new StreamableInput(in);
			// Header
			for(int c=0, len=PackProtocol.HEADER.length(); c<len; c++) {
				int ch = streamIn.read();
				if(ch==-1) throw new EOFException("End of file while reading header");
				if(ch!=PackProtocol.HEADER.charAt(c)) throw new IOException("ParallelPack header not found");
			}
			// Version
			int version = streamIn.readInt();
			if(version!=PackProtocol.VERSION) throw new IOException("Unsupported pack version "+version+", expecting version "+PackProtocol.VERSION);
			boolean compress = streamIn.readBoolean();
			if(compress) streamIn = new StreamableInput(new GZIPInputStream(in, PackProtocol.BUFFER_SIZE));
			// Reused in main loop
			final StringBuilder sb = new StringBuilder();
			final byte[] buffer = PackProtocol.BUFFER_SIZE == BufferManager.BUFFER_SIZE ? BufferManager.getBytes() : new byte[PackProtocol.BUFFER_SIZE];
			try {
				// Hard link management
				final Map<Long, PathAndCount> linkPathAndCounts = new HashMap<>();
				// Directory modify time management
				final Map<String, Stack<PathAndModifyTime>> directoryModifyTimes = new HashMap<>();
				try {
					// Main loop
					while(true) {
						if(Thread.currentThread().isInterrupted()) throw new InterruptedIOException();
						byte type = streamIn.readByte();
						if(type==PackProtocol.END) break;
						String packPath = streamIn.readCompressedUTF();
						// Verbose output
						if(verboseQueue != null) {
							try {
								verboseQueue.put(packPath);
							} catch(InterruptedException err) {
								// Restore the interrupted status
								Thread.currentThread().interrupt();
								InterruptedIOException ioErr = new InterruptedIOException();
								ioErr.initCause(err);
								throw ioErr;
							}
						}

						if(packPath.length() == 0) throw new IOException("Empty packPath");
						if(packPath.charAt(0)!='/') throw new IOException("Invalid packPath, first character is not /");
						sb.setLength(0);
						sb.append(path);
						sb.append(packPath);
						String fullPath = sb.toString();

						// Maintain modify time of directories
						int slashPos = packPath.indexOf('/', 1);
						String subtreeRoot = slashPos==-1 ? packPath : packPath.substring(0, slashPos);
						Stack<PathAndModifyTime> mtimeStack = directoryModifyTimes.get(subtreeRoot);
						if(mtimeStack != null) {
							// Unroll stack as much as needed for current item
							while(!mtimeStack.isEmpty()) {
								PathAndModifyTime pathAndMod = mtimeStack.peek();
								if(packPath.startsWith(pathAndMod.path)) break;
								sb.setLength(0);
								PosixFile uf = new PosixFile(sb.append(path).append(pathAndMod.path).toString());
								uf.utime(
									uf.getStat().getAccessTime(),
									pathAndMod.modifyTime
								);
								mtimeStack.pop();
							}
						}

						// Make sure doesn't exist if not in force mode
						PosixFile uf = new PosixFile(fullPath);
						Stat ufStat = uf.getStat();
						if(!force && ufStat.exists()) throw new IOException("Exists: "+fullPath);

						// Handle this file
						if(type==PackProtocol.REGULAR_FILE) {
							long linkId = streamIn.readLong();
							if(linkId == 0) {
								// No hard links
								int uid = streamIn.readInt();
								int gid = streamIn.readInt();
								long mode = streamIn.readLong();
								long modifyTime = streamIn.readLong();
								if(dryRun) skipFile(streamIn, buffer);
								else {
									if(ufStat.exists()) uf.deleteRecursive();
									readFile(uf, streamIn, buffer);
									uf.chown(uid, gid).setMode(mode);
									ufStat = uf.getStat();
									uf.utime(ufStat.getAccessTime(), modifyTime);
								}
							} else {
								Long linkIdL = linkId;
								PathAndCount pathAndCount = linkPathAndCounts.get(linkIdL);
								if(pathAndCount != null) {
									// Already sent, link and decrement our count
									if(!dryRun) {
										if(ufStat.exists()) uf.deleteRecursive();
										sb.setLength(0);
										sb.append(path);
										sb.append(pathAndCount.path);
										String linkPath = sb.toString();
										uf.link(linkPath);
									}
									if(--pathAndCount.linkCount<=0) linkPathAndCounts.remove(linkIdL);
								} else {
									// New file, receive file data
									int uid = streamIn.readInt();
									int gid = streamIn.readInt();
									long mode = streamIn.readLong();
									long modifyTime = streamIn.readLong();
									int numLinks = streamIn.readInt();
									if(dryRun) skipFile(streamIn, buffer);
									else {
										if(ufStat.exists()) uf.deleteRecursive();
										readFile(uf, streamIn, buffer);
										uf.chown(uid, gid).setMode(mode);
										ufStat = uf.getStat();
										uf.utime(ufStat.getAccessTime(), modifyTime);
									}
									linkPathAndCounts.put(linkIdL, new PathAndCount(packPath, numLinks-1));
								}
							}
						} else if(type==PackProtocol.DIRECTORY) {
							int uid = streamIn.readInt();
							int gid = streamIn.readInt();
							long mode = streamIn.readLong();
							long modifyTime = streamIn.readLong();
							if(!dryRun) {
								if(ufStat.exists()) {
									if(!ufStat.isDirectory()) {
										uf.deleteRecursive();
										uf.mkdir().chown(uid, gid).setMode(mode);
									} else {
										if(ufStat.getUid()!=uid || ufStat.getGid()!=gid) uf.chown(uid, gid);
										if(ufStat.getMode()!=mode) uf.setMode(mode);
									}
								} else {
									uf.mkdir().chown(uid, gid).setMode(mode);
								}
							}
							if(mtimeStack == null) {
								mtimeStack = new Stack<>();
								directoryModifyTimes.put(subtreeRoot, mtimeStack);
							}
							mtimeStack.push(new PathAndModifyTime(packPath+'/', modifyTime));
						} else if(type==PackProtocol.SYMLINK) {
							int uid = streamIn.readInt();
							int gid = streamIn.readInt();
							String target = streamIn.readCompressedUTF();
							if(!dryRun) {
								if(ufStat.exists()) uf.deleteRecursive();
								uf.symLink(target).chown(uid, gid);
							}
						} else if(type==PackProtocol.BLOCK_DEVICE) {
							int uid = streamIn.readInt();
							int gid = streamIn.readInt();
							long mode = streamIn.readLong();
							long deviceIdentifier = streamIn.readLong();
							if(!dryRun) {
								if(ufStat.exists()) uf.deleteRecursive();
								uf.mknod(mode|PosixFile.IS_BLOCK_DEVICE, deviceIdentifier).chown(uid, gid);
							}
						} else if(type==PackProtocol.CHARACTER_DEVICE) {
							int uid = streamIn.readInt();
							int gid = streamIn.readInt();
							long mode = streamIn.readLong();
							long deviceIdentifier = streamIn.readLong();
							if(!dryRun) {
								if(ufStat.exists()) uf.deleteRecursive();
								uf.mknod(mode|PosixFile.IS_CHARACTER_DEVICE, deviceIdentifier).chown(uid, gid);
							}
						} else if(type==PackProtocol.FIFO) {
							int uid = streamIn.readInt();
							int gid = streamIn.readInt();
							long mode = streamIn.readLong();
							if(!dryRun) {
								if(ufStat.exists()) uf.deleteRecursive();
								uf.mkfifo(mode).chown(uid, gid);
							}
						} else throw new IOException("Unexpected value for type: "+type);
					}
				} finally {
					// Unroll stacks entirely
					for(Stack<PathAndModifyTime> mtimeStack : directoryModifyTimes.values()) {
						while(!mtimeStack.isEmpty()) {
							PathAndModifyTime pathAndMod = mtimeStack.pop();
							sb.setLength(0);
							PosixFile uf = new PosixFile(sb.append(path).append(pathAndMod.path).toString());
							uf.utime(uf.getStat().getAccessTime(), pathAndMod.modifyTime);
						}
					}
				}
			} finally {
				if(PackProtocol.BUFFER_SIZE == BufferManager.BUFFER_SIZE) BufferManager.release(buffer, false);
			}
			// TODO: If verbose, warn hard links not fully transferred
		} finally {
			// Wait for verbose queue to be empty
			if(verboseThread != null) {
				synchronized(verboseThreadRun) {
					verboseThreadRun[0] = false;
				}
				try {
					verboseThread.join();
				} catch(InterruptedException err) {
					// Restore the interrupted status
					Thread.currentThread().interrupt();
					InterruptedIOException ioErr = new InterruptedIOException();
					ioErr.initCause(err);
					throw ioErr;
				}
			}
		}
	}

	private static void skipFile(DataInput in, byte[] buffer) throws IOException {
		int count;
		while((count = in.readShort()) != -1) {
			in.readFully(buffer, 0, count);
		}
	}

	private static void readFile(PosixFile uf, DataInput in, byte[] buffer) throws IOException {
		try (OutputStream out = new FileOutputStream(uf.getFile())) {
			int count;
			while((count=in.readShort()) != -1) {
				in.readFully(buffer, 0, count);
				out.write(buffer, 0, count);
			}
		}
	}
}
