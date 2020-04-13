/*
 * ao-io-filesystems-unix - Advanced filesystem utilities for Unix.
 * Copyright (C) 2015, 2019  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-io-filesystems-unix.
 *
 * ao-io-filesystems-unix is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-io-filesystems-unix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-io-filesystems-unix.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.io.filesystems.unix;

import com.aoindustries.io.IoUtils;
import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.filesystems.RandomFailFileSystem;
import com.aoindustries.io.unix.Stat;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * A Unix file system implementation that randomly fails, this is used by test
 * suites to verify correct behavior under expected failure modes.
 *
 * @author  AO Industries, Inc.
 */
public class RandomFailUnixFileSystem extends RandomFailFileSystem implements UnixFileSystem {

	public static interface UnixFailureProbabilities extends FailureProbabilities {
		default float getStat() {
			return 0.00001f;
		}
		default float getCreateFileMode() {
			return getCreateFile();
		}
		default float getCreateDirectoryMode() {
			return getCreateDirectory();
		}
	}

	private final UnixFileSystem wrapped;
	private final UnixFailureProbabilities unixFailureProbabilities;

	public RandomFailUnixFileSystem(
		UnixFileSystem wrappedFileSystem,
		UnixFailureProbabilities unixFailureProbabilities,
		Random random
	) {
		super(
			wrappedFileSystem,
			unixFailureProbabilities,
			random
		);
		this.wrapped = wrappedFileSystem;
		this.unixFailureProbabilities = unixFailureProbabilities;
	}

	private static final Random fastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(8)));

	/**
	 * Uses default probabilities and a fast Random source.
	 * 
	 * @see SecureRandom
	 */
	public RandomFailUnixFileSystem(UnixFileSystem wrappedFileSystem) {
		this(
			wrappedFileSystem,
			new UnixFailureProbabilities() {},
			fastRandom
		);
	}

	/**
	 * Delegates to the wrapped file system, but with a random chance of fail.
	 */
	@Override
	public Stat stat(Path path) throws RandomFailIOException, IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		randomFail(unixFailureProbabilities.getStat());
		return wrapped.stat(unwrapPath(path));
	}

	@Override
	public Path createFile(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		randomFail(unixFailureProbabilities.getCreateFileMode());
		wrapped.createFile(unwrapPath(path), mode);
		return path;
	}

	@Override
	public Path createDirectory(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		randomFail(unixFailureProbabilities.getCreateDirectoryMode());
		wrapped.createDirectory(unwrapPath(path), mode);
		return path;
	}
}