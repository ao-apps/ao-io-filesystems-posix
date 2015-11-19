/*
 * ao-io-filesystems-unix - Advanced filesystem utilities for Unix.
 * Copyright (C) 2015  AO Industries, Inc.
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

import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.filesystems.RandomFailFileSystem;
import com.aoindustries.io.unix.Stat;
import java.io.IOException;
import java.util.Random;

/**
 * A Unix file system implementation that randomly fails, this is used by test
 * suites to verify correct behavior under expected failure modes.
 *
 * @author  AO Industries, Inc.
 */
public class RandomFailUnixFileSystem extends RandomFailFileSystem implements UnixFileSystem {

	/**
	 * Default probabilities
	 */
	public static final float
		DEFAULT_STAT_FAILURE_PROBABILITY = 0.00001f
	;

	private final UnixFileSystem wrapped;
	private final float statFailureProbability;

	public RandomFailUnixFileSystem(
		UnixFileSystem wrapped,
		float listFailureProbability,
		float listIterateFailureProbability,
		float listIterateCloseFailureProbability,
		float statFailureProbability,
		float unlinkFailureProbability,
		float sizeFailureProbability,
		Random random
	) {
		super(
			wrapped,
			listFailureProbability,
			listIterateFailureProbability,
			listIterateCloseFailureProbability,
			unlinkFailureProbability,
			sizeFailureProbability,
			random
		);
		this.wrapped = wrapped;
		this.statFailureProbability = statFailureProbability;
	}

	/**
	 * @see RandomFailFileSystem#RandomFailFileSystem(com.aoindustries.io.filesystems.FileSystem)
	 */
	public RandomFailUnixFileSystem(UnixFileSystem wrapped) {
		super(wrapped);
		this.wrapped = wrapped;
		this.statFailureProbability = DEFAULT_STAT_FAILURE_PROBABILITY;
	}

	/**
	 * Delegates to the wrapped file system, but with a random chance of fail.
	 */
	@Override
	public Stat stat(Path path) throws RandomFailIOException, IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		randomFail(statFailureProbability);
		return wrapped.stat(unwrapPath(path));
	}
}
