package org.crashsafeio;

import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.crashsafeio.internals.InternalAtomicDurableOutputStream;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * An output stream similar to {@link FileOutputStream}, but the file is only created when
 * {@link #commit()} is called.  When it is created, it is created atomically and durably.
 *
 * <p>
 *   Performance notes: the bytes are written to a temporary file, so instances of this class do
 *   not consume an unbounded amount of memory.  The output is buffered, so clients will
 *   <em>not</em> benefit from additional layers of {@link java.io.BufferedOutputStream buffering}.
 *
 * <p>
 *   Suggested usage:
 *   <pre>{@code
 * Path outPath = ...;
 * try (AtomicDurableOutputStream out = new AtomicDurableOutputStream(outPath)) {
 *     // write output
 *     out.write(...);
 *
 *     // if that worked (no exception thrown), then flush those changes
 *     // to durable storage by creating the file durably and atomically
 *     out.commit();
 * }
 * }</pre>
 */
public abstract class AtomicDurableOutputStream extends BufferedOutputStream {

  /**
   * Construct a new atomic durable output stream.
   *
   * @param out the file that will be created when {@link #close()} is called
   * @throws IOException if an I/O error occurs when creating or opening a temporary file for writing
   */
  public static AtomicDurableOutputStream open(Path out) throws IOException {
    return InternalAtomicDurableOutputStream.open(DurableIOUtil.OPS, out);
  }

  public @MustCallAlias AtomicDurableOutputStream(@MustCallAlias OutputStream out) {
    super(out);
  }

  /**
   * Make the changes to this file durable.  After this call, the file will be closed
   * and {@link #write(byte[])} is no longer legal.  Regardless of the outcome,
   * you still need to {@link #close()} this object.
   *
   * @throws IOException if an I/O error occurs while making the changes durable
   */
  public abstract void commit() throws IOException;

}
