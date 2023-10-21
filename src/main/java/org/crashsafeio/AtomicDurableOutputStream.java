package org.crashsafeio;

import org.crashsafeio.internals.InternalAtomicDurableOutputStream;
import org.crashsafeio.internals.PhysicalFile;

import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * An output stream similar to {@link FileOutputStream}, but the file is only created when
 * {@link #commit()} is called.  When it is created, it is created atomically and durably.
 *
 * <p>
 *   Performance notes: the bytes are written to a temporary file, so instances of this class do
 *   not consume an unbounded amount of memory.  The output is buffered, so applications will
 *   <em>not</em> benefit from additional layers of {@link java.io.BufferedOutputStream buffering}.
 *
 * <p>
 *   Suggested usage:
 *   <pre>
 * Path outPath = ...;
 * try (AtomicDurableOutputStream out = new AtomicDurableOutputStream(outPath)) {
 *     // write output
 *     out.write(...);
 *
 *     // if that worked (no exception thrown), then flush those changes
 *     // to durable storage by creating the file durably and atomically
 *     out.commit();
 * }
 *   </pre>
 */
public class AtomicDurableOutputStream extends FilterOutputStream {

  private final InternalAtomicDurableOutputStream<PhysicalFile> out;

  private AtomicDurableOutputStream(InternalAtomicDurableOutputStream<PhysicalFile> out) {
    super(out);
    this.out = out;
  }

  /**
   * Construct a new atomic durable output stream.
   * @param out the file that will be created when {@link #close()} is called
   * @throws IOException if an I/O error occurs when creating or opening a temporary file for writing
   */
  public AtomicDurableOutputStream(Path out) throws IOException {
    this(new InternalAtomicDurableOutputStream<>(DurableIOUtil.OPS, out));
  }

  /**
   * Make the changes to this file durable.  After this call, the file will be closed
   * and {@link #write(byte[])} is no longer legal.  Regardless of the outcome,
   * you still need to {@link #close()} this object.
   *
   * @throws IOException if an I/O error occurs while making the changes durable
   */
  public void commit() throws IOException {
    out.commit();
  }

}
