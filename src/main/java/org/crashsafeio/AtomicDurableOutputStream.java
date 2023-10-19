package org.crashsafeio;

import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
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

  /** The final path that will be created on {@link #close()} */
  private final Path out;

  /** The location of the temporary file where bytes are written */
  private final Path tmp;

  /** The file descriptor for the open handle to {@link #tmp} */
  private final FileDescriptor fd;

  private AtomicDurableOutputStream(Path out, Path tmp, FileOutputStream outputStream) throws IOException {
    super(new BufferedOutputStream(outputStream));
    this.out = out;
    this.tmp = tmp;
    this.fd = outputStream.getFD();
  }

  private AtomicDurableOutputStream(Path out, Path tmp) throws IOException {
    this(out, tmp, new FileOutputStream(tmp.toFile()));
  }

  /**
   * Construct a new atomic durable output stream.
   * @param out the file that will be created when {@link #close()} is called
   * @throws IOException if an I/O error occurs when creating or opening a temporary file for writing
   */
  public AtomicDurableOutputStream(Path out) throws IOException {
    this(out, Files.createTempFile(null, null));
  }

  /**
   * Make the changes to this file durable.  After this call, the file will be closed
   * and {@link #write(byte[])} is no longer legal.  Regardless of the outcome,
   * you still need to {@link #close()} this object.
   *
   * @throws IOException if an I/O error occurs while making the changes durable
   */
  public void commit() throws IOException {
    flush();
    fd.sync();
    super.close();
    DurableIOUtil.moveWithoutPromisingSourceDeletion(tmp, out);
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      bestEffortDelete(tmp);
    }
  }

  /**
   * Try to delete the given path.  The deletion is best-effort only: if the deletion fails,
   * then no error will be thrown or reported.
   *
   * @param path the path to delete
   */
  private static void bestEffortDelete(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (Exception ignored) {
    }
  }
}
