package org.crashsafeio;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * An output stream similar to {@link FileOutputStream}, but the file is only created when the
 * stream is closed.
 *
 * <p>
 *   Performance notes: the bytes are written to a temporary file, so instances of this class do
 *   not consume an unbounded amount of memory.  The output is not buffered, so applications will
 *   generally benefit from wrapping instances of this class in a
 *   {@link java.io.BufferedOutputStream}.
 *
 * <p>
 *   Suggested usage:
 *   <pre>
 * Path outPath = ...;
 * try (OutputStream out = new BufferedOutputStream(new AtomicDurableOutputStream(outPath))) {
 *     out.write(...);
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
    super(outputStream);
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

  @Override
  public void close() throws IOException {
    flush();
    fd.sync();
    super.close();
    DurableIOUtil.moveWithoutPromisingSourceDeletion(tmp, out);
  }
}
