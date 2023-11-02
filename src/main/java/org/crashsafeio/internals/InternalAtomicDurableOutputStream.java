package org.crashsafeio.internals;

import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.mustcall.qual.Owning;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Generic version of {@link org.crashsafeio.AtomicDurableOutputStream} that uses an abstract filesystem
 * instead of a physical one.
 *
 * @param <F>
 */
public class InternalAtomicDurableOutputStream<F extends FileHandle> extends BufferedOutputStream {

  /** The final path that will be created on {@link #close()} */
  private final Path out;

  /** The location of the temporary file where bytes are written */
  private final Path tmp;

  /** The abstract filesystem to use */
  private final DurableFilesystemOperations<?, F> fs;

  /** The file descriptor for the open handle to {@link #tmp} */
  private final @NotOwning F fd;

  private InternalAtomicDurableOutputStream(DurableFilesystemOperations<?, F> fs, Path out, Path tmp, @Owning F fd) {
    super(new AbstractFileOutputStream<>(fs.underlyingFilesystem(), fd));
    this.out = out;
    this.tmp = tmp;
    this.fs = fs;
    this.fd = fd;
  }

  /**
   * Construct a new atomic durable output stream.
   *
   * @param fs the filesystem
   * @param out the file that will be created when {@link #close()} is called
   * @throws IOException if an I/O error occurs when creating or opening a temporary file for writing
   */
  public static <F extends FileHandle> InternalAtomicDurableOutputStream<F> open(DurableFilesystemOperations<?, F> fs, Path out) throws IOException {
    var rawFs = fs.underlyingFilesystem();
    Path tmp = rawFs.createTempFile();
    @MustCall("close") F fd = rawFs.openFile(tmp);
    try {
      return new InternalAtomicDurableOutputStream<>(fs, out, tmp, fd);
    } catch (Exception e) {
      try {
        fd.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
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
    fs.underlyingFilesystem().sync(fd);
    super.close();
    fs.moveWithoutPromisingSourceDeletion(tmp, out);
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
  private void bestEffortDelete(Path path) {
    try {
      fs.underlyingFilesystem().deleteIfExists(path);
    } catch (Exception ignored) {
    }
  }
}
