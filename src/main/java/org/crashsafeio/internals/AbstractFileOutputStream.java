package org.crashsafeio.internals;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Output stream that writes to an abstract file descriptor.
 *
 * @param <F> the file descriptor type
 * @see Filesystem#openFile(Path)
 * @see Filesystem#write(FileHandle, byte[], int, int)
 */
public class AbstractFileOutputStream<F extends FileHandle> extends OutputStream {

  private final Filesystem<?, F> fs;

  private final F fd;

  public AbstractFileOutputStream(Filesystem<?, F> fs, F fd) {
    this.fs = fs;
    this.fd = fd;
  }

  @Override
  public void write(int b) throws IOException {
    fs.write(fd, new byte[] { (byte)b }, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    fs.write(fd, b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    fs.write(fd, b, off, len);
  }

  @Override
  public void close() throws IOException {
    try {
      fd.close();
    } finally {
      super.close();
    }
  }

}
