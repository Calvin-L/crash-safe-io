package org.crashsafeio;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AtomicDurableOutputStream extends FilterOutputStream {

  private final Path out;
  private final Path tmp;
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

  public AtomicDurableOutputStream(Path out) throws IOException {
    this(out, Files.createTempFile(null, null));
  }

  @Override
  public void close() throws IOException {
    flush();
    fd.sync();
    super.close();
    DurableIOUtil.moveWithoutPromisingSoureDeletion(tmp, out);
  }
}
