package org.crashsafeio.internals;

import java.io.FileOutputStream;
import java.io.IOException;

public record PhysicalFile(FileOutputStream outputStream) implements FileHandle {
  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
