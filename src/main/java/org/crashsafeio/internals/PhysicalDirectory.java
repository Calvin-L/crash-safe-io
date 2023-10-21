package org.crashsafeio.internals;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public record PhysicalDirectory(Path path, FileChannel channel) implements DirectoryHandle {
  @Override
  public void close() throws IOException {
    channel.close();
  }
}
