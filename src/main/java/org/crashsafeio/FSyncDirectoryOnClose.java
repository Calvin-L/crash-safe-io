package org.crashsafeio;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FSyncDirectoryOnClose implements AutoCloseable {

  // see: https://stackoverflow.com/questions/7433057/is-rename-without-fsync-safe
  // see: http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003140.html

  private final FileChannel channel;

  public FSyncDirectoryOnClose(Path directory) throws IOException {
    channel = FileChannel.open(directory, StandardOpenOption.READ);
  }

  @Override
  public void close() throws IOException {
    try {
      channel.force(true);
    } finally {
      channel.close();
    }
  }
}
