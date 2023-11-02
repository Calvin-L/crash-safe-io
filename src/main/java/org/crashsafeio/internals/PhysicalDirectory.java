package org.crashsafeio.internals;

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.mustcall.qual.Owning;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public record PhysicalDirectory(Path path, @Owning FileChannel channel) implements DirectoryHandle {
  public PhysicalDirectory(Path path, @Owning FileChannel channel) {
    // need this constructor b/c CF resource leak analysis not yet record-compatible
    this.path = path;
    this.channel = channel;
  }

  @Override
  public @NotOwning FileChannel channel() {
    return channel;
  }

  @Override
  @EnsuresCalledMethods(value = "channel", methods = {"close"})
  public void close() throws IOException {
    channel.close();
  }
}
