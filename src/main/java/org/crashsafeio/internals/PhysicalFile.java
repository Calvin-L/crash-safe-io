package org.crashsafeio.internals;

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.mustcall.qual.Owning;

import java.io.FileOutputStream;
import java.io.IOException;

public record PhysicalFile(@Owning FileOutputStream outputStream) implements FileHandle {
  public PhysicalFile(@Owning FileOutputStream outputStream) {
    // need this constructor b/c CF resource leak analysis not yet record-compatible
    this.outputStream = outputStream;
  }

  @Override
  public @NotOwning FileOutputStream outputStream() {
    return outputStream;
  }

  @Override
  @EnsuresCalledMethods(value = "outputStream", methods = {"close"})
  public void close() throws IOException {
    outputStream.close();
  }
}
