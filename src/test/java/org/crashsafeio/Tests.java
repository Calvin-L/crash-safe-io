package org.crashsafeio;

import org.testng.annotations.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class Tests {

  @Test
  public void testClosingAfterReferenceCopy() throws Exception {
    AtomicBoolean didClose = new AtomicBoolean(false);
    AutoCloseable toClose = () -> didClose.set(true);
    try (AutoCloseable ignored = toClose) {
    }
    assert didClose.get();
  }

  @Test
  public void testOutputStreamAtomicity() throws IOException {
    byte[] bytes = new byte[1024];
    Random r = new Random(33L);
    r.nextBytes(bytes);

    Path out = Files.createTempDirectory(null).resolve("somefile");
    try (OutputStream stream = new BufferedOutputStream(new AtomicDurableOutputStream(out))) {
      int nwritten = 0;
      while (nwritten < bytes.length) {
        int toWrite = Math.min(bytes.length - nwritten, bytes.length / 4);
        stream.write(bytes, nwritten, toWrite);
        nwritten += toWrite;
        assert !Files.exists(out);
      }
    }
    assert Files.exists(out);
    assert Arrays.equals(Files.readAllBytes(out), bytes);
    Files.delete(out);
    Files.delete(out.getParent());
  }

  @Test
  public void testCreateFolders() throws IOException {
    Path out = Files.createTempDirectory(null).resolve(Paths.get("a", "b", "c"));
    DurableIOUtil.createFolders(out);
    assert Files.isDirectory(out);
  }

  @Test
  public void testWriteFileBasics() throws IOException {
    byte[] bytes = "my data".getBytes(StandardCharsets.UTF_8);
    Path out = Files.createTempDirectory(null).resolve(Paths.get("a", "b", "c"));
    try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
      DurableIOUtil.write(out, stream);
    }
    assert Arrays.equals(Files.readAllBytes(out), bytes);
  }

  @Test
  public void testFileDeletion() throws IOException {
    Path file = Files.createTempFile(null, null);
    Files.write(file, "hello".getBytes(StandardCharsets.UTF_8));
    DurableIOUtil.atomicallyDelete(file);
    assert !Files.exists(file);
  }

  @Test
  public void testEmptyDirectoryDeletion() throws IOException {
    Path dir = Files.createTempDirectory(null);
    DurableIOUtil.atomicallyDelete(dir);
    assert !Files.exists(dir);
  }

  @Test
  public void testTreeDeletion() throws IOException {
    Path dir = Files.createTempDirectory(null);
    Files.createDirectory(dir.resolve("subfolder"));
    Files.createFile(dir.resolve("subfolder").resolve("subchild"));
    Files.write(dir.resolve("child"), "hello".getBytes(StandardCharsets.UTF_8));
    DurableIOUtil.atomicallyDelete(dir);
    assert !Files.exists(dir);
  }

}
