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

public class Tests {

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

}
