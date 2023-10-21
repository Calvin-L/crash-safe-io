package org.crashsafeio;

import org.crashsafeio.internals.DurableFilesystemOperations;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

@Test
public class InvariantTest {

  @DataProvider
  public Object[] seeds() {
    Object[] result = new Object[10];
    long seed = 1;
    for (int i = 0; i < result.length; ++i) {
      seed *= 31;
      seed += 3;
      result[i] = seed;
    }
    return result;
  }

  @Test(dataProvider = "seeds")
  @Parameters({"seed"})
  public void testWrite(long seed) throws IOException {

    Random rand = new Random(seed);
    TestFilesystem fs = new TestFilesystem(rand);
    Path path = Path.of("/foo/bar/baz");
    byte[] data = new byte[10];
    rand.nextBytes(data);

    List<Byte> dataAsByteList =
        IntStream.range(0, data.length)
            .map(i -> data[i])
            .mapToObj(x -> (byte) x)
            .toList();

    fs.addInvariant(root -> {
      TestFilesystem.INode iNode = TestFilesystem.get(root, path);
      return iNode == null || (iNode instanceof TestFilesystem.File f
          && f.volatileContents.equals(dataAsByteList)
          && f.durableContents.equals(dataAsByteList));
    });

    new DurableFilesystemOperations<>(fs).write(path, data);

    fs.addInvariant(root -> {
      TestFilesystem.INode iNode = TestFilesystem.get(root, path);
      return iNode instanceof TestFilesystem.File f
          && f.volatileContents.equals(dataAsByteList)
          && f.durableContents.equals(dataAsByteList);
    });
  }

}
