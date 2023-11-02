package org.crashsafeio.internals;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Collectors;

public class PhysicalFilesystem implements Filesystem<PhysicalDirectory, PhysicalFile> {

  public static final PhysicalFilesystem INSTANCE = new PhysicalFilesystem();

  private PhysicalFilesystem() { }

  @Override
  public Path createTempDirectory() throws IOException {
    return Files.createTempDirectory("");
  }

  @Override
  public Path createTempFile() throws IOException {
    return Files.createTempFile("", "");
  }

  @Override
  public void deleteIfExists(Path path) throws IOException {
    Files.deleteIfExists(path);
  }

  @Override
  public Set<Path> list(Path directory) throws IOException {
    try (var entries = Files.list(directory)) {
      return entries.collect(Collectors.toSet());
    }
  }

  @Override
  public PhysicalDirectory openDirectory(Path path) throws IOException {
    var fd = FileChannel.open(path, StandardOpenOption.READ);
    try {
      return new PhysicalDirectory(path, fd);
    } catch (Exception e) {
      try {
        fd.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
  }

  @Override
  public boolean isReadableDirectory(PhysicalDirectory parentDirectory, String name) throws IOException {
    return Files.isDirectory(parentDirectory.path().resolve(name));
  }

  @Override
  public void mkdir(PhysicalDirectory parentDirectory, String name) throws IOException {
    Files.createDirectory(parentDirectory.path().resolve(name));
  }

  @Override
  public void unlink(PhysicalDirectory directory, String name) throws IOException {
    Files.delete(directory.path().resolve(name));
  }

  @Override
  public void moveAtomically(Path source, Path target) throws IOException {
    Files.move(
        source,
        target,
        StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void rename(PhysicalDirectory sourceDirectory, String sourceName, PhysicalDirectory targetDirectory, String targetName) throws IOException {
    moveAtomically(
        sourceDirectory.path().resolve(sourceName),
        targetDirectory.path().resolve(targetName));
  }

  @Override
  public void sync(PhysicalDirectory directory) throws IOException {
    directory.channel().force(true);
  }

  @Override
  public PhysicalFile openFile(Path path) throws IOException {
    var stream = new FileOutputStream(path.toFile());
    try {
      return new PhysicalFile(stream);
    } catch (Exception e) {
      try {
        stream.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
  }

  @Override
  public void write(PhysicalFile file, byte[] bytes, int offset, int length) throws IOException {
    file.outputStream().write(bytes, offset, length);
  }

  @Override
  public void sync(PhysicalFile file) throws IOException {
    FileOutputStream stream = file.outputStream();
    stream.flush();
    stream.getFD().sync();
  }

}
