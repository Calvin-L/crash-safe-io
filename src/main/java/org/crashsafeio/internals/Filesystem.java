package org.crashsafeio.internals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * An abstract filesystem.  This generic type allows for in-memory implementations
 * that can simulate crashes, but in practice everyone will use {@link PhysicalFilesystem}.
 *
 * @param <D> the type of open directory descriptors
 * @param <F> the type of open file descriptors
 */
public interface Filesystem<D extends DirectoryHandle, F extends FileHandle> {

  Path createTempDirectory() throws IOException;

  Path createTempFile() throws IOException;

  default void deleteIfExists(Path path) throws IOException {
    path = path.toAbsolutePath();
    try (D dir = openDirectory(path.getParent())) {
      unlink(dir, path.getFileName().toString());
    }
  }

  default void moveAtomically(Path source, Path target) throws IOException {
    try (D sourceParent = openDirectory(source.getParent());
         D targetParent = openDirectory(target.getParent())) {
      rename(sourceParent, source.getFileName().toString(), targetParent, target.getFileName().toString());
    }
  }

  Set<Path> list(Path directory) throws IOException;

  D openDirectory(Path path) throws IOException;

  boolean isReadableDirectory(D parentDirectory, String name) throws IOException;

  void mkdir(D parentDirectory, String name) throws IOException;

  void unlink(D parentDirectory, String name) throws IOException;

  void rename(D sourceParentDirectory, String sourceName, D targetParentDirectory, String targetName) throws IOException;

  void sync(D directory) throws IOException;

  F openFile(Path path) throws IOException;

  void write(F file, byte[] bytes, int offset, int length) throws IOException;

  void sync(F file) throws IOException;

}
