package org.crashsafeio.internals;

import org.checkerframework.checker.mustcall.qual.MustCall;

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

    Path parentPath = path.getParent();
    if (parentPath == null) {
      throw new IllegalArgumentException("Path has no parent: " + path);
    }

    Path fileName = path.getFileName();
    if (fileName == null) {
      throw new IllegalArgumentException("Path does not reference a filename: " + path);
    }

    try (@MustCall("close") D dir = openDirectory(parentPath)) {
      unlink(dir, fileName.toString());
    }
  }

  default void moveAtomically(Path source, Path target) throws IOException {
    source = source.toAbsolutePath();

    Path sourceParentPath = source.getParent();
    if (sourceParentPath == null) {
      throw new IllegalArgumentException("Source path has no parent: " + source);
    }

    Path sourceFileName = source.getFileName();
    if (sourceFileName == null) {
      throw new IllegalArgumentException("Source path has no filename: " + source);
    }

    target = target.toAbsolutePath();

    Path targetParentPath = target.getParent();
    if (targetParentPath == null) {
      throw new IllegalArgumentException("Target path has no parent: " + target);
    }

    Path targetFileName = target.getFileName();
    if (targetFileName == null) {
      throw new IllegalArgumentException("Target path has no filename: " + target);
    }

    try (@MustCall("close") D sourceParent = openDirectory(sourceParentPath);
         @MustCall("close") D targetParent = openDirectory(targetParentPath)) {
      rename(sourceParent, sourceFileName.toString(), targetParent, targetFileName.toString());
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
