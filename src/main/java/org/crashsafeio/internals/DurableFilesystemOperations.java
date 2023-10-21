package org.crashsafeio.internals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public record DurableFilesystemOperations<D extends DirectoryHandle, F extends FileHandle>(
    Filesystem<D, F> underlyingFilesystem) {

  /**
   * Helper for {@link #createDirectories(Path)}.
   *
   * <p>
   *   Precondition: <code>path</code> durably exists and is not a symlink.
   *
   * <p>
   *   Precondition: <code>folderName</code> does not contain any path separators (e.g. / on unix).
   *
   * <p>
   *   Postcondition: the folder <code>folderName</code> durably exists.  The modification time
   *   and access time of the parent path may have been modified, whether the parent is a
   *   folder or regular file.  The changes to the modification and access time of the parent
   *   path are durably saved.  The access time of any prefix of the parent path may have been
   *   modified; those changes are NOT durably saved.
   *
   * <p>
   *   Crash safety: <code>folderName</code> will either exist or not exist.
   *   Independently, the modification time and access time of the parent path may
   *   have been modified, whether the parent is a folder or regular file.
   *
   * @param path - the parent folder
   * @param folderName - the name of a folder to create within the parent
   * @return A Path equivalent to
   *  <code>path.{@link Path#resolve(Path) resolve}(folderName)</code> that now durably
   *  exists and contains no components that are symlinks.
   * @throws java.nio.file.FileAlreadyExistsException if another process created a
   *   folder or file with the same name concurrently while this procedure was running
   * @throws SecurityException if a security manager is installed and it denies read
   *   access to the path OR the folder did not exist and it denies write access to the
   *   path
   * @throws IOException if an I/O exception occurs while this procedure is running, for
   *   instance because another process concurrently deleted the path
   */
  private Path createOneDirectory(Path path, String folderName) throws IOException {
    Path result = path.resolve(folderName);

    try (D dir = underlyingFilesystem.openDirectory(path)) {
      // Do this check in case the target exists but we don't have write access to
      // the parent, which is a very common case.
      if (!underlyingFilesystem.isReadableDirectory(dir, folderName)) {
        underlyingFilesystem.mkdir(dir, folderName);
      }
      underlyingFilesystem.sync(dir);
    }

    return result;
  }

  public void createDirectories(Path folderToCreate) throws IOException {
    folderToCreate = folderToCreate.toAbsolutePath();
    Path root = folderToCreate.getRoot(); // we have to assume the filesystem root durably exists
    if (root == null) {
      throw new IllegalArgumentException("Path has no root: " + folderToCreate);
    }
    for (int i = 0; i < folderToCreate.getNameCount(); ++i) {
      root = createOneDirectory(root, folderToCreate.getName(i).toString());
    }
  }

  /**
   * Walk the tree, deleting <code>path</code> and all of its children.
   * This procedure returns without error if the path does not exist.
   * This procedure provides absolutely no crash safety guarantees;
   * it may arbitrarily corrupt <code>path</code> or its children.
   * It may or may not change the modification and access times of
   * <code>path</code>'s parent.
   *
   * @param path the path to delete
   * @throws SecurityException if there is a security manager installed and its
   *         {@link SecurityManager#checkDelete(String)} method returns false for any entry in the
   *         tree
   * @throws IOException if an I/O error occurs
   */
  private void deleteTreeUnsafe(Path path) throws IOException {
    List<Path> toDelete = new ArrayList<>();
    toDelete.add(path);
    do {
      int index = toDelete.size() - 1;
      path = toDelete.get(index);
      try {
        underlyingFilesystem.deleteIfExists(path);
        toDelete.remove(index);
      } catch (DirectoryNotEmptyException ignored) {
        toDelete.addAll(underlyingFilesystem.list(path));
      }
    } while (!toDelete.isEmpty());
  }

  public void atomicallyDelete(Path path) throws IOException {
    path = path.toAbsolutePath();

    Path parentPath = path.getParent();
    if (parentPath == null) {
      throw new IllegalArgumentException("Path has no parent: " + path);
    }

    Path fileName = path.getFileName();
    if (fileName == null) {
      throw new IllegalArgumentException("Path does not reference a filename: " + path);
    }

    Path tmp = null;
    try (D parent = underlyingFilesystem.openDirectory(parentPath)) {
      try {
        underlyingFilesystem.unlink(parent, fileName.toString());
      } catch (DirectoryNotEmptyException ignoredException) {
        // TODO: I would like to have a primitive to create a temporary folder on the same filesystem
        tmp = underlyingFilesystem.createTempDirectory();
        // NOTE: We do not need the full power of move() below since we do not care
        // about preserving the target file.
        underlyingFilesystem.moveAtomically(path, tmp.resolve("thingToDelete"));
      }
      underlyingFilesystem.sync(parent);
    }

    // This might be very slow. Putting it after the force() call makes it
    // less likely that the system crashes during this optional cleanup step.
    if (tmp != null) {
      deleteTreeUnsafe(tmp);
    }
  }

  public void move(Path source, Path target) throws IOException {
    // Javadocs are vague about how ATOMIC_MOVE behaves.
    // See https://stackoverflow.com/questions/3764822/how-to-durably-rename-a-file-in-posix

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

    try (D sourceParent = underlyingFilesystem.openDirectory(sourceParentPath);
         D targetParent = underlyingFilesystem.openDirectory(targetParentPath)) {
      underlyingFilesystem.rename(
          sourceParent, sourceFileName.toString(),
          targetParent, targetFileName.toString());
      underlyingFilesystem.sync(targetParent);
      underlyingFilesystem.sync(sourceParent);
    }
  }

  public void moveWithoutPromisingSourceDeletion(Path source, Path target) throws IOException {
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

    try (D sourceParent = underlyingFilesystem.openDirectory(sourceParentPath);
         D targetParent = underlyingFilesystem.openDirectory(targetParentPath)) {
      underlyingFilesystem.rename(
          sourceParent, sourceFileName.toString(),
          targetParent, targetFileName.toString());
      underlyingFilesystem.sync(targetParent);
    }
  }

  public void write(Path file, byte[] bytes) throws IOException {
    Path parentPath = file.toAbsolutePath().getParent();
    if (parentPath == null) {
      throw new IllegalArgumentException("Path has no parent: " + file);
    }

    try (var out = new InternalAtomicDurableOutputStream<>(this, file)) {
      out.write(bytes);
      createDirectories(parentPath);
      out.commit();
    }
  }

  public void write(Path file, InputStream data) throws IOException {
    Path parentPath = file.toAbsolutePath().getParent();
    if (parentPath == null) {
      throw new IllegalArgumentException("Path has no parent: " + file);
    }

    byte[] buffer = new byte[1024 * 8];
    try (var out = new InternalAtomicDurableOutputStream<>(this, file)) {
      int nread;
      do {
        nread = data.read(buffer);
        if (nread > 0) {
          out.write(buffer, 0, nread);
        }
      } while (nread >= 0);
      createDirectories(parentPath);
      out.commit();
    }
  }

}
