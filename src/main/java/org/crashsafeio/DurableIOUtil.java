package org.crashsafeio;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Objects;
import java.util.Stack;

/**
 * Static methods for durable I/O.  The methods mirror those in {@link Files}, but offer stronger
 * guarantees.
 */
public class DurableIOUtil {

  /** Utility class is not to be instantiated */
  private DurableIOUtil() { }

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
  private static Path createOneDirectory(Path path, String folderName) throws IOException {
    Path result = path.resolve(folderName);

    // Do this check FIRST in case the target exists but we don't have write access to
    // the parent, which is a very common case.
    if (Files.isDirectory(result)) {
      // Call .toRealPath() in case the "folder" is actually a symlink to the folder.
      return result.toRealPath();
    }

    try (FSyncDirectoryOnClose ignored = new FSyncDirectoryOnClose(path)) {
      Files.createDirectory(result);
    }
    return result;
  }

  /**
   * Create the folder at the given path, as well as any intermediate folders.
   * This procedure is similar to {@link Files#createDirectories(Path, FileAttribute[])},
   * but offers stronger durability and crash safety guarantees.
   *
   * <p>
   *   Precondition: the filesystem that <code>folderToCreate</code> would reside on
   *   if it existed must be a local filesystem.  If it is not then this procedure will
   *   still create the folders and return successfully, but it offers no durability
   *   guarantee.
   *
   * <p>
   *   Precondition: all prefixes of the <code>folderToCreate</code> path that already
   *   exist <em>durably</em> exist.  If they do not, then this procedure will
   *   still create the folders and return successfully, but it offers no durability
   *   guarantee.
   *
   * <p>
   *   Postcondition: <code>folderToCreate</code> durably exists and is a directory.
   *   The modification and access times of pre-existing components of the folder might
   *   have changed; only the changes to the most-specific pre-existing component of the
   *   path are durably saved.
   *
   * <p>
   *   Crash safety: <code>folderName</code> will either exist or not exist.
   *   This function may have created some, but not all, of the necessary folders.
   *   Independently, the modification time and access time of the path components may
   *   have been modified.
   *
   * @param folderToCreate an absolute or relative path of a folder to create
   * @throws java.nio.file.FileAlreadyExistsException if another process created a
   *   folder or file with the same name as a component of <code>folderToCreate</code>
   *   while this procedure was running
   * @throws SecurityException if a security manager is installed and it denies read
   *   access to the path OR the folder did not exist and it denies write access to the
   *   path
   * @throws IOException if an I/O exception occurs while this procedure is running, for
   *   instance because another process concurrently deleted part of the path
   */
  public static void createDirectories(Path folderToCreate) throws IOException {
    Objects.requireNonNull(folderToCreate);
    folderToCreate = folderToCreate.toAbsolutePath();
    Path root = folderToCreate.getRoot(); // we have to assume the filesystem root durably exists
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
  private static void deleteTreeUnsafe(Path path) throws IOException {
    Stack<Path> toDelete = new Stack<>();
    toDelete.push(path);
    do {
      path = toDelete.peek();
      try {
        Files.deleteIfExists(path);
        toDelete.pop();
      } catch (DirectoryNotEmptyException ignored) {
        Files.list(path).forEach(toDelete::push);
      }
    } while (!toDelete.empty());
  }

  /**
   * Atomically and durably delete the named path and everything underneath it.
   * Note that this procedure does not <em>securely</em> delete the tree; like
   * most deletion procedures, it may be possible to read the bytes of the deleted
   * files from disk even after this function returns.
   *
   * <p>
   *   Precondition: if <code>path</code> does not exist, then it is durably
   *   missing
   *
   * <p>
   *   Postcondition: <code>path</code> has been durably deleted and its parent
   *   is left intact.  The modification and access times of <code>path</code>'s
   *   parent may have been changed.  If <code>path</code> names a symbolic link,
   *   then the link (and not its target) is deleted.
   *
   * <p>
   *   Crash safety: this procedure either deletes <code>path</code> and its
   *   entire subtree or leaves <code>path</code> completely untouched.  If the
   *   JVM crashes but the system stays up then this procedure may leave behind
   *   possibly-corrupted "junk" files in a temporary folder somewhere on the
   *   system.  Most systems will clean up all temporary files on reboot, but
   *   otherwise there is no available mechanism to locate and delete the
   *   leftover junk files.  If <code>path</code> names a file or a folder with
   *   no contents, then no junk files will be created.
   *
   * @param path the path to delete
   * @throws SecurityException if there is a security manager that prevents deletion of
   *   any file in the tree.  Note that the tree might still have been moved if this
   *   exception is thrown, making it appear "deleted".
   * @throws java.nio.file.AtomicMoveNotSupportedException if the underlying
   *   filesystem cannot atomically move the named path to a temporary location
   * @throws IOException if an I/O error occurs
   */
  public static void atomicallyDelete(Path path) throws IOException {
    Path tmp = null;
    try (FSyncDirectoryOnClose ignored = new FSyncDirectoryOnClose(path.getParent())) {
      try {
        Files.deleteIfExists(path);
      } catch (DirectoryNotEmptyException ignoredException) {
        // TODO: I would like to have a primitive to create a temporary folder on the same filesystem
        tmp = Files.createTempDirectory(null);
        // NOTE: We do not need the full power of move() below since we do not care
        // about preserving the target file.
        Files.move(path, tmp.resolve("thingToDelete"), StandardCopyOption.ATOMIC_MOVE);
      }
    }

    // This might be very slow. Putting it after the force() call makes it
    // less likely that the system crashes during this optional cleanup step.
    if (tmp != null) {
      deleteTreeUnsafe(tmp);
    }
  }

  /**
   * Atomically move or rename a file from one path to another.
   *
   * <p>
   *   Precondition: <code>source</code> exists
   *
   * <p>
   *   Postcondition: <code>source</code> does not exist and <code>target</code>
   *   has the original contents of <code>source</code>
   *
   * <p>
   *   Crash safety: the target is either atomically overwritten or not.  If this
   *   function returns then the rename has been durably saved.  On some systems it
   *   may be possible to observe both the source and destination existing after a
   *   hardware crash, although modern filesystems do their best to prevent this.
   *
   * @param source the source name
   * @param target the target name (NOTE: if this is a directory, then it is
   *               overwritten, rather than moving the source into the directory.
   *               This is a difference from the UNIX <code>mv</code> utility.)
   * @throws java.nio.file.AtomicMoveNotSupportedException if the system does not
   *  support atomic move, or if the move cannot be done atomically because the
   *  source and destination are on different filesystems
   * @throws java.nio.file.FileSystemException if the target already exists and is
   *  a directory
   * @throws SecurityException if there is a security manager that prevents writes
   *  to either the source or target
   * @throws IOException if there is an I/O error, for instance because the source
   *  file does not exist
   */
  public static void move(Path source, Path target) throws IOException {
    // Javadocs are vague about how ATOMIC_MOVE behaves.
    // See https://stackoverflow.com/questions/3764822/how-to-durably-rename-a-file-in-posix
    try (FSyncDirectoryOnClose ignored1 = new FSyncDirectoryOnClose(source.getParent());
         FSyncDirectoryOnClose ignored2 = new FSyncDirectoryOnClose(target.getParent())) {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }
  }

  /**
   * Identical to {@link #move(Path, Path)}, but does not promise that the source
   * file is durably deleted after completion.  In many cases (for instance, when
   * the source file is a temporary file), this is perfectly fine.
   *
   * @param source the source name
   * @param target the target name (NOTE: if this is a directory, then it is
   *               overwritten, rather than moving the source into the directory.
   *               This is a difference from the UNIX <code>mv</code> utility.)
   * @throws IOException if an I/O error occurs
   */
  public static void moveWithoutPromisingSourceDeletion(Path source, Path target) throws IOException {
    try (FSyncDirectoryOnClose ignored = new FSyncDirectoryOnClose(target.getParent())) {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }
  }

  /**
   * Write some bytes to a file, creating the file and all necessary intermediate
   * directories if they do not exist.
   *
   * <p>
   *   Calling this method is equivalent to calling
   *   <code>{@link #write(Path, InputStream) write}(file, new {@link java.io.ByteArrayInputStream ByteArrayInputStream}(bytes))</code>,
   *   but has slightly better performance since it does not use an intermediate buffer.
   *
   * @param file the file to create or overwrite
   * @param bytes the bytes to write
   * @throws IOException if an I/O error occurs
   */
  public static void write(Path file, byte[] bytes) throws IOException {
    try (OutputStream out = new AtomicDurableOutputStream(file)) {
      out.write(bytes);
      createDirectories(file.getParent());
    }
  }

  /**
   * Write some bytes to a file, creating the file and all necessary intermediate directories if
   * they do not exist.
   *
   * <p>
   *   Crash safety: intermediate directories are not created atomically.  It is possible to
   *   observe some or all of the intermediate directories without the final file while this
   *   procedure is in progress, or if the system crashes, or if this method throws an exception.
   *   If you need to achieve atomicity, durably construct the entire tree in a temporary location
   *   and then {@link #move(Path, Path) move} it into place.
   *
   * <p>
   *   Performance note: it is <em>not</em> beneficial to wrap the input stream in a
   *   {@link java.io.BufferedInputStream} since this method uses its own internal buffer.
   *   However, doing so does not impair performance either.
   *
   * @param file the file to create or overwrite
   * @param data the bytes to write
   * @throws IOException if an I/O error occurs
   */
  public static void write(Path file, InputStream data) throws IOException {
    byte[] buffer = new byte[1024 * 8];
    try (OutputStream out = new BufferedOutputStream(new AtomicDurableOutputStream(file))) {
      int nread;
      do {
        nread = data.read(buffer);
        if (nread > 0) {
          out.write(buffer, 0, nread);
        }
      } while (nread >= 0);
      createDirectories(file.getParent());
    }
  }

}
