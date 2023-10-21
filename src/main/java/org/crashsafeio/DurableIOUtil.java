package org.crashsafeio;

import org.crashsafeio.internals.DurableFilesystemOperations;
import org.crashsafeio.internals.PhysicalDirectory;
import org.crashsafeio.internals.PhysicalFile;
import org.crashsafeio.internals.PhysicalFilesystem;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

/**
 * Static methods for durable I/O.  The methods mirror those in {@link Files}, but offer stronger
 * guarantees.
 */
public class DurableIOUtil {

  /** Utility class is not to be instantiated */
  private DurableIOUtil() { }

  /* package-private */ final static DurableFilesystemOperations<PhysicalDirectory, PhysicalFile> OPS =
      new DurableFilesystemOperations<>(PhysicalFilesystem.INSTANCE);

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
    OPS.createDirectories(folderToCreate);
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
    OPS.atomicallyDelete(path);
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
    OPS.move(source, target);
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
    OPS.moveWithoutPromisingSourceDeletion(source, target);
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
    OPS.write(file, bytes);
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
    OPS.write(file, data);
  }

}
