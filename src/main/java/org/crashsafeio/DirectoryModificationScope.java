package org.crashsafeio;

import org.crashsafeio.internals.PhysicalDirectory;
import org.crashsafeio.internals.PhysicalFilesystem;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * An object that makes changes to a directory durable when {@link #commit()}
 * is called.  Many filesystem operations like creating, renaming, or deleting
 * a file are actually modifications to the parent directory, not changes to
 * the file.  You can use this class to ensure that those operations are durably
 * saved.
 *
 * <p>
 *   Recommended usage:
 *   <pre>
 * Path dir = ...
 * try (DirectoryModificationScope scope = new DirectoryModificationScope(dir)) {
 *     // make a change that modifies `dir`, for instance, creating or
 *     // deleting a file inside the directory
 *     modify(dir);
 *
 *     // if that worked (no exception thrown), then flush those changes
 *     // to durable storage
 *     scope.commit();
 * }
 *   </pre>
 *
 * <p>
 *   Note that <code>DirectoryModificationScope</code> opens a file handle, and
 *   therefore it needs to be closed regardless of whether you call
 *   {@link #commit()}.
 *
 * <p>
 *   You should create a <code>DirectoryModificationScope</code> instance
 *   <em>before</em> making changes to the directory that you need durable.
 *   The {@link #commit()} method only promises to make durable the changes
 *   made since the <code>DirectoryModificationScope</code> object was constructed.
 *
 * <p>
 *   The "construct-before-modify" restriction exists because the contract for
 *   {@link FileChannel#force(boolean)}, which this class uses as a subroutine,
 *   states that
 *   <blockquote>
 *     when this method returns it is guaranteed that all changes made to the file
 *     since this channel was created, or since this method was last invoked, will
 *     have been written to that device
 *   </blockquote>
 *   The "SINCE THIS CHANNEL WAS CREATED" qualifier suggests that the
 *   <code>force</code> call is only obligated to make changes since the creation
 *   of the channel durable.  This class constructs a channel in its constructor,
 *   so constructing a <code>DirectoryModificationScope</code> before making changes
 *   to the directory ensures that you satisfy the contract.
 *
 * <p>
 *   In fact, the "SINCE THIS CHANNEL WAS CREATED" qualifier is not just
 *   weasely wording in the library documentation.  It is based on real
 *   behavior in Linux and other operating systems.  PostgreSQL's
 *   "checkpointer" thread is an infamous example:
 *   <blockquote>
 *     the checkpointer will not see any errors that happened before it opened
 *     the file. If something bad happens before the checkpointer's open()
 *     call, the subsequent fsync() call will return successfully.
 *   </blockquote>
 *   (<a href="https://lwn.net/Articles/752063/">source: lwn.net</a>)
 *
 * <p>
 *   Caveat: Calling {@link #commit()} only makes <em>direct</em> changes
 *   to the directory durable; changes to subdirectories may not be durable.
 *
 * <p>
 *   Caveat: this object is attached to the logical directory object (i.e.
 *   "inode"), <em>not</em> the path to the directory.  If the directory
 *   is moved or deleted and replaced with a new directory, then
 *   {@link #commit()} will not make changes to the new directory
 *   durable.
 *
 * @see DurableIOUtil#createDirectories(Path)
 */
public class DirectoryModificationScope implements AutoCloseable {

  // see: https://stackoverflow.com/questions/7433057/is-rename-without-fsync-safe
  // see: http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003140.html

  /** An open channel to the directory. */
  private final PhysicalDirectory fd;

  /**
   * Create an instance.
   * @param directory the path to the directory whose changes will be made
   *                  durable when {@link #close()} is called
   * @throws UnsupportedOperationException if the JVM or underlying system
   *    does not support opening directories as file channels
   * @throws SecurityException if a security manager is installed and it
   *    does not allow us to open the directory in read mode
   * @throws IOException if an I/O exception prevents us from opening the
   *    directory in read mode
   */
  public DirectoryModificationScope(Path directory) throws IOException {
    fd = PhysicalFilesystem.INSTANCE.openDirectory(directory);
  }

  /**
   * Make durable any changes to the directory that have happened since this
   * object was constructed.
   *
   * @throws IllegalStateException if {@link #close()} has been called
   * @throws IOException if an I/O error occurs
   */
  public void commit() throws IOException {
    try {
      PhysicalFilesystem.INSTANCE.sync(fd);
    } catch (ClosedChannelException e) {
      throw new IllegalStateException("DirectoryModificationScope was already closed", e);
    }
  }

  /**
   * Release any resources.  This method <em>must</em> be called to release
   * the resources, but it can be called more than once.  Subsequent calls are
   * no-ops.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    fd.close();
  }

}
