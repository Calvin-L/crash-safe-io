package org.crashsafeio;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An object that makes changes to a directory durable when {@link #close()}
 * is called.  Many filesystem operations like creating, renaming, or deleting
 * a file are actually modifications to the parent directory, not changes to
 * the file.  You can use this class to ensure that those operations are durably
 * saved.
 *
 * <p>
 *   Recommended usage:
 *   <pre>
 * Path dir = ...
 * try (FSyncDirectoryOnClose ignored = new FSyncDirectoryOnClose(dir)) {
 *     // make a change that modifies `dir`, for instance, creating or
 *     // deleting a file inside the directory
 * }
 *   </pre>
 * </p>
 *
 * <p>
 *   Your application might not need to make the changes durable if an exception
 *   gets thrown.  In that case you can omit the try-with-resources statement:
 *   <pre>
 * Path dir = ...
 * FSyncDirectoryOnClose dirSync = new FSyncDirectoryOnClose(dir);
 * // make a change that modifies `dir`, for instance, creating or
 * // deleting a file inside the directory
 * dirSync.close();
 *   </pre>
 * </p>
 *
 * <p>
 *   You should create an <code>FSyncDirectoryOnClose</code> instance
 *   <em>before</em> making changes to the directory that you need durable.
 *   The {@link #close()} method only promises to make durable the changes
 *   made since the <code>FSyncDirectoryOnClose</code> object was constructed.
 * </p>
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
 *   so constructing an <code>FSyncDirectoryOnClose</code> before making changes
 *   to the directory ensures that you satisfy the contract.
 * </p>
 *
 * <p>
 *   Caveats: Calling {@link #close()} only makes <em>direct</em> changes to
 *   the directory durable; changes to subdirectories may not be durable.
 * </p>
 *
 * @see DurableIOUtil#createFolders(Path)
 */
public class FSyncDirectoryOnClose implements AutoCloseable {

  // see: https://stackoverflow.com/questions/7433057/is-rename-without-fsync-safe
  // see: http://mail.openjdk.java.net/pipermail/nio-dev/2015-May/003140.html

  /** An open channel to the directory. */
  private final FileChannel channel;

  /**
   * Create an instance.
   * @param directory the path to the directory whose changes will be made
   *                  durable when {@link #close()} is called
   * @throws UnsupportedOperationException if the JVM or underlying system
   *    does not support opening directories as file channels
   * @throws SecurityException if the a security manager is installed and
   *    its {@link SecurityManager#checkRead(String)} method does not allow
   *    us to open the directory in read mode
   * @throws IOException if an I/O exception prevents us from opening the
   *    directory in read mode
   */
  public FSyncDirectoryOnClose(Path directory) throws IOException {
    channel = FileChannel.open(directory, StandardOpenOption.READ);
  }

  /**
   * Make changes to the directory durable.
   * This method only promises to make changes made since the creation of
   * this object durable.  Other changes might not be made durable.
   *
   * <p>
   *   Calling this method closes the <code>FSyncDirectoryOnClose</code>
   *   instance.  This method should not be called more than once.
   * </p>
   *
   * @see FSyncDirectoryOnClose
   * @throws java.nio.channels.ClosedChannelException if this object was already closed
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    // Creating the `toClose` reference in a try-with-resources statement ensures
    // that the channel gets closed.  It also produces nicer stack traces if an exception
    // happens than try-finally would.
    try (FileChannel toClose = channel) {
      toClose.force(true);
    }
  }
}
