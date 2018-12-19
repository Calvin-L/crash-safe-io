# crash-safe-io

[![Build Status](https://travis-ci.org/Calvin-L/crash-safe-io.svg?branch=master
)](https://travis-ci.org/Calvin-L/crash-safe-io)

This is a small Java library for crash-safe file I/O.  The procedures here
offer much stronger guarantees than the standard library routines, but incur
more I/O performance overhead.

This library is not currently available from any major repos, but it has no
external dependencies and is easy to build from source:

    $ git clone https://github.com/Calvin-L/crash-safe-io.git
    $ gradle build
    $ ls build/libs/crashsafeio-*.jar

## Concepts

_Atomicity_ and _durability_ are the essence of crash safety:

  - Atomicity means that the operation either happens or doesn't happen.  Even
    if the system crashes during the operation, there will be no corrupt data
    or partially-complete operations.
  - Durability means that the effect is saved to permanent storage and will
    survive a subsequent power outage.

The standard library routines are neither atomic nor durable.  For example,
[`Files.write(path, data)`](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#write-java.nio.file.Path-byte:A-java.nio.file.OpenOption...-) is not
atomic, since if the system crashes it may have only written some of the desired
data to the file.  It is also not durable, since if the system crashes shortly
after it returns, the written file may disappear if it was only written to a
cache and was not flushed to the physical hard drive.

## Important functionality in this library

  - `DurableIOUtil` contains static methods for common filesystem operations:
    - `write(path, data)` writes a file atomically and durably
    - `createFolders(path)` durably creates a folder and all intermediate folders
    - `atomicallyDelete(path)` atomically and durably deletes an entire subtree
    - `move(source, target)` atomically and durably renames the source file to
      the target file
  - `AtomicDurableOutputStream` is similar to [`FileOutputStream`](https://docs.oracle.com/javase/8/docs/api/java/io/FileOutputStream.html),
    but creates its file atomically and durably when `close()` is called.
