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
    $ ls build/libs/crash-safe-io-*.jar


## Documentation

To build the JavaDocs, run

    $ gradle javadoc

and then open `build/docs/javadoc/index.html`.

See also: the "Important functionality in this library" section below.


## Concepts

_Atomicity_ and _durability_ are the essence of crash safety:

  - Atomicity means that there are only two observable outcomes: either the
    operation either happens in its entirety or the operation does not happen
    at all.  Even if the system crashes during the operation, there will be no
    corrupt data or partial effects.
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
    but only creates its file atomically and durably when `close()` is called.


## Caveats

Do not use the filesystem for thread synchronization.  It is possible for
threads and processes on the machine to see the effects of calls to this
library before they have been durably saved.  For instance, if thread A calls
`DurableIOUtil.write(...)`, then thread B might see the new file before thread
A has made the creation durable.  It is dangerous for thread B to act as if
the file has been durably created, even though it is observable.  Instead,
thread B must wait until the call made by thread A has completely _returned_,
which requires a different form of thread synchronization.

This library achieves the best guarantees possible using only the Java
standard library.  On some systems there may be machine-dependent techniques
that further improve the guarantees.
