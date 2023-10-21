package org.crashsafeio;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.crashsafeio.internals.DirectoryHandle;
import org.crashsafeio.internals.FileHandle;
import org.crashsafeio.internals.Filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TestFilesystem implements Filesystem<TestFilesystem.Dir, TestFilesystem.File> {

  public sealed interface INode permits TestFilesystem.Dir, TestFilesystem.File {
    boolean inSync();
    void sync1(Random random);
    INode crash();
    void remap(Map<INode, INode> remap);
  }

  public final static class Dir implements INode, DirectoryHandle {
    Map<String, INode> durableEntries = new HashMap<>();
    Map<String, INode> volatileEntries = new HashMap<>();

    @Override
    public boolean inSync() {
      return durableEntries.equals(volatileEntries);
    }

    @Override
    public void sync1(Random random) {
      List<Runnable> ops = new ArrayList<>();
      for (var volatileEntry : volatileEntries.entrySet()) {
        var key = volatileEntry.getKey();
        var val = volatileEntry.getValue();
        if (!Objects.equals(durableEntries.get(key), val)) {
          ops.add(() -> durableEntries.put(key, val));
        }
      }
      for (var key : durableEntries.keySet()) {
        if (!volatileEntries.containsKey(key)) {
          ops.add(() -> durableEntries.remove(key));
        }
      }
      randomChoice(random, ops).run();
    }

    @Override
    public Dir crash() {
      Dir result = new Dir();
      result.durableEntries.putAll(durableEntries);
      result.volatileEntries.putAll(durableEntries);
      return result;
    }

    @Override
    public void remap(Map<INode, INode> remap) {
      for (var entry : volatileEntries.entrySet()) {
        entry.setValue(remap.getOrDefault(entry.getValue(), entry.getValue()));
      }
      for (var entry : durableEntries.entrySet()) {
        entry.setValue(remap.getOrDefault(entry.getValue(), entry.getValue()));
      }
    }

    @Override
    public void close() {
    }
  }

  public final static class File implements INode, FileHandle {
    List<Byte> durableContents = new ArrayList<>();
    List<Byte> volatileContents = new ArrayList<>();

    @Override
    public boolean inSync() {
      return durableContents.equals(volatileContents);
    }

    @Override
    public void sync1(Random random) {
      List<Runnable> ops = new ArrayList<>();
      for (int i = 0; i < Math.min(durableContents.size(), volatileContents.size()); ++i) {
        if (!Objects.equals(durableContents.get(i), volatileContents.get(i))) {
          int indexToUpdate = i;
          ops.add(() -> durableContents.set(indexToUpdate, volatileContents.get(indexToUpdate)));
        }
      }
      if (durableContents.size() < volatileContents.size()) {
        ops.add(() -> durableContents.add((byte)(random.nextInt(0xFF))));
      } else if (durableContents.size() > volatileContents.size()) {
        ops.add(() -> durableContents.remove(durableContents.size() - 1));
      }
      randomChoice(random, ops).run();
    }

    @Override
    public File crash() {
      File result = new File();
      result.durableContents.addAll(durableContents);
      result.volatileContents.addAll(durableContents);
      return result;
    }

    @Override
    public void remap(Map<INode, INode> remap) {
    }

    @Override
    public void close() {
    }
  }

  private final Dir root = new Dir();

  private final Random random;

  private final Set<Predicate<Dir>> invariants = new LinkedHashSet<>();

  public TestFilesystem(Random random) {
    this.random = random;
  }

  public static <T> T randomChoice(Random random, List<T> choices) {
    return choices.get(random.nextInt(choices.size()));
  }

  public void addInvariant(Predicate<Dir> invariant) {
    if (invariants.add(invariant)) {
      checkInvariants();
    }
  }

  public void checkInvariants() {
    for (var inv : invariants) {
      if (!inv.test(root)) {
        onInvariantFailure();
      }
    }

    var postCrash = crash();
    for (var inv : invariants) {
      if (!inv.test(postCrash)) {
        onInvariantFailure();
      }
    }
  }

  protected void onInvariantFailure() {
    throw new IllegalStateException("Invariant is not true (set a breakpoint in "
        + getClass().getName() + ".onInvariantFailure() to debug)");
  }

  @Override
  public Path createTempDirectory() throws IOException {
    String name = UUID.randomUUID().toString();
    mkdir(root, name);
    checkInvariants();
    return Path.of("/" + name);
  }

  @Override
  public Path createTempFile() throws IOException {
    String name = UUID.randomUUID().toString();
    root.volatileEntries.put(name, new File());
    checkInvariants();
    return Path.of("/" + name);
  }

  public static @Nullable INode get(Dir root, Path path) {
    if (!path.isAbsolute()) {
      path = path.toAbsolutePath();
    }
    Path parent = path.getParent();
    if (parent == null) {
      return root;
    }
    Path fileName = path.getFileName();
    if (fileName == null) {
      return null;
    }
    INode parentNode = get(root, parent);
    if (parentNode instanceof Dir d) {
      return d.volatileEntries.get(fileName.toString());
    }
    return null;
  }

  public INode get(Path path) throws IOException {
    INode result = get(root, path);
    if (result == null) {
      throw new NoSuchFileException(path.toString());
    }
    return result;
  }

  @Override
  public Set<Path> list(Path directory) throws IOException {
    INode node = get(directory);
    if (node instanceof Dir d) {
      return d.volatileEntries.keySet()
          .stream()
          .map(Path::of)
          .collect(Collectors.toSet());
    }
    throw new IOException("not a directory: " + directory);
  }

  @Override
  public Dir openDirectory(Path path) throws IOException {
    if (get(path) instanceof Dir d) {
      return d;
    }
    throw new IOException("not a directory: " + path);
  }

  @Override
  public boolean isReadableDirectory(Dir parentDirectory, String name) throws IOException {
    return parentDirectory.volatileEntries.get(name) instanceof Dir;
  }

  @Override
  public void mkdir(Dir parentDirectory, String name) throws IOException {
    var prev = parentDirectory.volatileEntries.putIfAbsent(name, new Dir());
    if (prev != null) {
      throw new FileAlreadyExistsException(name);
    }
    checkInvariants();
  }

  @Override
  public void unlink(Dir parentDirectory, String name) throws IOException {
    parentDirectory.volatileEntries.remove(name);
    checkInvariants();
  }

  @Override
  public void rename(Dir sourceParentDirectory, String sourceName, Dir targetParentDirectory, String targetName) throws IOException {
    INode toMove = sourceParentDirectory.volatileEntries.remove(sourceName);
    if (toMove == null) {
      throw new FileNotFoundException(sourceName);
    }
    targetParentDirectory.volatileEntries.put(targetName, toMove);
    checkInvariants();
  }

  @Override
  public void sync(Dir directory) throws IOException {
    while (!directory.inSync()) {
      directory.sync1(random);
      checkInvariants();
    }
  }

  @Override
  public File openFile(Path path) throws IOException {
    if (get(path) instanceof File f) {
      return f;
    }
    throw new IOException("not a file: " + path);
  }

  @Override
  public void write(File file, byte[] bytes, int offset, int length) throws IOException {
    // TODO: this is not quite right... File INode and File Handle should be separate,
    //       because each handle has its own pointer
    for (int i = 0; i < length; ++i) {
      file.volatileContents.add(bytes[offset + i]);
    }
    checkInvariants();
  }

  @Override
  public void sync(File file) throws IOException {
    while (!file.inSync()) {
      file.sync1(random);
      checkInvariants();
    }
  }

  private <T> void enqueue(Set<T> seen, Collection<T> worklist, T value) {
    if (seen.add(value)) {
      worklist.add(value);
    }
  }

  protected Set<INode> reachableINodes() {
    Set<INode> seen = new LinkedHashSet<>();
    List<INode> worklist = new ArrayList<>();

    enqueue(seen, worklist, root);

    while (!worklist.isEmpty()) {
      INode n = worklist.remove(worklist.size() - 1);
      if (n instanceof Dir d) {
        for (INode next : d.volatileEntries.values()) {
          enqueue(seen, worklist, next);
        }
        for (INode next : d.durableEntries.values()) {
          enqueue(seen, worklist, next);
        }
      }
    }

    return seen;
  }

  private Dir crash() {
    Map<INode, INode> remap = new HashMap<>();

    for (var iNode : reachableINodes()) {
      remap.put(iNode, iNode.crash());
    }

    for (var iNode : remap.values()) {
      iNode.remap(remap);
    }

    if (remap.get(root) instanceof Dir newRoot) {
      return newRoot;
    } else {
      throw new IllegalStateException("remap does not contain root; it should have been in reachableINodes()!");
    }
  }
}
