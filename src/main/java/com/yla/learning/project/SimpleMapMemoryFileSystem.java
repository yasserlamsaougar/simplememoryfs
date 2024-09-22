package com.yla.learning.project;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/**
 * A simple in-memory file system that supports large files using chunked data storage.
 */
public class SimpleMapMemoryFileSystem extends FileSystem {

  private final ConcurrentMap<Path, MemoryFile> folders = new ConcurrentHashMap<>();
  private volatile Path workingDir = new Path("/");

  // Default block size system property
  private static final long DEFAULT_BLOCK_SIZE = Long.getLong("memoryfs.blocksize", 64 * 1024 * 1024); // Default to 64MB

  static class MemoryFile {
    final FileSystem.Statistics statistics;
    final ChunkedData dataChunks;
    final boolean isFolder;
    volatile long modificationTime;
    volatile long accessTime;
    final ConcurrentMap<String, Attribute> attributes;
    final FsPermission permissions;
    final String owner;

    MemoryFile(FileSystem.Statistics statistics, boolean isFolder,
               long modificationTime, long accessTime, String owner, FsPermission permissions, int chunkSize) {
      this.statistics = statistics;
      this.dataChunks = new ChunkedData(chunkSize);
      this.isFolder = isFolder;
      this.modificationTime = modificationTime;
      this.accessTime = accessTime;
      this.attributes = new ConcurrentHashMap<>();
      this.permissions = permissions;
      this.owner = owner;
    }

    long getSize() {
      return dataChunks.getSize();
    }
  }

  static class Attribute {
    final byte[] value;
    final Set<XAttrSetFlag> flags;

    Attribute(byte[] value, Set<XAttrSetFlag> flags) {
      this.value = value;
      this.flags = flags;
    }
  }

  static class MemoryWriteStream extends OutputStream implements Seekable {
    private final MemoryFile memoryFile;
    private final Progressable progressable;
    private long position;

    MemoryWriteStream(MemoryFile memoryFile, Progressable progressable, long initialPosition) {
      this.memoryFile = memoryFile;
      this.progressable = progressable;
      this.position = initialPosition;
    }

    @Override
    public void write(int b) {
      byte[] singleByte = new byte[]{(byte) b};
      write(singleByte, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      memoryFile.dataChunks.write(position, b, off, len);
      position += len;
      if (progressable != null) {
        progressable.progress();
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0) {
        throw new EOFException("Cannot seek to negative position");
      }
      position = pos;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }
  }

  static class MemoryReadStream extends InputStream implements Seekable, PositionedReadable {
    private final MemoryFile memoryFile;
    private long position;

    MemoryReadStream(MemoryFile memoryFile) {
      this.memoryFile = memoryFile;
    }

    @Override
    public int read() {
      byte[] singleByte = new byte[1];
      int result = read(singleByte, 0, 1);
      if (result == -1) {
        return -1;
      } else {
        return singleByte[0] & 0xFF;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) {
      int bytesRead = memoryFile.dataChunks.read(position, b, off, len);
      if (bytesRead > 0) {
        position += bytesRead;
      }
      return bytesRead;
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0 || pos > memoryFile.getSize()) {
        throw new EOFException("Seek position out of bounds");
      }
      position = pos;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
      return memoryFile.dataChunks.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      int bytesRead = 0;
      while (bytesRead < length) {
        int result = read(position + bytesRead, buffer, offset + bytesRead, length - bytesRead);
        if (result == -1) {
          throw new EOFException("End of file reached before reading fully");
        }
        bytesRead += result;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }

  @Override
  public URI getUri() {
    return URI.create("memoryfs:///");
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absPath = makeAbsolute(f);
    if (!hasPermission(absPath, FsAction.READ)) {
      throw new AccessControlException("Permission denied: Cannot read " + f);
    }
    MemoryFile memoryFile = folders.get(absPath);
    if (memoryFile == null || memoryFile.isFolder) {
      throw new FileNotFoundException(f.toString());
    }
    memoryFile.accessTime = System.currentTimeMillis();
    return new FSDataInputStream(new MemoryReadStream(memoryFile));
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize, Progressable progress)
          throws IOException {
    Path absPath = makeAbsolute(f);

    // Ensure a user is present
    String currentUser = getCurrentUser();
    if (currentUser == null || currentUser.isEmpty()) {
      throw new AccessControlException("Cannot create file: No user is set");
    }

    // Use the provided blockSize or default if not provided
    int chunkSize = (int) (blockSize > 0 ? blockSize : DEFAULT_BLOCK_SIZE);

    // Check write permission on parent directory
    Path parentPath = absPath.getParent();
    if (parentPath != null && !hasPermission(parentPath, FsAction.WRITE)) {
      throw new AccessControlException("Permission denied: Cannot create file " + f);
    }
    MemoryFile existingFile = folders.get(absPath);
    if (existingFile != null) {
      if (!overwrite) {
        throw new FileAlreadyExistsException("File already exists: " + absPath);
      }
      if (existingFile.isFolder) {
        throw new IOException("Cannot overwrite a directory: " + absPath);
      }
    }

    // Create the new file
    MemoryFile memoryFile = new MemoryFile(
            new FileSystem.Statistics(getUri().getScheme()),
            false,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            currentUser,
            permission,
            chunkSize
    );
    folders.put(absPath, memoryFile);

    return new FSDataOutputStream(new MemoryWriteStream(memoryFile, progress, 0), null);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    Path absPath = makeAbsolute(f);
    if (!hasPermission(absPath, FsAction.WRITE)) {
      throw new AccessControlException("Permission denied: Cannot append to " + f);
    }
    MemoryFile memoryFile = folders.get(absPath);

    if (memoryFile == null || memoryFile.isFolder) {
      throw new FileNotFoundException("File not found or is a directory: " + absPath);
    }
    memoryFile.modificationTime = System.currentTimeMillis();
    memoryFile.accessTime = System.currentTimeMillis();

    long initialPosition = memoryFile.getSize();

    return new FSDataOutputStream(new MemoryWriteStream(memoryFile, progress, initialPosition), null);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absSrcPath = makeAbsolute(src);
    Path absDstPath = makeAbsolute(dst);

    // Check if the source exists
    MemoryFile srcFile = folders.get(absSrcPath);
    if (srcFile == null) {
      throw new FileNotFoundException("Source file not found: " + absSrcPath);
    }

    // Check read and write permissions on the source path
    if (!hasPermission(absSrcPath, FsAction.READ_WRITE)) {
      throw new AccessControlException("Permission denied: Cannot rename " + src);
    }

    // Check write permissions on the destination parent directory
    Path dstParentPath = absDstPath.getParent();
    if (dstParentPath != null && !hasPermission(dstParentPath, FsAction.WRITE)) {
      throw new AccessControlException("Permission denied: Cannot rename to " + dst);
    }

    if (folders.containsKey(absDstPath)) {
      throw new FileAlreadyExistsException("Destination file already exists: " + absDstPath);
    }

    synchronized (folders) {
      folders.remove(absSrcPath);
      folders.put(absDstPath, srcFile);
    }
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    Path absPath = makeAbsolute(f);
    if (!hasPermission(absPath, FsAction.WRITE)) {
      throw new AccessControlException("Permission denied: Cannot delete " + f);
    }
    MemoryFile memoryFile = folders.get(absPath);

    if (memoryFile == null) {
      return false;
    }

    if (memoryFile.isFolder) {
      if (!recursive) {
        throw new IOException("Directory is not empty: " + absPath);
      }
      synchronized (folders) {
        folders.keySet().removeIf(key -> key.toString().startsWith(absPath.toString()));
      }
    } else {
      folders.remove(absPath);
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absPath = makeAbsolute(f);
    MemoryFile memoryFile = folders.get(absPath);
    if (memoryFile == null) {
      throw new FileNotFoundException("File not found: " + absPath);
    }

    if (memoryFile.isFolder) {
      List<FileStatus> statuses = new ArrayList<>();
      synchronized (folders) {
        for (Map.Entry<Path, MemoryFile> entry : folders.entrySet()) {
          Path key = entry.getKey();
          if (key.getParent() != null && key.getParent().equals(absPath)) {
            MemoryFile value = entry.getValue();
            statuses.add(createFileStatus(key, value));
          }
        }
      }
      return statuses.toArray(new FileStatus[0]);
    } else {
      return new FileStatus[]{createFileStatus(absPath, memoryFile)};
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    Path absPath = makeAbsolute(f);

    // Ensure a user is present
    String currentUser = getCurrentUser();
    if (currentUser == null || currentUser.isEmpty()) {
      throw new AccessControlException("Cannot create directory: No user is set");
    }

    // Check write permission on parent directory
    Path parentPath = absPath.getParent();
    if (parentPath != null && !hasPermission(parentPath, FsAction.WRITE)) {
      throw new AccessControlException("Permission denied: Cannot create directory " + f);
    }
    MemoryFile existingFile = folders.get(absPath);

    if (existingFile != null) {
      if (!existingFile.isFolder) {
        throw new IOException("Path is not a directory: " + absPath);
      }
      return true;
    }

    // Use default chunk size for directories (no data)
    int chunkSize = (int) DEFAULT_BLOCK_SIZE;

    MemoryFile memoryFile = new MemoryFile(
            new FileSystem.Statistics(getUri().getScheme()),
            true,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            currentUser,
            permission,
            chunkSize
    );
    folders.put(absPath, memoryFile);
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Path absPath = makeAbsolute(f);
    MemoryFile memoryFile = folders.get(absPath);
    if (memoryFile == null) {
      throw new FileNotFoundException("File not found: " + absPath);
    }
    return createFileStatus(absPath, memoryFile);
  }

  private FileStatus createFileStatus(Path path, MemoryFile memoryFile) {
    return new FileStatus(
            memoryFile.isFolder ? 0 : memoryFile.getSize(),
            memoryFile.isFolder,
            1,
            DEFAULT_BLOCK_SIZE,
            memoryFile.modificationTime,
            memoryFile.accessTime,
            memoryFile.permissions,
            memoryFile.owner,
            null, // group
            path
    );
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    } else {
      return new Path(workingDir, path);
    }
  }

  private String getCurrentUser() {
    String user = System.getProperty("user.name");
    if (user == null || user.isEmpty()) {
      return null;
    }
    return user;
  }

  private boolean isSecurityEnabled() {
    return Boolean.getBoolean("security.enabled");
  }

  private boolean hasPermission(Path path, FsAction action) throws IOException {
    if (!isSecurityEnabled()) {
      return true;
    }

    String userName = getCurrentUser();
    if (userName == null || userName.isEmpty()) {
      throw new AccessControlException("Permission denied: No user is set");
    }

    MemoryFile memoryFile = folders.get(path);

    if (memoryFile == null) {
      // For non-existent paths, check permissions on the parent directory
      Path parentPath = path.getParent();
      if (parentPath == null) {
        // Root directory; assume accessible
        return true;
      }
      return hasPermission(parentPath, FsAction.WRITE);
    } else {
      // Check permissions based on ownership
      FsAction userAction;
      if (userName.equals(memoryFile.owner)) {
        userAction = memoryFile.permissions.getUserAction();
      } else {
        userAction = memoryFile.permissions.getOtherAction();
      }
      return userAction.implies(action);
    }
  }
}
