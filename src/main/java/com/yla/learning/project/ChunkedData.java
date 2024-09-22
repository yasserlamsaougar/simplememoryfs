package com.yla.learning.project;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages data storage in chunks using List<List<Byte>> to support large files.
 * Maintains a size field to accurately track the total data size.
 */
class ChunkedData {
    private final List<List<Byte>> chunks;               // List of data chunks, each chunk is a List<Byte>.
    private final int chunkSize;                         // Size of each chunk.
    private long size = 0;                               // Total size of the data.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(); // Read-write lock for thread safety.

    /**
     * Constructs a new ChunkedData with the specified chunk size.
     *
     * @param chunkSize The size of each chunk in bytes.
     */
    public ChunkedData(int chunkSize) {
        this.chunks = new ArrayList<>();
        this.chunkSize = chunkSize;
    }

    /**
     * Gets the total size of the data stored.
     *
     * @return The total size in bytes.
     */
    public long getSize() {
        lock.readLock().lock();
        try {
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Writes data to the storage at the specified position.
     *
     * @param position The position to start writing at.
     * @param data     The data to write.
     * @param offset   The offset within the data array.
     * @param length   The number of bytes to write.
     */
    public void write(long position, byte[] data, int offset, int length) {
        lock.writeLock().lock();
        try {
            long currentPosition = position;
            int bytesWritten = 0;
            while (bytesWritten < length) {
                int chunkIndex = (int) (currentPosition / chunkSize);    // Determine which chunk to write to.
                int chunkOffset = (int) (currentPosition % chunkSize);   // Offset within the chunk.

                // Ensure the chunk exists; create empty chunks if necessary.
                while (chunkIndex >= chunks.size()) {
                    chunks.add(new ArrayList<>()); // Start with an empty chunk.
                }

                List<Byte> chunk = chunks.get(chunkIndex);

                // Expand the chunk if necessary.
                while (chunk.size() < chunkSize) {
                    chunk.add((byte) 0);
                }

                int spaceInChunk = chunkSize - chunkOffset;                          // Available space in the current chunk.
                int bytesToWrite = Math.min(length - bytesWritten, spaceInChunk);    // Bytes to write in this iteration.

                // Write data to the chunk.
                for (int i = 0; i < bytesToWrite; i++) {
                    int index = chunkOffset + i;
                    chunk.set(index, data[offset + bytesWritten + i]);
                }

                currentPosition += bytesToWrite;    // Update current position.
                bytesWritten += bytesToWrite;       // Update bytes written.
            }

            // Update the size field if we have written beyond the previous size.
            long newEndPosition = position + length;
            if (newEndPosition > size) {
                size = newEndPosition;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reads data from the storage starting at the specified position.
     *
     * @param position The position to start reading from.
     * @param buffer   The buffer to read data into.
     * @param offset   The offset within the buffer.
     * @param length   The number of bytes to read.
     * @return The number of bytes actually read, or -1 if end of data is reached.
     */
    public int read(long position, byte[] buffer, int offset, int length) {
        lock.readLock().lock();
        try {
            if (position >= size) {
                return -1; // End of data reached.
            }

            long currentPosition = position;
            int bytesRead = 0;

            while (bytesRead < length && currentPosition < size) {
                int chunkIndex = (int) (currentPosition / chunkSize);    // Determine which chunk to read from.
                int chunkOffset = (int) (currentPosition % chunkSize);   // Offset within the chunk.

                List<Byte> chunk = chunks.get(chunkIndex);

                int availableInChunk = chunk.size() - chunkOffset;                   // Available data in the chunk.
                int bytesToRead = Math.min(length - bytesRead, availableInChunk); // Bytes to read in this iteration.

                // Adjust bytesToRead if reading beyond the actual size.
                if (currentPosition + bytesToRead > size) {
                    bytesToRead = (int) (size - currentPosition);
                }

                // Read data from the chunk.
                for (int i = 0; i < bytesToRead; i++) {
                    buffer[offset + bytesRead + i] = chunk.get(chunkOffset + i);
                }

                currentPosition += bytesToRead;  // Update current position.
                bytesRead += bytesToRead;        // Update bytes read.
            }

            return bytesRead;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Truncates the data to the specified new size.
     *
     * @param newSize The new size in bytes.
     */
    public void truncate(long newSize) {
        lock.writeLock().lock();
        try {
            if (newSize >= size) {
                return; // No truncation needed.
            }

            int newChunkCount = (int) ((newSize + chunkSize - 1) / chunkSize);  // Calculate the required number of chunks.
            while (chunks.size() > newChunkCount) {
                chunks.removeLast(); // Remove excess chunks.
            }
            if (chunks.size() > 0) {
                int lastChunkSize = (int) (newSize % chunkSize);
                if (lastChunkSize > 0) {
                    List<Byte> lastChunk = chunks.getLast();
                    while (lastChunk.size() > lastChunkSize) {
                        lastChunk.removeLast(); // Truncate the last chunk.
                    }
                }
            }

            // Update the size field.
            size = newSize;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
