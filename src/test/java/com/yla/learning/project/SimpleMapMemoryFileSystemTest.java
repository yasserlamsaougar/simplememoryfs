package com.yla.learning.project;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.*;

class SimpleMapMemoryFileSystemTest {

    private SimpleMapMemoryFileSystem fs;

    @BeforeEach
    void setUp() {
        fs = new SimpleMapMemoryFileSystem();
        // Enable security for testing permissions
        System.setProperty("security.enabled", "true");
    }

    @Test
    void testCreateFileWithNoUser() {
        // Clear the user.name system property to simulate no user being set
        System.clearProperty("user.name");

        Path filePath = new Path("/testfile.txt");

        Exception exception = assertThrows(AccessControlException.class, () -> fs.create(filePath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null));

        String expectedMessage = "Cannot create file: No user is set";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate missing user");
    }

    @Test
    void testCreateFileWithUser() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "testuser");

        Path filePath = new Path("/testfile.txt");

        // Create and write to the file
        try (FSDataOutputStream out = fs.create(filePath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("Hello, world!".getBytes());
        }

        // Verify that the file exists
        assertTrue(fs.exists(filePath), "File should exist after creation");

        // Get file status and check owner
        FileStatus status = fs.getFileStatus(filePath);
        assertEquals("testuser", status.getOwner(), "File owner should be 'testuser'");
    }

    @Test
    void testCreateDirectoryWithNoUser() {
        // Clear the user.name system property to simulate no user being set
        System.clearProperty("user.name");

        Path dirPath = new Path("/testdir");

        Exception exception = assertThrows(AccessControlException.class, () -> fs.mkdirs(dirPath, FsPermission.getDirDefault()));

        String expectedMessage = "Cannot create directory: No user is set";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate missing user");
    }

    @Test
    void testCreateAndListDirectoryWithUser() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "testuser");

        Path dirPath = new Path("/testdir");
        Path filePath1 = new Path("/testdir/file1.txt");
        Path filePath2 = new Path("/testdir/file2.txt");

        // Create directory
        assertTrue(fs.mkdirs(dirPath, FsPermission.getDirDefault()), "Directory should be created successfully");

        // Create files inside the directory
        try (FSDataOutputStream out = fs.create(filePath1, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("File 1 content".getBytes());
        }
        try (FSDataOutputStream out = fs.create(filePath2, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("File 2 content".getBytes());
        }

        // List the directory contents
        FileStatus[] fileStatuses = fs.listStatus(dirPath);
        assertEquals(2, fileStatuses.length, "Directory should contain 2 files");

        // Verify ownership
        for (FileStatus status : fileStatuses) {
            assertEquals("testuser", status.getOwner(), "File owner should be 'testuser'");
        }
    }

    @Test
    void testPermissionDeniedWhenReadingWithoutPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");

        // Create and write to the file with permissions only for the owner
        FsPermission permission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
        try (FSDataOutputStream out = fs.create(filePath, permission, true, 1024, (short) 1, 1024L, null)) {
            out.write("Sensitive data".getBytes());
        }

        // Change user to someone else
        System.setProperty("user.name", "otheruser");

        // Attempt to read the file
        Exception exception = assertThrows(AccessControlException.class, () -> fs.open(filePath, 1024));

        String expectedMessage = "Permission denied";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate permission denied");
    }

    @Test
    void testPermissionAllowedForOwner() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");

        // Create and write to the file
        try (FSDataOutputStream out = fs.create(filePath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("Owner's data".getBytes());
        }

        // Read the file as the owner
        try (FSDataInputStream in = fs.open(filePath, 1024)) {
            byte[] buffer = new byte[20];
            int bytesRead = in.read(buffer);
            assertTrue(bytesRead > 0, "Should read data from the file");
        }
    }

    @Test
    void testDeleteFileWithoutPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");
        FsPermission ownerOnlyPermissions = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);

        // Create and write to the file
        try (FSDataOutputStream out = fs.create(filePath, ownerOnlyPermissions, true, 1024, (short) 1, 1024L, null)) {
            out.write("Owner's data".getBytes());
        }

        // Change user to someone else
        System.setProperty("user.name", "otheruser");

        // Attempt to delete the file
        Exception exception = assertThrows(AccessControlException.class, () -> fs.delete(filePath, false));

        String expectedMessage = "Permission denied";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate permission denied");
    }

    @Test
    void testDeleteFileWithPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");

        // Create and write to the file
        try (FSDataOutputStream out = fs.create(filePath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("Owner's data".getBytes());
        }

        // Delete the file as the owner
        assertTrue(fs.delete(filePath, false), "Owner should be able to delete the file");

        // Verify that the file no longer exists
        assertFalse(fs.exists(filePath), "File should not exist after deletion");
    }

    @Test
    void testRenameFileWithoutPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path srcPath = new Path("/srcfile.txt");
        Path dstPath = new Path("/dstfile.txt");

        // Create the source file
        FsPermission ownerOnlyPermissions = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);

        try (FSDataOutputStream out = fs.create(srcPath, ownerOnlyPermissions, true, 1024, (short) 1, 1024L, null)) {
            out.write("Some data".getBytes());
        }

        // Change user to someone else
        System.setProperty("user.name", "otheruser");

        // Attempt to rename the file
        Exception exception = assertThrows(AccessControlException.class, () -> fs.rename(srcPath, dstPath));

        String expectedMessage = "Permission denied";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate permission denied");
    }

    @Test
    void testRenameFileWithPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path srcPath = new Path("/srcfile.txt");
        Path dstPath = new Path("/dstfile.txt");

        // Create the source file
        try (FSDataOutputStream out = fs.create(srcPath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("Some data".getBytes());
        }

        // Rename the file as the owner
        assertTrue(fs.rename(srcPath, dstPath), "Owner should be able to rename the file");

        // Verify that the destination file exists
        assertTrue(fs.exists(dstPath), "Destination file should exist after renaming");

        // Verify that the source file no longer exists
        assertFalse(fs.exists(srcPath), "Source file should not exist after renaming");
    }

    @Test
    void testAppendToFileWithoutPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");

        // Create the file
        FsPermission ownerOnlyPermissions = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);

        try (FSDataOutputStream out = fs.create(filePath, ownerOnlyPermissions, true, 1024, (short) 1, 1024L, null)) {
            out.write("Initial data".getBytes());
        }

        // Change user to someone else
        System.setProperty("user.name", "otheruser");

        // Attempt to append to the file
        Exception exception = assertThrows(AccessControlException.class, () -> {
            OutputStream out = fs.append(filePath, 1024, null);
            out.write("Additional data".getBytes());
            out.close();
        });

        String expectedMessage = "Permission denied";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage), "Exception message should indicate permission denied");
    }

    @Test
    void testAppendToFileWithPermission() throws IOException {
        // Set the user.name system property
        System.setProperty("user.name", "owneruser");

        Path filePath = new Path("/testfile.txt");

        // Create the file
        try (FSDataOutputStream out = fs.create(filePath, FsPermission.getFileDefault(), true, 1024, (short) 1, 1024L, null)) {
            out.write("Initial data".getBytes());
        }

        // Append to the file as the owner
        try (OutputStream out = fs.append(filePath, 1024, null)) {
            out.write(" Additional data".getBytes());
        }

        // Read the file and verify contents
        try (FSDataInputStream in = fs.open(filePath, 1024)) {
            byte[] buffer = new byte[50];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            assertEquals("Initial data Additional data", content, "File content should match the appended data");
        }
    }
}
