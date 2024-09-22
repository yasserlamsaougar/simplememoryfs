Certainly! Here's the README in GitHub Markdown format:

---

# SimpleMemoryFS

**SimpleMemoryFS** is a lightweight in-memory file system implemented in Java. It extends the Hadoop `FileSystem` class, making it compatible with Hadoop, Spark, and any application that uses the `org.apache.hadoop.fs.FileSystem` interface. This in-memory file system is ideal for testing and scenarios where disk I/O might be a bottleneck.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
- [Usage](#usage)
    - [Including in Your Project](#including-in-your-project)
    - [Example: Hadoop FileSystem Operations](#example-hadoop-filesystem-operations)
    - [Example: Spark Application](#example-spark-application)
- [Contributing](#contributing)
- [License](#license)

## Introduction

**SimpleMemoryFS** provides an in-memory implementation of the Hadoop `FileSystem` interface. It supports large files using chunked data storage and allows for standard file system operations such as read, write, append, rename, and delete. By operating entirely in memory, it offers high-speed file operations without the overhead of disk I/O.

## Features

- **In-Memory Storage**: All data is stored in memory, providing fast read/write operations.
- **Hadoop `FileSystem` Implementation**: Fully implements the `org.apache.hadoop.fs.FileSystem` interface.
- **Compatibility**: Works with Hadoop, Spark, and other applications using Hadoop's `FileSystem`.
- **Large File Support**: Uses chunked data storage to support large files.
- **Thread-Safe**: Utilizes concurrent data structures and synchronization mechanisms for safe multithreaded access.

## Getting Started

### Prerequisites

- **Java 8** or higher
- **Apache Maven** for building the project
- **Apache Hadoop** (Core Libraries) if running Hadoop or Spark applications
- **Apache Spark** if running Spark applications

### Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yasserlamsaougar/simplememoryfs.git
   ```

2. **Build the project using Maven**:

   ```bash
   cd simplememoryfs
   mvn clean install
   ```

   This will compile the source code and package it into a JAR file located in the `target` directory.

## Usage

### Example: Hadoop FileSystem Operations

Here's an example of using **SimpleMemoryFS** for standard Hadoop filesystem operations.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.yla.learning.project.SimpleMapMemoryFileSystem;

import java.io.OutputStream;
import java.io.InputStream;

public class HadoopFSExample {
    public static void main(String[] args) throws Exception {
        // Create a new Configuration and set the FileSystem to use SimpleMemoryFS
        Configuration conf = new Configuration();
        conf.setClass("fs.memoryfs.impl", SimpleMapMemoryFileSystem.class, FileSystem.class);
        conf.set("fs.defaultFS", "memoryfs:///");

        // Get the FileSystem instance
        FileSystem fs = FileSystem.get(conf);

        // Create a new file and write data to it
        Path filePath = new Path("/example.txt");
        try (OutputStream out = fs.create(filePath)) {
            out.write("Hello, SimpleMemoryFS!".getBytes());
        }

        // Read data from the file
        try (InputStream in = fs.open(filePath)) {
            byte[] buffer = new byte[1024];
            int bytesRead = in.read(buffer);
            String content = new String(buffer, 0, bytesRead);
            System.out.println("File Content: " + content);
        }

        // List files in the root directory
        for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(new Path("/"))) {
            System.out.println("Found: " + status.getPath());
        }

        // Rename the file
        Path newFilePath = new Path("/renamed_example.txt");
        fs.rename(filePath, newFilePath);

        // Delete the file
        fs.delete(newFilePath, false);
    }
}
```

**Explanation:**

- **Configuration Setup**: We configure Hadoop to use `SimpleMapMemoryFileSystem` for the `memoryfs` scheme.
- **File Operations**: Demonstrates file creation, writing, reading, listing, renaming, and deletion.
- **Execution**: Compile and run the code with the **SimpleMemoryFS** JAR in the classpath.

### Example: Spark Application

You can use **SimpleMemoryFS** as the file system for Spark jobs. Here's a simple Spark application that writes and reads data using **SimpleMemoryFS**.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.yla.learning.project.SimpleMapMemoryFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.io.OutputStream;

public class SparkExample {
    public static void main(String[] args) throws Exception {
        // Configure Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SimpleMemoryFSSparkApp")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Set up Hadoop configuration to use SimpleMemoryFS
        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.setClass("fs.memoryfs.impl", SimpleMapMemoryFileSystem.class, FileSystem.class);
        hadoopConf.set("fs.defaultFS", "memoryfs:///");

        // Get the FileSystem instance
        FileSystem fs = FileSystem.get(hadoopConf);

        // Define a path in the in-memory file system
        Path dataPath = new Path("/data/input.txt");

        // Write some data to the in-memory file system
        try (OutputStream out = fs.create(dataPath)) {
            out.write("Spark with SimpleMemoryFS!\nLine 2\nLine 3".getBytes());
        }

        // Read data using Spark
        JavaRDD<String> lines = sc.textFile("memoryfs:///data/input.txt");
        lines.foreach(line -> System.out.println("Read Line: " + line));

        sc.stop();
    }
}
```

**Explanation:**

- **Spark Configuration**: We set the Spark context to use `SimpleMapMemoryFileSystem` by configuring the Hadoop settings.
- **Writing Data**: We write a text file into the in-memory file system.
- **Reading Data with Spark**: We use `sc.textFile()` to read from the in-memory file system and print each line.
- **Execution**: Run the Spark application with the **SimpleMemoryFS** JAR included in the classpath.

**Note**: When running Spark applications, make sure that the **SimpleMemoryFS** JAR is accessible to all executors. This can be achieved by adding the JAR to the `--jars` option when submitting the Spark job or placing it in the `jars` directory of your Spark installation.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

