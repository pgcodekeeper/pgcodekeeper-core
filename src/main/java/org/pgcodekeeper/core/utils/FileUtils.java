/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.utils;

import org.pgcodekeeper.core.localizations.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Comprehensive file system utility class providing various file operations,
 * temporary file/directory management, ZIP file handling, filename validation,
 * and database project-related file operations.
 */
public final class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    private static final boolean IS_POSIX = FileSystems.getDefault().supportedFileAttributeViews().contains("posix"); //$NON-NLS-1$

    private static final FileAttribute<?> POSIX_PERMISSIONS =
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")); //$NON-NLS-1$

    private static final String EMPTY_STRING = ""; //$NON-NLS-1$

    private static final Random RANDOM = new SecureRandom();

    private static final DateTimeFormatter FILE_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH''mm''ss"); //$NON-NLS-1$
    private static final Pattern INVALID_FILENAME = Pattern.compile("[\\\\/:*?\"<>|]"); //$NON-NLS-1$
    private static final Pattern MS_DB_NAME_PATTERN = Pattern.compile("=[^;]+;"); //$NON-NLS-1$

    /**
     * Deletes folder and its contents recursively.
     *
     * @param f the path to delete recursively
     * @throws IOException if deletion fails
     */
    public static void deleteRecursive(Path f) throws IOException {
        if (Files.isDirectory(f, LinkOption.NOFOLLOW_LINKS)) {
            try (Stream<Path> stream = Files.list(f)) {
                for (Path sub : Utils.streamIterator(stream)) {
                    deleteRecursive(sub);
                }
            } catch (UncheckedIOException wrapEx) {
                throw wrapEx.getCause();
            }
        }
        removeReadOnly(f);
    }

    /**
     * Removes read-only attribute from file and deletes it.
     * Handles DOS file attributes and symlink edge cases on Linux.
     *
     * @param path the path to remove read-only attribute from and delete
     * @throws IOException if operation fails
     */
    public static void removeReadOnly(Path path) throws IOException {
        DosFileAttributeView att = Files.getFileAttributeView(
                path, DosFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (att != null) {
            try {
                att.setReadOnly(false);
            } catch (FileSystemException ex) {
                // expected behaviour for symlinks on linux
                // the impl calls open(path, O_RDONLY | O_NOFOLLOW, 0)
                // which returns ELOOP which is translated into this exception
            }
        }
        Files.deleteIfExists(path);
    }

    /**
     * Sanitizes filename by removing invalid characters.
     * Removes characters: {@code \ / : * ? " < > |}
     *
     * @param name the filename to sanitize
     * @return sanitized filename with invalid characters removed
     */
    public static String sanitizeFilename(String name) {
        return INVALID_FILENAME.matcher(name).replaceAll(EMPTY_STRING); // $NON-NLS-1$
    }

    /**
     * Gets valid filename by replacing invalid characters with underscores.
     * Replaces characters: {@code \ / : * ? " < > | with '_'}
     *
     * @param name the filename to validate
     * @return valid filename with invalid characters replaced by underscores
     */
    public static String getValidFilename(String name) {
        Matcher m = INVALID_FILENAME.matcher(name);
        return m.find() ? m.replaceAll("_") : name; //$NON-NLS-1$
    }

    /**
     * Gets current date and time formatted for filename usage.
     * Format: "yyyy-MM-dd HH''mm''ss"
     *
     * @return formatted current date and time string
     */
    public static String getFileDate() {
        return FILE_DATE.format(LocalDateTime.now());
    }

    /**
     * Checks if file is a valid ZIP file by reading file signature.
     * Recognizes ZIP file signatures: 0x504B0304, 0x504B0506, 0x504B0708
     *
     * @param path the file path to check
     * @return true if file is a valid ZIP file, false otherwise
     * @throws IOException if file cannot be read
     */
    public static boolean isZipFile(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) { //$NON-NLS-1$
            int fileSignature = raf.readInt();
            return fileSignature == 0x504B0304 || fileSignature == 0x504B0506
                    || fileSignature == 0x504B0708;
        } catch (EOFException e) {
            return false;
        }
    }

    /**
     * Generates unique file path for unzipped content based on original path.
     * Creates name using original filename + MD5 hash of relative path.
     *
     * @param metaPath the base metadata path
     * @param path     the original file path
     * @return unique path for unzipped file
     */
    public static Path getUnzippedFilePath(Path metaPath, Path path) {
        String hash;
        if (path.startsWith(metaPath)) {
            hash = metaPath.relativize(path).toString();
        } else {
            hash = path.toString();
        }

        String name = path.getFileName().toString() + '_' + Utils.md5(hash).substring(0, 10);

        return metaPath.resolve(name);
    }

    /**
     * Generates unique file path for loaded content from URI.
     * Creates name using valid filename + MD5 hash of URI path.
     *
     * @param metaPath the base metadata path
     * @param uri      the source URI
     * @return unique path for loaded file
     */
    public static Path getLoadedFilePath(Path metaPath, URI uri) {
        String path = uri.getPath();
        String fileName = FileUtils.getValidFilename(Paths.get(path).getFileName().toString());
        String name = fileName + '_' + Utils.md5(path).substring(0, 10);
        return metaPath.resolve(name);
    }

    /**
     * Extracts database name from JDBC URL.
     * Handles Microsoft SQL Server URLs specially, otherwise uses URI parsing.
     *
     * @param url the JDBC URL
     * @return database name extracted from URL, or empty string if extraction fails
     */
    public static String dbNameFromUrl(String url) {
        if (url.startsWith("jdbc:sqlserver")) { //$NON-NLS-1$
            Matcher m = MS_DB_NAME_PATTERN.matcher(url);
            if (m.find()) {
                String s = m.group();
                return s.substring(1, s.length() - 1).replace("{", EMPTY_STRING).replace("}", EMPTY_STRING); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }

        try {
            return getNameFromUri(new URI(url.substring(5)));
        } catch (URISyntaxException e) {
            return EMPTY_STRING;
        }
    }

    /**
     * Extracts name (last path segment) from URI.
     *
     * @param uri the URI to extract name from
     * @return last path segment of URI, or full URI string if path is null, or null if URI is null
     */
    public static String getNameFromUri(URI uri) {
        if (uri == null) {
            return null;
        }
        String urlPath = uri.getPath();
        if (urlPath != null) {
            return urlPath.substring(urlPath.lastIndexOf('/') + 1);
        }

        return uri.toString();
    }

    /**
     * Reads resource file content as UTF-8 string.
     *
     * @param clazz    the class to load resource relative to
     * @param fileName the resource file name
     * @return resource content as UTF-8 string, or null if resource not found
     * @throws IOException if reading fails
     */
    public static String readResource(Class<?> clazz, String fileName) throws IOException {
        try (InputStream inputStream = clazz.getResourceAsStream(fileName)) {
            if (inputStream == null) {
                return null;
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Downloads content from URI to file in directory.
     * Creates directory if it doesn't exist. Handles race conditions and cleanup on failure.
     *
     * @param uri      the URI to download from
     * @param fileName the target file name
     * @param dir      the target directory
     * @throws IOException if download or file operations fail
     */
    public static void loadURI(URI uri, String fileName, Path dir) throws IOException {
        // do nothing if directory already exists
        if (Files.exists(dir)) {
            return;
        }

        Path file = dir.resolve(fileName);
        try (InputStream in = uri.toURL().openStream()) {
            Files.createDirectories(dir);
            Files.copy(in, file);
        } catch (FileAlreadyExistsException e) {
            // someone else won the race and created the file
        } catch (IOException e) {
            IOException ioe = new IOException(
                    Messages.FileUtils_error_while_read_uri_lib.formatted(uri, e.getLocalizedMessage()), e);

            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                ioe.addSuppressed(ex);
            }

            try {
                Files.deleteIfExists(dir);
            } catch (IOException ex) {
                ioe.addSuppressed(ex);
            }

            throw ioe;
        }
    }

    /**
     * Extracts ZIP file to target directory.
     * Uses temporary directory with unique name to avoid parallel extraction conflicts.
     * Returns real path of extraction directory.
     *
     * @param zip the ZIP file to extract
     * @param dir the target directory for extraction
     * @return real path of extraction directory as string
     * @throws IOException if extraction fails
     */
    public static String unzip(Path zip, Path dir) throws IOException {
        // return output directory if it exists
        if (Files.exists(dir)) {
            return dir.toString();
        }
        // create a directory with a unique name to avoid problems with parallel downloads
        Path tempDir = dir.resolveSibling(dir.getFileName() + "_" + RANDOM.nextInt()); //$NON-NLS-1$

        Files.createDirectories(tempDir);
        Path destDir = tempDir.toRealPath();

        try (FileSystem fs = FileSystems.newFileSystem(zip, (ClassLoader) null)) {
            final Path root = fs.getPath("/"); //$NON-NLS-1$

            // walk the zip file tree and copy files to the destination
            Files.walkFileTree(root, new SimpleFileVisitor<>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Path destFile = Paths.get(destDir.toString(), file.toString());
                    Files.copy(file, destFile, StandardCopyOption.REPLACE_EXISTING);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path dirToCreate = Paths.get(destDir.toString(), dir.toString());
                    if (Files.notExists(dirToCreate)) {
                        Files.createDirectory(dirToCreate);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        }

        try {
            Files.move(tempDir, dir, StandardCopyOption.REPLACE_EXISTING);
        } catch (FileSystemException e) {
            // another thread already created the directory
            deleteRecursive(tempDir);
        }

        return dir.toRealPath().toString();
    }

    /**
     * Creates temporary file with secure permissions.
     * On POSIX systems, sets permissions to rwx------. On other systems, uses file attribute methods.
     *
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @return path to created temporary file
     * @throws IOException if file creation fails
     */
    public static Path createTempFile(String prefix, String suffix) throws IOException {
        var msg = Messages.FileUtils_creating_temp_file.formatted(prefix);
        LOG.info(msg);
        if (IS_POSIX) {
            return Files.createTempFile(prefix, suffix, POSIX_PERMISSIONS);
        }

        Path path = Files.createTempFile(prefix, suffix);
        File f = path.toFile();
        setAttributes(f);
        return path;
    }

    /**
     * Creates temporary file in specified directory.
     *
     * @param dir    the directory to create file in
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @return path to created temporary file
     * @throws IOException if file creation fails
     */
    public static Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        var msg = Messages.FileUtils_creating_temp_file.formatted(prefix);
        LOG.info(msg);
        return Files.createTempFile(dir, prefix, suffix);
    }

    /**
     * Creates temporary directory with secure permissions.
     * On POSIX systems, sets permissions to rwx------. On other systems, uses file attribute methods.
     *
     * @param prefix the directory name prefix
     * @return path to created temporary directory
     * @throws IOException if directory creation fails
     */
    public static Path createTempDirectory(String prefix) throws IOException {
        var msg = Messages.FileUtils_creating_temp_directory.formatted(prefix);
        LOG.info(msg);

        if (IS_POSIX) {
            return Files.createTempDirectory(prefix, POSIX_PERMISSIONS);
        }

        Path path = Files.createTempDirectory(prefix);
        File f = path.toFile();
        setAttributes(f);
        return path;
    }

    /**
     * Creates temporary directory in specified parent directory.
     *
     * @param dir    the parent directory
     * @param prefix the directory name prefix
     * @return path to created temporary directory
     * @throws IOException if directory creation fails
     */
    public static Path createTempDirectory(Path dir, String prefix) throws IOException {
        var msg = Messages.FileUtils_creating_temp_directory.formatted(prefix);
        LOG.info(msg);
        return Files.createTempDirectory(dir, prefix);
    }

    private static void setAttributes(File f) throws IOException {
        var readable = f.setReadable(true, true);
        if (!readable) {
            throw new IOException("Failed to set temp file readable"); //$NON-NLS-1$
        }

        var writable = f.setWritable(true, true);
        if (!writable) {
            throw new IOException("Failed to set temp file writable"); //$NON-NLS-1$
        }

        var executable = f.setExecutable(true, true);
        if (!executable) {
            throw new IOException("Failed to set temp file executable"); //$NON-NLS-1$
        }
    }


    private FileUtils() {
    }
}
