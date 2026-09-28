package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/** ZIP implementation shared by durable checkpoint stores. */
final class CheckpointArchives {
    private CheckpointArchives() {
    }

    static String entryPoint(Path checkpointDirectory, StateHandle handle) throws IOException {
        if (!"file".equalsIgnoreCase(handle.uri().getScheme())) {
            throw new IOException("a local checkpoint entry point must be a file URI: " + handle.uri());
        }
        Path root = checkpointDirectory.toAbsolutePath().normalize();
        Path entry = Path.of(handle.uri()).toAbsolutePath().normalize();
        if (!entry.startsWith(root) || entry.equals(root)) {
            throw new IOException("checkpoint entry point must be inside its checkpoint directory: " + entry);
        }
        return root.relativize(entry).toString().replace('\\', '/');
    }

    static URI withEntryPoint(URI archiveUri, String entryPoint) throws IOException {
        try {
            return new URI(archiveUri.getScheme(), archiveUri.getAuthority(), archiveUri.getPath(),
                    archiveUri.getQuery(), entryPoint);
        } catch (java.net.URISyntaxException failure) {
            throw new IOException("could not create checkpoint archive URI", failure);
        }
    }

    static void archive(Path directory, Path archive) throws IOException {
        Path root = directory.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IOException("checkpoint directory does not exist: " + root);
        }
        Files.createDirectories(archive.toAbsolutePath().getParent());
        try (ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(archive)))) {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    String name = root.relativize(file).toString().replace('\\', '/');
                    zip.putNextEntry(new ZipEntry(name));
                    Files.copy(file, zip);
                    zip.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    static StateHandle extract(Path archive, URI archiveUri, Path targetDirectory, long sizeBytes)
            throws IOException {
        String entryPoint = archiveUri.getFragment();
        if (entryPoint == null || entryPoint.isBlank()) {
            throw new IOException("checkpoint archive handle has no entry-point fragment: " + archiveUri);
        }
        Path target = targetDirectory.toAbsolutePath().normalize();
        deleteTree(target);
        Files.createDirectories(target);
        try (ZipInputStream zip = new ZipInputStream(new BufferedInputStream(Files.newInputStream(archive)))) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                Path destination = target.resolve(entry.getName()).normalize();
                if (!destination.startsWith(target)) {
                    throw new IOException("checkpoint archive contains an unsafe entry: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(zip, destination);
                }
                zip.closeEntry();
            }
        }
        Path entry = target.resolve(entryPoint).normalize();
        if (!entry.startsWith(target) || !Files.isRegularFile(entry)) {
            throw new IOException("checkpoint archive entry point is missing: " + entryPoint);
        }
        return new StateHandle(entry.toUri(), sizeBytes);
    }

    static void deleteTree(Path target) throws IOException {
        if (!Files.exists(target)) {
            return;
        }
        Files.walkFileTree(target, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException failure) throws IOException {
                if (failure != null) {
                    throw failure;
                }
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
