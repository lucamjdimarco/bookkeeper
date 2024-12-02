package org.apache.bookkeeper.helper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public enum EnumForDir {
    EXISTENT_WITH_FILE {
        @Override
        public File[] getDirectories() {
            return createTemporaryDirectoriesWithFiles(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return createTemporaryDirectoryWithFile();
        }
    },
    EXISTENT_DIR_WITH_SUBDIR_AND_FILE {
        @Override
        public File[] getDirectories() {
            return createDirectoriesWithSubdirsAndFiles(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return createTemporaryDirectoryWithSubdirAndFile();
        }
    },
    EXISTENT_DIR_WITH_NON_REMOVABLE_SUBDIR {
        @Override
        public File[] getDirectories() {
            return createDirectoriesWithNonRemovableSubdirs(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return createTemporaryDirectoryWithNonRemovableSubdir();
        }
    },
    EXISTENT_DIR_WITH_NON_REMOVABLE_FILE {
        @Override
        public File[] getDirectories() {
            return createDirectoriesWithNonRemovableFiles(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return createTemporaryDirectoryWithNonRemovableFile();
        }
    },
    EXISTENT_DIR_WITH_NON_REMOVABLE_EMPTY_SUBDIR {
        @Override
        public File[] getDirectories() {
            return createDirectoriesWithNonRemovableEmptySubdirs(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return createTemporaryDirectoryWithNonRemovableEmptySubdir();
        }

    },
    NON_EXISTENT_DIRS {
        @Override
        public File[] getDirectories() {
            return createNonExistentDirectories(3);
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return "nonexistentGcEntryLogMetadataPath";
        }
    },
    EMPTY_ARRAY {
        @Override
        public File[] getDirectories() {
            return new File[0];
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return "";
        }
    },
    NULL {
        @Override
        public File[] getDirectories() {
            return null;
        }
        @Override
        public String getGcEntryLogMetadataCachePath() {
            return null;
        }
    };

    public abstract File[] getDirectories();
    public abstract String getGcEntryLogMetadataCachePath();

    private static File[] createTemporaryDirectoriesWithFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithFile" + i).toFile();
                new File(dir, "file.txt").createNewFile();
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String createTemporaryDirectoryWithFile() {
        try {
            File dir = Files.createTempDirectory("tempGcPathWithFile").toFile();
            new File(dir, "file.txt").createNewFile();
            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] createDirectoriesWithSubdirsAndFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithSubdir" + i).toFile();
                File subDir = new File(dir, "subdir");
                subDir.mkdir();
                new File(subDir, "file.txt").createNewFile();
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String createTemporaryDirectoryWithSubdirAndFile() {
        try {
            File dir = Files.createTempDirectory("tempGcPathWithSubdir").toFile();
            File subDir = new File(dir, "subdir");
            subDir.mkdir();
            new File(subDir, "file.txt").createNewFile();
            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] createDirectoriesWithNonRemovableSubdirs(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableSubdir" + i).toFile();
                File subDir = new File(dir, "subdir");
                subDir.mkdir();
                if (!subDir.setWritable(false)) {
                    throw new RuntimeException("Failed to make directory non-removable");
                }
                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dirs;
    }

    private static String createTemporaryDirectoryWithNonRemovableSubdir() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableSubdir").toFile();

            File subDir = new File(dir, "nonRemovableSubdir");
            if (!subDir.mkdir()) {
                throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
            }

            if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
            }

            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable subdirectory", e);
        }
    }

    private static File[] createNonExistentDirectories(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            dirs[i] = new File("nonexistentDir" + i);
        }
        return dirs;
    }

    private static File[] createDirectoriesWithNonRemovableFiles(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableFile" + i).toFile();
                File nonRemovableFile = new File(dir, "nonRemovableFile.txt");
                if (!nonRemovableFile.createNewFile()) {
                    throw new RuntimeException("Failed to create file: " + nonRemovableFile.getAbsolutePath());
                }
                if (!nonRemovableFile.setWritable(false, false) || !nonRemovableFile.setReadable(false, false)) {
                    throw new RuntimeException("Failed to make file non-removable: " + nonRemovableFile.getAbsolutePath());
                }
                if (!dir.setWritable(false, false)) {
                    throw new RuntimeException("Failed to make directory non-writable: " + dir.getAbsolutePath());
                }

                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory with non-removable file", e);
            }
        }
        return dirs;
    }

    private static String createTemporaryDirectoryWithNonRemovableFile() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableFile").toFile();
            File nonRemovableFile = new File(dir, "nonRemovableFile.txt");
            if (!nonRemovableFile.createNewFile()) {
                throw new RuntimeException("Failed to create file: " + nonRemovableFile.getAbsolutePath());
            }

            if (!nonRemovableFile.setWritable(false, false) || !nonRemovableFile.setReadable(false, false)) {
                throw new RuntimeException("Failed to make file non-removable: " + nonRemovableFile.getAbsolutePath());
            }

            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable file", e);
        }
    }

    private static File[] createDirectoriesWithNonRemovableEmptySubdirs(int count) {
        File[] dirs = new File[count];
        for (int i = 0; i < count; i++) {
            try {
                File dir = Files.createTempDirectory("tempDirWithNonRemovableEmptySubdir" + i).toFile();
                File subDir = new File(dir, "nonRemovableSubdir");
                if (!subDir.mkdir()) {
                    throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
                }

                if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                    throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
                }

                if (!dir.setWritable(false, false)) {
                    throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
                }

                dirs[i] = dir;
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory with non-removable empty subdirectory", e);
            }
        }
        return dirs;
    }

    private static String createTemporaryDirectoryWithNonRemovableEmptySubdir() {
        try {
            File dir = Files.createTempDirectory("tempDirWithNonRemovableEmptySubdir").toFile();

            File subDir = new File(dir, "nonRemovableEmptySubdir");
            if (!subDir.mkdir()) {
                throw new RuntimeException("Failed to create subdirectory: " + subDir.getAbsolutePath());
            }

            if (!subDir.setWritable(false, false) || !subDir.setExecutable(false, false) || !subDir.setReadable(true, false)) {
                throw new RuntimeException("Failed to make subdirectory non-removable: " + subDir.getAbsolutePath());
            }
            if (!dir.setWritable(false, false)) {
                throw new RuntimeException("Failed to make parent directory non-writable: " + dir.getAbsolutePath());
            }

            return dir.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory with non-removable empty subdirectory", e);
        }
    }
}
