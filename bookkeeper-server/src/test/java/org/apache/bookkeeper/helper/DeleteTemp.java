package org.apache.bookkeeper.helper;

import java.io.File;

public class DeleteTemp {

    private DeleteTemp() {
        throw new IllegalStateException("Cannot instantiate utility class");
    }

    /**
     * Elimina i file e le directory specificati nei parametri.
     *
     * @param journalDirs Array di directory journal.
     * @param indexDirs Array di directory index.
     * @param ledgerDirs Array di directory ledger.
     */
    public static void deleteFiles(File[] journalDirs, File[] indexDirs, File[] ledgerDirs) {
        if (journalDirs != null) {
            deleteFilesRecursive(journalDirs);
        }
        if (indexDirs != null) {
            deleteFilesRecursive(indexDirs);
        }
        if (ledgerDirs != null) {
            deleteFilesRecursive(ledgerDirs);
        }
    }

    /**
     * Elimina ricorsivamente i file e le directory specificate.
     *
     * @param dirs Array di directory da eliminare.
     */
    private static void deleteFilesRecursive(File[] dirs) {
        for (File dir : dirs) {
            if (dir != null) {
                deleteFileOrDirectory(dir);
            }
        }
    }

    /**
     * Elimina ricorsivamente un file o directory.
     *
     * @param fileOrDir File o directory da eliminare.
     */
    private static void deleteFileOrDirectory(File fileOrDir) {
        if (fileOrDir.isDirectory()) {
            File[] children = fileOrDir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteFileOrDirectory(child);
                }
            }
        }
        if (!fileOrDir.delete()) {
            System.err.println("Unable to delete: " + fileOrDir.getAbsolutePath());
        }
    }
}