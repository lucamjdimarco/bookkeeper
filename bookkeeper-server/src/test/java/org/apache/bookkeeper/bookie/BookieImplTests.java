package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Supplier;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value = Enclosed.class)
public class BookieImplTests {

    @RunWith(Parameterized.class)
    public static class TestCheckDirectoryStructure {

        private final boolean exists;
        private final boolean oldLayout;
        private final boolean mkdirsSuccess;
        private final boolean isDirectory;
        private final Class<? extends Exception> expectedException;

        private TmpDirs tmpDirs;
        private Path testDir;
        private Path parentDir;

        public TestCheckDirectoryStructure(boolean exists, boolean oldLayout, boolean mkdirsSuccess, boolean isDirectory,
                                           Class<? extends Exception> expectedException) {
            this.exists = exists;
            this.oldLayout = oldLayout;
            this.mkdirsSuccess = mkdirsSuccess;
            this.isDirectory = isDirectory;
            this.expectedException = expectedException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // exists, oldLayout, mkdirsSuccess, isDirectory, expectedException

                    // Test Case 1: Directory esistente, nessun vecchio layout, dovrebbe passare
                    {true, false, true, true, null},
                    // Test Case 2: Directory non esistente, nessun vecchio layout, mkdirs() ha successo, dovrebbe passare
                    {false, false, true, true, null},
                    // Test Case 3: Directory non esistente, vecchio layout presente, mi aspetto eccezione
                    {false, true, true, true, IOException.class},
                    // Test Case 4: Directory non esistente, nessun vecchio layout, mkdirs() fallisce, mi aspetto eccezione
                    {false, false, false, true, IOException.class},
                    // Test Case 5: Directory esistente, ma è un file, mi aspetto eccezione
                    {true, false, true, false, IOException.class},
                    // Test Case 6: Directory non esistente, genitore non accessibile, mi aspetto eccezione
                    {false, false, true, true, IOException.class},
            });
        }

        @Before
        public void setUp() throws Exception {
            tmpDirs = new TmpDirs();

            // directory padre
            parentDir = tmpDirs.createNew("parentDir", null).toPath();

            // parent non accessibile (Test Case 6)
            if (expectedException == IOException.class && !exists && !oldLayout && mkdirsSuccess && testCaseIs(6)) {
                parentDir.toFile().setReadable(false);
                parentDir.toFile().setExecutable(false);
            }

            if (exists) {
                if (isDirectory) {
                    testDir = tmpDirs.createNew("testDir", null).toPath();
                } else {
                    testDir = Files.createFile(parentDir.resolve("testDir"));
                }
            } else {
                testDir = parentDir.resolve("testDir");
            }

            if (oldLayout) {
                // Creo un file di vecchio layout nella directory padre
                Files.createFile(parentDir.resolve("file.txn"));
            }

            // mock di mkdirs() per simulare
            if (!mkdirsSuccess && !exists) {
                File dirFile = Mockito.spy(testDir.toFile());
                Mockito.doReturn(false).when(dirFile).mkdirs();
                testDir = dirFile.toPath();
            }
        }

        @Test
        public void testCheckDirectoryStructure() {
            try {
                BookieImpl.checkDirectoryStructure(testDir.toFile());
                if (expectedException != null) {
                    fail("Expected exception " + expectedException.getSimpleName() + " but none was thrown.");
                }
            } catch (Exception e) {
                if (expectedException == null) {
                    fail("Unexpected exception thrown: " + e);
                } else if (!expectedException.isInstance(e)) {
                    fail("Expected exception " + expectedException.getSimpleName() + " but got " + e.getClass().getSimpleName());
                }
            }
        }

        @After
        public void tearDown() throws Exception {
            if (parentDir != null) {
                parentDir.toFile().setReadable(true);
                parentDir.toFile().setExecutable(true);
            }
            if (tmpDirs != null) {
                tmpDirs.cleanup();
            }
        }


        //helper per identificare il numero del test case corrente
        private boolean testCaseIs(int testCaseNumber) {
            return data().toArray()[testCaseNumber - 1] == this;
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieIdTest {

        private final ServerConfiguration conf;
        private final boolean expectException;

        public BookieImplGetBookieIdTest(ServerConfiguration conf, boolean expectException) {
            this.conf = conf;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Configurazioni valide
                    {createValidConfig(1, "127.0.0.1", "eth0", 1), false},       // Limite inferiore delle porte
                    {createValidConfig(65535, "localhost", "lo", 65535), false}, // Limite superiore delle porte
                    {createValidConfig(3181, "192.168.1.1", "eth0", 8080), false}, // Configurazione valida

                    // Configurazioni non valide
                    {createInvalidConfig(0, "127.0.0.1", "eth0", 8080), true},         // Porta bookie fuori range (inferiore)
                    {createInvalidConfig(65536, "127.0.0.1", "eth0", 8080), true},     // Porta bookie fuori range (superiore)
                    {createInvalidConfig(3181, null, "eth0", 8080), true},             // AdvertisedAddress null
                    {createInvalidConfig(3181, "", "eth0", 8080), true},               // AdvertisedAddress vuoto
                    {createInvalidConfig(3181, "invalid_address", "eth0", 8080), true},// AdvertisedAddress non risolvibile
                    {createInvalidConfig(3181, "127.0.0.1", null, 8080), true},        // ListeningInterface null
                    {createInvalidConfig(3181, "127.0.0.1", "", 8080), true},          // ListeningInterface vuoto
                    {createInvalidConfig(3181, "127.0.0.1", "invalid_iface", 8080), true}, // ListeningInterface non esistente
                    {createInvalidConfig(3181, "127.0.0.1", "eth0", 0), true},         // Porta HTTP fuori range (inferiore)
                    {createInvalidConfig(3181, "127.0.0.1", "eth0", 65536), true},     // Porta HTTP fuori range (superiore)
                    {null, true},                                                      // Configurazione null
            });
        }

        private static ServerConfiguration createValidConfig(int bookiePort, String advertisedAddress, String listeningInterface, int httpPort) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setBookiePort(bookiePort);
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setListeningInterface(listeningInterface);
            conf.setHttpServerPort(httpPort);
            return conf;
        }

        private static ServerConfiguration createInvalidConfig(Integer bookiePort, String advertisedAddress, String listeningInterface, Integer httpPort) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            if (bookiePort != null) conf.setBookiePort(bookiePort);
            if (advertisedAddress != null) conf.setAdvertisedAddress(advertisedAddress);
            if (listeningInterface != null) conf.setListeningInterface(listeningInterface);
            if (httpPort != null) conf.setHttpServerPort(httpPort);
            return conf;
        }

        @Test
        public void testGetBookieId() {
            try {
                BookieId bookieId = BookieImpl.getBookieId(conf);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);
            } catch (UnknownHostException | IllegalArgumentException e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            } catch (NullPointerException e) {
                if (conf == null) {
                    assertTrue("Null configuration should result in NullPointerException.", true);
                } else {
                    fail("Unexpected NullPointerException thrown: " + e.getMessage());
                }
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieAddressTest {

        private final ServerConfiguration conf;
        private final boolean expectException;

        public BookieImplGetBookieAddressTest(ServerConfiguration conf, boolean expectException) {
            this.conf = conf;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Configurazioni valide
                    {createValidConfig(1, "127.0.0.1", "eth0", false), false},        // Porta minima
                    {createValidConfig(65535, "localhost", "lo", false), false},      // Porta massima
                    {createValidConfig(3181, "192.168.1.1", "eth0", false), false},   // Configurazione tipica valida
                    {createValidConfig(3181, null, "eth0", false), false},            // AdvertisedAddress null, dovrebbe usare l'indirizzo locale
                    {createValidConfig(3181, "127.0.0.1", null, false), false},       // ListeningInterface null, dovrebbe usare l'indirizzo predefinito
                    {createValidConfig(3181, null, null, true), false},               // useHostNameAsBookieID true

                    // Configurazioni non valide
                    {createInvalidConfig(0, "127.0.0.1", "eth0", false), true},         // Porta bookie fuori range (inferiore)
                    {createInvalidConfig(65536, "127.0.0.1", "eth0", false), true},     // Porta bookie fuori range (superiore)
                    {createInvalidConfig(3181, "", "eth0", false), true},               // AdvertisedAddress vuoto
                    {createInvalidConfig(3181, "invalid_address", "eth0", false), true},// AdvertisedAddress non risolvibile
                    {createInvalidConfig(3181, "127.0.0.1", "", false), true},          // ListeningInterface vuoto
                    {createInvalidConfig(3181, "127.0.0.1", "invalid_iface", false), true}, // ListeningInterface non esistente
                    {null, true},                                                      // Configurazione null
            });
        }

        private static ServerConfiguration createValidConfig(Integer bookiePort, String advertisedAddress, String listeningInterface, boolean useHostnameAsBookieID) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            if (bookiePort != null) conf.setBookiePort(bookiePort);
            if (advertisedAddress != null) conf.setAdvertisedAddress(advertisedAddress);
            if (listeningInterface != null) conf.setListeningInterface(listeningInterface);
            conf.setUseHostNameAsBookieID(useHostnameAsBookieID);
            return conf;
        }

        private static ServerConfiguration createInvalidConfig(Integer bookiePort, String advertisedAddress, String listeningInterface, boolean useHostnameAsBookieID) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            if (bookiePort != null) conf.setBookiePort(bookiePort);
            if (advertisedAddress != null) conf.setAdvertisedAddress(advertisedAddress);
            if (listeningInterface != null) conf.setListeningInterface(listeningInterface);
            conf.setUseHostNameAsBookieID(useHostnameAsBookieID);
            return conf;
        }

        @Test
        public void testGetBookieAddress() {
            try {
                BookieSocketAddress bookieAddress = BookieImpl.getBookieAddress(conf);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieSocketAddress should not be null for valid configuration.", bookieAddress);
            } catch (UnknownHostException | IllegalArgumentException e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            } catch (NullPointerException e) {
                if (conf == null) {
                    // Test passa, ci aspettiamo NullPointerException quando conf è null
                } else {
                    fail("Unexpected NullPointerException thrown: " + e.getMessage());
                }
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetCurrentDirectoriesTest {

        private static TmpDirs tmpDirs;
        private static File existingDir;
        private static File anotherExistingDir;
        private static File nonAccessibleDir;
        private static File nonExistingDir;
        private static File regularFile;

        private final File[] dirs;
        private final boolean expectException;

        public BookieImplGetCurrentDirectoriesTest(File[] dirs, boolean expectException) {
            this.dirs = dirs;
            this.expectException = expectException;
        }

        @BeforeClass
        public static void setUpClass() throws Exception {
            tmpDirs = new TmpDirs();

            // Create resources for testing using TmpDirs
            existingDir = tmpDirs.createNew("existingDir", null);
            anotherExistingDir = tmpDirs.createNew("anotherExistingDir", null);

            nonAccessibleDir = tmpDirs.createNew("nonAccessibleDir", null);
            nonAccessibleDir.setReadable(false);
            nonAccessibleDir.setExecutable(false);

            nonExistingDir = new File("nonExistingDir");

            regularFile = tmpDirs.createNew("regularFile", ".txt");
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid: dir singola accessibile ed esistente
                    {new File[]{existingDir}, false},
                    // valid: molteplici dir accessibili ed esistenti
                    {new File[]{existingDir, anotherExistingDir}, false},
                    // invalid: dir esistente ma non accessibile
                    {new File[]{nonAccessibleDir}, true},
                    // invalid: dir non esistente
                    {new File[]{nonExistingDir}, true},
                    // invalid: array vuoto
                    {new File[]{}, false},
                    // invalid: null
                    {null, true},
                    // invalid: file invece che una directory
                    {new File[]{regularFile}, true},
                    // invalid: elemento null all'interno dell'array
                    {new File[]{existingDir, null}, true},
            });
        }

        @Test
        public void testGetCurrentDirectories() {
            try {
                File[] result = BookieImpl.getCurrentDirectories(dirs);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("Result should not be null", result);

            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            }
        }

        @AfterClass
        public static void tearDownClass() {
            restorePermissions(nonAccessibleDir);
            if (tmpDirs != null) {
                try {
                    tmpDirs.cleanup();
                } catch (IOException e) {
                    System.err.println("Failed to clean up temporary directories: " + e.getMessage());
                    e.printStackTrace();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // devo ripristinare i permessi delle dir altrimenti non posso eliminarle
        private static void restorePermissions(File dir) {
            if (dir != null && dir.exists()) {
                dir.setReadable(true);
                dir.setExecutable(true);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplMountLedgerStorageOfflineTest {

        private final ServerConfiguration conf;
        private final LedgerStorage ledgerStorage;
        private final boolean expectException;

        public BookieImplMountLedgerStorageOfflineTest(ServerConfiguration conf, LedgerStorage ledgerStorage, boolean expectException) {
            this.conf = conf;
            this.ledgerStorage = ledgerStorage;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Valid
                    {createValidConfig(), createMockLedgerStorage(false), false},   // conf valida con ledgerStorage non inizializzato
                    {createValidConfig(), null, false},                            // conf valida con ledgerStorage null

                    // Invalid
                    {null, createMockLedgerStorage(false), true},                  // conf null
                    {createInvalidConfig(), createMockLedgerStorage(false), true}, // conf invalida con ledgerStorage non inizializzato
                    {createValidConfig(), createMockLedgerStorage(true), true},    // conf valida con ledgerStorage già inizializzato
                    {createInvalidConfig(), null, true},                           // conf invalida e ledgerStorage null
                    {null, null, true},                                            // conf null e ledgerStorage null
            });
        }

        private static ServerConfiguration createValidConfig() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setLedgerDirNames(new String[]{"/tmp/ledgerDirs"});
            conf.setDiskUsageThreshold(0.90f);
            conf.setDiskUsageWarnThreshold(0.85f);
            return conf;
        }

        private static ServerConfiguration createInvalidConfig() {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            // conf vuota o non valida
            conf.setLedgerDirNames(null); // no directory impostata
            return conf;
        }

        private static LedgerStorage createMockLedgerStorage(boolean alreadyInitialized) {
            LedgerStorage mockStorage = mock(LedgerStorage.class);
            if (alreadyInitialized) {
                try {
                    doThrow(new IllegalStateException("LedgerStorage already initialized"))
                            .when(mockStorage).initialize(any(), any(), any(), any(), any(), any());
                } catch (IOException e) {
                    fail("Unexpected IOException during mock setup");
                }
            }
            return mockStorage;
        }

        @Test
        public void testMountLedgerStorageOffline() {
            try {
                LedgerStorage result = BookieImpl.mountLedgerStorageOffline(conf, ledgerStorage);
                if (expectException) {
                    fail("Expected exception but none was thrown.");
                }
                assertNotNull("Resulting LedgerStorage should not be null", result);
                assertEquals("Returned LedgerStorage should match input LedgerStorage when provided",
                        ledgerStorage != null ? ledgerStorage : result, result);
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception: " + e.getMessage());
                }
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplTriggerShutdownParameterizedTest {

        private final int exitCode;
        private final boolean isAlreadyTriggered;
        private final boolean expectShutdownCall;

        public BookieImplTriggerShutdownParameterizedTest(int exitCode, boolean isAlreadyTriggered, boolean expectShutdownCall) {
            this.exitCode = exitCode;
            this.isAlreadyTriggered = isAlreadyTriggered;
            this.expectShutdownCall = expectShutdownCall;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // exit code = 0 (arresto normale), non già attivato
                    {0, false, true},
                    // exit code > 0 (arresto anomalo), non già attivato
                    {1, false, true},
                    // exit code < 0 (arresto anomalo), non già attivato
                    {-1, false, true},
                    // shutdown già attivato
                    {1, true, false},
                    {0, true, false},
            });
        }

        @Test
        public void testTriggerBookieShutdown() throws Exception {

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            BookieImpl bookie = spy(new TestBookieImpl(conf));

            // imposto lo stato iniziale di shutdownTriggered
            bookie.shutdownTriggered.set(isAlreadyTriggered);
            bookie.triggerBookieShutdown(exitCode);
            if (expectShutdownCall) {
                verify(bookie, timeout(1000)).shutdown(exitCode);
            } else {
                verify(bookie, never()).shutdown(anyInt());
            }
        }
    }

    /*@RunWith(Parameterized.class)
    public static class BookieImplGetLedgerForEntryTest {

        private final ByteBuf entry;
        private final byte[] masterKey;
        private final boolean expectException;

        public BookieImplGetLedgerForEntryTest(ByteBuf entry, byte[] masterKey, boolean expectException) {
            this.entry = entry;
            this.masterKey = masterKey;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // nel ByteBuf i dati sono scritti in maniera sequenziale
                    // metadati: ledgerId (long), entryId (long), lastConfirmedLAC (long)
                    // valida
                    {createValidEntry(1L), new byte[]{1, 2, 3}, false},
                    // null
                    {null, new byte[]{1, 2, 3}, true},
                    // metadati negativi
                    {createInvalidEntry(-1L), new byte[]{1, 2, 3}, true},
                    // metadati assenti
                    {createInvalidEntry(0L), new byte[]{1, 2, 3}, true},
                    // entry valida, masterKey null
                    {createValidEntry(1L), null, true},
                    // entry valida, masterKey vuota
                    {createValidEntry(1L), new byte[]{}, true},
                    // entry valida, masterKey non valida
                    {createValidEntry(1L), new byte[]{9, 9, 9}, true},
            });
        }

        private static ByteBuf createValidEntry(long ledgerId) {
            ByteBuf entry = Unpooled.buffer();
            entry.writeLong(ledgerId); // Ledge ID
            entry.writeLong(1L); // Entry ID
            entry.writeLong(0L); // Last confirmed LC
            entry.writeBytes(new byte[]{10, 20, 30}); // Data
            return entry;
        }

        private static ByteBuf createInvalidEntry(long ledgerId) {
            ByteBuf entry = Unpooled.buffer();
            entry.writeLong(ledgerId);
            return entry;
        }

        @Test
        public void testGetLedgerForEntry() throws IOException {
            // Mock degli handles per simulare la gestione dei LedgerDescriptor
            HandleFactory handles = mock(HandleFactory.class);

            // Mock del LedgerDescriptor
            LedgerDescriptor mockDescriptor = mock(LedgerDescriptor.class);
            try {
                if (entry != null && entry.readerIndex() >= 0) {
                    when(handles.getHandle(anyLong(), eq(masterKey))).thenReturn(mockDescriptor);
                } else {
                    when(handles.getHandle(anyLong(), eq(masterKey))).thenThrow(BookieException.class);
                }
            } catch (BookieException e) {
                // Non gestire eccezioni qui, sono simulate nei test
            }

            // Creazione di BookieImpl
            BookieImpl bookie = new BookieImpl(null, null, null, null, null, null, null, null, null) {
                @Override
                HandleFactory getHandles() {
                    return handles; // Usa il mock degli handles
                }
            };

            try {
                LedgerDescriptor descriptor = bookie.getLedgerForEntry(entry, masterKey);
                if (expectException) {
                    fail("Expected an exception but none was thrown.");
                }
                assertNotNull("Descriptor should not be null for valid inputs.", descriptor);
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            }
        }
    }*/

    

}
