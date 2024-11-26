package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.helper.EntryBuilder;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.security.auth.callback.Callback;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
                    // Test Case 2: Directory non esistente, no vecchio layout, mkdirs() ha successo, dovrebbe passare
                    {false, false, true, true, null},
                    // Test Case 3: Directory non esistente, vecchio layout presente, mkdirs() ha successo, mi aspetto eccezione
                    {false, true, true, true, IOException.class},
                    // Test Case 4: Directory esistente, ma non accessibile, nessun vecchio layout, mi aspetto eccezione
                    //{true, false, true, true, IOException.class}
            });
        }

        @Before
        public void setUp() throws Exception {
            tmpDirs = new TmpDirs();

            parentDir = tmpDirs.createNew("parentDir", null).toPath();

            if (exists) {
                if (isDirectory) {
                    testDir = tmpDirs.createNew("testDir", null).toPath();
                } else {
                    testDir = Files.createFile(parentDir.resolve("testDir"));
                }
            } else {
                testDir = parentDir.resolve("testDir");
                if (!mkdirsSuccess) {
                    // simulo il fallimento di mkdirs()
                    File dirFile = Mockito.spy(testDir.toFile());
                    Mockito.doReturn(false).when(dirFile).mkdirs();
                    testDir = dirFile.toPath();

                    if (Files.exists(testDir)) {
                        fail("La directory non dovrebbe esistere in questo caso.");
                    }
                }
            }

            if (oldLayout) {
                // Creo un file di vecchio layout nella directory padre
                Files.createFile(parentDir.resolve("file.txn"));
            }
        }

        @Test
        public void testCheckDirectoryStructure() {
            try {
                if (testDir == null) {
                    if (expectedException == NullPointerException.class) {
                        throw new NullPointerException("testDir is null as expected");
                    } else {
                        fail("testDir is null, but NullPointerException was not expected");
                    }
                }

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
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetBookieIdTest {
        private final String customBookieId;
        private final boolean expectException;

        public BookieImplGetBookieIdTest(String customBookieId, boolean expectException) {
            this.customBookieId = customBookieId;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Valid configuration
                    {"bookie-01.example.com:9000", false},

                    // Invalid configurations
                    {"127.0.0.1:3181!", true},                   // Invalid customBookieId
                    {"", true},                                   // Empty customBookieId
            });
        }

        @Test
        public void testGetBookieId() {
            ServerConfiguration conf = null;
            try {
                conf = TestBKConfiguration.newServerConfiguration();
                if (customBookieId != null) conf.setBookieId(customBookieId);

                BookieId bookieId = BookieImpl.getBookieId(conf);

                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);

            } catch (IllegalArgumentException | UnknownHostException e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            } catch (Exception e) {
                fail("Unexpected exception type: " + e.getClass().getName());
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
                    // valide
                    {createConfig("127.0.0.1", 3181), false},    // Advertised address, valid interface, valid port

                    // non valide
                    {createConfig("127.0.0.1", -1), true},            // Invalid port (-1)
                    {createConfig("127.0.0.1", 65536), true},         // Invalid port (65536)
            });
        }

        private static ServerConfiguration createConfig(String advertisedAddress, int bookiePort) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setBookiePort(bookiePort);
            return conf;
        }

        @Test
        public void testGetBookieAddress() {
            try {
                BookieSocketAddress address = BookieImpl.getBookieAddress(conf);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieSocketAddress should not be null for valid configuration.", address);
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplGetCurrentDirectoriesTest {

        private static TmpDirs tmpDirs;
        private static File existingDir;
        private static File anotherExistingDir;

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
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid: array di dirs
                    {new File[]{existingDir, anotherExistingDir}, false},
                    // invalid: array vuoto
                    {new File[]{}, false},
                    // invalid: null
                    {null, true},
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
                    // valid
                    {createValidConfig(), createMockLedgerStorage(false), false},   // conf valida con ledgerStorage non inizializzato

                    // invalid
                    {null, createMockLedgerStorage(false), true},                  // conf null
                    {createInvalidConfig(), createMockLedgerStorage(false), true}, // conf invalida con ledgerStorage non inizializzato
                    {createValidConfig(), createMockLedgerStorage(true), true},    // conf valida con ledgerStorage già inizializzato
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
            conf.setDiskUsageThreshold(0.8f);
            conf.setDiskUsageWarnThreshold(0.9f); // Warn > Threshold => invalida
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

    @RunWith(Parameterized.class)
    public static class CreateMasterKeyEntryTest {

        private final long ledgerId;
        private final byte[] masterKey;
        private final boolean expectException;

        public CreateMasterKeyEntryTest(long ledgerId, byte[] masterKey, boolean expectException) {
            this.ledgerId = ledgerId;
            this.masterKey = masterKey;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {1L, "ValidMasterKey".getBytes(), false},    // ledgerID positivo, masterKey valida

                    //invalid
                    {1L, null, true},                            // masterKey null
                    //limite
                    {Long.MAX_VALUE, "MaxLedgerIdKey".getBytes(), false}, // ledgerID massimo, masterKey valida
            });
        }

        @Test
        public void testCreateMasterKeyEntry() {
            try {
                ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
                BookieImpl bookie = new TestBookieImpl(conf);

                ByteBuf result = bookie.createMasterKeyEntry(ledgerId, masterKey);

                if (expectException) {
                    fail("Expected an exception but none was thrown.");
                }
                assertNotNull("Resulting ByteBuf should not be null", result);
                assertEquals("LedgerId mismatch in the ByteBuf", ledgerId, result.getLong(0));
                assertEquals("MasterKey length mismatch in the ByteBuf", masterKey.length, result.getInt(16));
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
            }
        }
    }


    public static class ConstructorTest {

        private TmpDirs tmpDirs;

        @Before
        public void setup() {
            tmpDirs = new TmpDirs();
        }

        @After
        public void cleanup() throws Exception {
            if (tmpDirs != null) {
                tmpDirs.cleanup();
            }
        }

        @Test
        public void testBookieImplConstructor() throws Exception {
            // dir temp per journal, ledger e index
            File journalDir = tmpDirs.createNew("test-journal", ".tmp");
            File ledgerDir = tmpDirs.createNew("test-ledger", ".tmp");
            File indexDir = tmpDirs.createNew("test-index", ".tmp");

            // ServerConfiguration valido
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(journalDir.getAbsolutePath());
            conf.setLedgerDirNames(new String[]{ledgerDir.getAbsolutePath()});
            conf.setIndexDirName(new String[]{indexDir.getAbsolutePath()});
            conf.setAllowMultipleDirsUnderSameDiskPartition(false);

            try {
                BookieImpl bookie = new TestBookieImpl(conf);
                bookie.start();
                fail("Expected BookieException to be thrown, but none was thrown.");
            } catch (BookieException e) {
                System.out.println("BookieException caught as expected.");
                assertTrue(true);
            } catch (Exception e) {
                Assert.fail("Expected BookieException, but a different exception was thrown: " + e.getMessage());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class BookieImplAddEntryTest {

        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;
        private final boolean expectException;

        private BookieImpl bookie;

        public BookieImplAddEntryTest(ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb,
                                      Object ctx, byte[] masterKey, boolean expectException) {
            this.entry = entry;
            this.ackBeforeSync = ackBeforeSync;
            this.cb = cb;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Valid case
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), false},
                    // Invalid case: entry null
                    {null, true, null, null, "ValidMasterKey".getBytes(), true},
                    // Invalid case: entry senza metadata
                    {EntryBuilder.createInvalidEntryWithoutMetadata(), false, mockWriteCallback(), new Object(), "".getBytes(StandardCharsets.UTF_8), true},
            });
        }

        @Before
        public void setup() throws Exception {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            bookie = new TestBookieImpl(conf);
        }

        @Test
        public void addEntryTest() {
            try {
                bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
                }
            }
        }

        @Test
        public void recoveryAddEntryTest() {
            try {
                bookie.recoveryAddEntry(entry, cb, ctx, masterKey);
                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
                }
            }
        }

        @After
        public void teardown() throws Exception {
            bookie.shutdown();
        }

        private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
            return mock(BookkeeperInternalCallbacks.WriteCallback.class);
        }
    }












    

}
