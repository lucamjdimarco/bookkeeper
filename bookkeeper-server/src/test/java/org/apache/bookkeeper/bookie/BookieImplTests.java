package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.helper.DeleteTemp;
import org.apache.bookkeeper.helper.EntryBuilder;
import org.apache.bookkeeper.helper.EnumForDir;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TmpDirs;
import org.awaitility.Awaitility;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


import static org.apache.bookkeeper.helper.EnumForDir.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value = Enclosed.class)
public class BookieImplTests {

    private static final Logger LOG = LoggerFactory.getLogger(BookieImplTests.class);

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
        private File mockDir;

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

                    // Test Case 1: Directory esistente, nessun vecchio layout, passa
                    {true, false, true, true, null},
                    // Test Case 2: Directory non esistente, no vecchio layout, mkdirs() ha successo, passa
                    {false, false, true, true, null},
                    // Test Case 3: Directory non esistente, vecchio layout presente, mkdirs() ha successo, eccezione
                    {false, true, true, true, IOException.class},
                    //AGGIUNTA questa casistica per aumentare la copertura di BADUA
                    {false, false, false, true, IOException.class}
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
                mockDir = Mockito.mock(File.class);
                Mockito.when(mockDir.getParentFile()).thenReturn(parentDir.toFile());

                if (!mkdirsSuccess) {
                    Mockito.when(mockDir.exists()).thenReturn(false);
                    Mockito.when(mockDir.mkdirs()).thenReturn(false);
                } else {
                    Mockito.when(mockDir.exists()).thenReturn(false);
                    Mockito.when(mockDir.mkdirs()).thenReturn(true);
                }

                Mockito.when(mockDir.toPath()).thenReturn(parentDir.resolve("testDir"));
            }

            if (oldLayout) {
                Files.createFile(parentDir.resolve("file.txn"));
            }
        }

        @Test
        public void testCheckDirectoryStructure() {
            try {
                File dirToTest = (exists) ? testDir.toFile() : mockDir;

                BookieImpl.checkDirectoryStructure(dirToTest);

                if (expectedException != null) {
                    fail("Expected exception " + expectedException.getSimpleName() + " but none was thrown.");
                }
            } catch (Exception e) {
                if (expectedException == null) {
                    fail("Unexpected exception thrown: " + e);
                } else if (!expectedException.isInstance(e)) {
                    fail("Expected exception " + expectedException.getSimpleName() + " but got " + e.getClass().getSimpleName());
                }

                // **Assert specifico per il caso mkdirs fallito**
                if (!mkdirsSuccess && e instanceof IOException) {
                    assertEquals("Unable to create directory " + mockDir, e.getMessage());
                }
            }

            if (!mkdirsSuccess) {
                Mockito.verify(mockDir, Mockito.times(1)).mkdirs();
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
        private final boolean useMock;

        public BookieImplGetBookieIdTest(String customBookieId, boolean expectException, boolean useMock) {
            this.customBookieId = customBookieId;
            this.expectException = expectException;
            this.useMock = useMock;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Valid configuration
                    {"bookie-01.example.com:9000", false, false},

                    // Invalid configurations
                    {"127.0.0.1:3181!", true, false},                   // Invalid customBookieId
                    {"", true, false},                                   // Empty customBookieId
                    // aggiunta per aumentare coverage per BADUA --> continua a non vederli
                    {"readonly", true, true},
                    {null, false, false},                                 // Null customBookieId
            });
        }

        @Test
        public void testGetBookieId() {
            try {
                /*ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
                if (customBookieId != null) conf.setBookieId(customBookieId);

                BookieId bookieId = BookieImpl.getBookieId(conf);

                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);*/

                //uso del mock per conf invalida ma BADUA non lo vede
                ServerConfiguration conf;
                if (useMock) {
                    conf = mock(ServerConfiguration.class);
                    when(conf.getBookieId()).thenReturn(customBookieId);
                } else {
                    conf = TestBKConfiguration.newServerConfiguration();
                    if (customBookieId != null) conf.setBookieId(customBookieId);
                }

                assertEquals("CustomBookieId should match the configured value.",
                        customBookieId, conf.getBookieId());

                BookieId bookieId = BookieImpl.getBookieId(conf);

                if (expectException) {
                    fail("Expected an exception, but none was thrown.");
                }
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);
                if (customBookieId != null) {
                    assertEquals("BookieId should match customBookieId.",
                            BookieId.parse(customBookieId), bookieId);
                }

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

                    //Test modificati e aggiunti per aumentare la coverage di Badua
                    // ######################

                    {createConfigForShortName(null, null, 3181, true, true, true), false}, //AGGIUNTO per coprire getUseShortHostName gestito anche iface == null
                    {createConfigWithiface(null, "lo0", 3181, false), true},

                    // ######################
                    //gli altri 0 covered non li posso coprire perché sono metodi statici
                    //PROBLEMATICO
                    //{createConfigForHostNotResolv(null, "en0", -1, true, false, false), true}, //AGGIUNTO per coprire UnknownHostInterface
                    //{createConfig("     ", 3181), false}, //AGGIUNTO per PIT


            });
        }



        private static ServerConfiguration createConfig(String advertisedAddress, int bookiePort) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setBookiePort(bookiePort);
            return conf;
        }

        private static ServerConfiguration createConfigWithiface(String advertisedAddress, String iface, int bookiePort, boolean allowLoopback) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setBookiePort(bookiePort);
            conf.setAllowLoopback(allowLoopback);
            if (iface != null) {
                conf.setListeningInterface(iface);
            } else {
                conf.setListeningInterface(null);
            }
            return conf;
        }

        private static ServerConfiguration createConfigForShortName(String advertisedAddress, String iface, int bookiePort, boolean allowLoopback, boolean useHostName, boolean useShortHostName) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setBookiePort(bookiePort);
            conf.setAllowLoopback(allowLoopback);
            conf.setUseHostNameAsBookieID(useHostName);
            conf.setUseShortHostName(useShortHostName);
            if (iface != null) {
                conf.setListeningInterface(iface);
            } else {
                conf.setListeningInterface(null);
            }
            return conf;
        }

        private static ServerConfiguration createConfigForHostNotResolv(String advertisedAddress, String iface, int bookiePort, boolean allowLoopback, boolean useHostName, boolean useShortHostName) {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setAdvertisedAddress(advertisedAddress);
            conf.setBookiePort(bookiePort);
            conf.setAllowLoopback(allowLoopback);
            conf.setUseHostNameAsBookieID(useHostName);
            conf.setUseShortHostName(useShortHostName);
            if (iface != null) {
                conf.setListeningInterface(iface);
            } else {
                conf.setListeningInterface(null);
            }
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
            } catch (UnknownHostException e){ //2
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
                Assert.assertTrue("Expected exception thrown: " + e.getMessage(), expectException);
            }
            catch (Exception e) {
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

                    // Da eliminare ??
                    {createValidConfig(), null, false},
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
    public static class BookieImplExplicitLacIntegrationTest {

        private final long ledgerId;
        private final ByteBuf explicitLac;
        private final BookkeeperInternalCallbacks.WriteCallback writeCallback;
        private final Object ctx;
        private final byte[] masterKey;
        private final boolean expectException;

        private BookieImpl bookie;

        public BookieImplExplicitLacIntegrationTest(long ledgerId, ByteBuf explicitLac,
                                                    BookkeeperInternalCallbacks.WriteCallback writeCallback,
                                                    Object ctx, byte[] masterKey, boolean expectException) {
            this.ledgerId = ledgerId;
            this.explicitLac = explicitLac;
            this.writeCallback = writeCallback;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.expectException = expectException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid
                    {1L, EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), false},
                    // invalid
                    {1L, null, null, null, "".getBytes(), true},
                    {1L, EntryBuilder.createInvalidEntryWithoutMetadata(), null, new Object(), null, true},
                    {-1L, EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), true},
                    //aumentare badua (?)
                    //{2L, EntryBuilder.createValidEntry(), mockWriteCallback(), new Object(), "MasterKey".getBytes(), false},
            });
        }

        @Before
        public void setup() throws Exception {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            bookie = new TestBookieImpl(conf);
        }

        @Test
        public void testExplicitLacWorkflow() {
            try {
                if (ledgerId < 0) {
                    assertTrue("Negative ledgerId should result in exception.", expectException);
                    return;
                }

                // ExplicitLACEntry
                ByteBuf lacEntry = bookie.createExplicitLACEntry(ledgerId, explicitLac);
                assertNotNull("ExplicitLACEntry should not be null for valid inputs", lacEntry);

                // SetExplicitLac
                bookie.setExplicitLac(Unpooled.copiedBuffer(lacEntry), writeCallback, ctx, masterKey);
                assertTrue("ExplicitLACEntry should have been set.", lacEntry.readableBytes() > 0);

                byte[] content = new byte[lacEntry.readableBytes()];
                lacEntry.getBytes(lacEntry.readerIndex(), content);

                // GetExplicitLac
                ByteBuf retrievedLac = bookie.getExplicitLac(ledgerId);

                // assert --> LAC è lo stesso dell'originale
                assertEquals(new String(content, StandardCharsets.UTF_8),
                        new String(retrievedLac.array(), StandardCharsets.UTF_8));
                assertFalse(this.expectException);
            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getClass().getSimpleName() + " - " + e.getMessage());
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



    @RunWith(Parameterized.class)
    public static class BookieImplEntryIntegrationTest {

        private final ByteBuf entry;
        private final boolean ackBeforeSync;
        private final BookkeeperInternalCallbacks.WriteCallback cb;
        private final Object ctx;
        private final byte[] masterKey;

        private final Long testLedgerId;
        private final boolean useWatcher;
        private final boolean expectException;
        private final Class<? extends Exception> exceptionClass;

        private File dir;
        private final TmpDirs tmpDirs=new TmpDirs();

        private BookieImpl bookie;

        private static int counter = 0;

        public BookieImplEntryIntegrationTest(ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb,
                                              Object ctx, byte[] masterKey, Long testLedgerId,
                                              boolean useWatcher, boolean expectException, Class<? extends Exception> exceptionClass) {
            this.entry = entry;
            this.ackBeforeSync = ackBeforeSync;
            this.cb = cb;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.testLedgerId = testLedgerId;
            this.useWatcher = useWatcher;
            this.expectException = expectException;
            this.exceptionClass = exceptionClass;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid case
                    {EntryBuilder.createValidEntry(), true, mockWriteCallback(), new Object(), "ValidMasterKey".getBytes(), null, true, false, null},
                    // invalid case
                    {null, true, null, null, "ValidMasterKey".getBytes(), -1L, true, true, null},
                    {EntryBuilder.createInvalidEntryWithoutMetadata(), false, mockWriteCallback(), new Object(), "".getBytes(StandardCharsets.UTF_8), null, false, true, null},

                    //PIT
                    {null, false, null, new Object(), "ValidMasterKey".getBytes(), null, true, true, LedgerDirsManager.NoWritableLedgerDirException.class},
            });
        }

        @Before
        public void setup() throws Exception {
            this.dir = this.tmpDirs.createNew("bookieEntryTest", ".tmp");

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});

            bookie = new TestBookieImpl(conf);

            //this.bookie.statsLogger.getOpStatsLogger("").clear();
        }

        @Test
        public void addEntryAndReadEntryTest() {
            try {
                // addEntry
                bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);

                assertEquals((EntryBuilder.createValidEntry().readableBytes()), ((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumSuccessfulEvent());

                if (expectException && exceptionClass == null) {
                    fail("Expected an exception, but none was thrown.");
                }

                if (!expectException) {
                    long ledgerId = (testLedgerId != null) ? testLedgerId : EntryBuilder.extractLedgerId(entry);
                    long entryId = EntryBuilder.extractEntryId(entry);

                    ByteBuf result = bookie.readEntry(ledgerId, entryId);
                    assertNotNull("Resulting ByteBuf should not be null for valid inputs.", result);

                    if(EntryBuilder.isValidEntry(entry)) {
                        assertEquals(2*(EntryBuilder.createValidEntry().readableBytes()), ((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumSuccessfulEvent());
                    }

                    byte[] addedContent = new byte[entry.readableBytes()];
                    entry.getBytes(entry.readerIndex(), addedContent);

                    byte[] readContent = new byte[result.readableBytes()];
                    result.getBytes(result.readerIndex(), readContent);

                    String addedContentStr = new String(addedContent, StandardCharsets.UTF_8);
                    String readContentStr = new String(readContent, StandardCharsets.UTF_8);

                    assertEquals("The read content should match the added content.", addedContentStr, readContentStr);

                    // Test readLastAddConfirmed
                    if (testLedgerId == null) {
                        long lastAddConfirmed = bookie.readLastAddConfirmed(ledgerId);
                        assertEquals("LastAddConfirmed should be the same as the entry ID.", entryId, lastAddConfirmed);

                        // Test waitForLastAddConfirmedUpdate
                        AtomicBoolean watcherNotified = new AtomicBoolean(false);
                        //Watcher<LastAddConfirmedUpdateNotification> watcher = notification -> watcherNotified.set(true);
                        Watcher<LastAddConfirmedUpdateNotification> watcher = useWatcher
                                ? notification -> watcherNotified.set(true)
                                : null;

                        boolean updateResult = bookie.waitForLastAddConfirmedUpdate(ledgerId, lastAddConfirmed, watcher);
                        assertTrue("Watcher should detect an update.", updateResult);

                        // Test cancelWaitForLastAddConfirmedUpdate
                        bookie.cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
                        watcherNotified.set(false);

                        // try updating LAC and confirm watcher is not notified after cancellation
                        bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
                        assertFalse("Watcher should not be notified after cancellation.", watcherNotified.get());
                    } else {
                        // ledger ID is invalid
                        try {
                            bookie.readLastAddConfirmed(ledgerId);
                            fail("Expected exception for invalid ledger ID.");
                        } catch (IOException | BookieException e) {
                            assertTrue("Correct exception for invalid ledger ID.", true);
                        }

                        try {
                            bookie.waitForLastAddConfirmedUpdate(ledgerId, 0L, null);
                            fail("Expected exception for invalid ledger ID.");
                        } catch (IOException e) {
                            assertTrue("Correct exception for invalid ledger ID.", true);
                        }

                        try {
                            bookie.cancelWaitForLastAddConfirmedUpdate(ledgerId, null);
                            fail("Expected exception for invalid ledger ID.");
                        } catch (IOException e) {
                            assertTrue("Correct exception for invalid ledger ID.", true);
                        }
                    }



                }

            } catch (Exception e) {
                if(entry!=null){
                    if(EntryBuilder.isInvalidEntry(entry)){
                        assertEquals(EntryBuilder.createInvalidEntry().readableBytes(), ((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumFailedEvent());
                    } else if(EntryBuilder.isInvalidEntryWithoutMetadata(entry)){
                        assertEquals(EntryBuilder.createInvalidEntryWithoutMetadata().readableBytes(), ((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumFailedEvent());
                    }


                }
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
                }
            }
        }

        @Test
        public void recoveryAddEntryTest() {
            try {

                if(exceptionClass != null && expectException) {
                    BookieImpl spyBookie = spy(bookie);

                    if (exceptionClass.equals(LedgerDirsManager.NoWritableLedgerDirException.class)) {
                        doThrow(new LedgerDirsManager.NoWritableLedgerDirException("No writable ledger directory"))
                                .when(spyBookie).getLedgerForEntry(any(ByteBuf.class), any(byte[].class));
                    }

                    spyBookie.recoveryAddEntry(entry, cb, ctx, masterKey);

                    fail("Expected exception but none was thrown.");
                } else {

                    bookie.recoveryAddEntry(entry, cb, ctx, masterKey);
                    assertTrue("Recovery add entry completed without exceptions", true);
                    long ledgerId = EntryBuilder.extractLedgerId(entry);
                    long entryId = EntryBuilder.extractEntryId(entry);
                    ByteBuf result = bookie.readEntry(ledgerId, entryId);


                    assertNotNull("Recovered entry should be readable.", result);
                    assertEquals("Entry content should match.", entry, result);

                    assertTrue(((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumSuccessfulEvent() > 0);

                    if (expectException) {
                        fail("Expected an exception, but none was thrown.");
                    }
                }

            } catch (IOException e) {
                if (expectException) {
                    assertTrue("Correct exception for NoWritableLedgerDirException.",
                            e.getMessage().contains("No writable ledger directory"));

                }

            } catch (Exception e) {
                if (!expectException) {
                    fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
                }

            }
        }

        @After
        public void teardown() {
            try {
                tmpDirs.cleanup();
                bookie.shutdown();
            } catch (Exception e) {
                fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
            }
        }

        private static BookkeeperInternalCallbacks.WriteCallback mockWriteCallback() {
            return (rc, ledgerId, entryId, addr, ctx) -> {
                if (rc != 0) {
                    fail("WriteCallback failed with result code: " + rc);
                }
            };
        }
    }

    /*public static class BookieImplReadEntryFailureTest {

        private File dir;
        private final TmpDirs tmpDirs = new TmpDirs();
        private BookieImpl bookie;

        @Before
        public void setup() throws Exception {

            this.dir = this.tmpDirs.createNew("bookieEntryTest", ".tmp");

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalDirName(this.dir.toString());
            conf.setLedgerDirNames(new String[]{this.dir.getAbsolutePath()});

            bookie = new TestBookieImpl(conf);
        }

        @Test
        public void readEntryFailureTest() {
            try {
                long invalidLedgerId = -1L;
                long invalidEntryId = -1L;

                bookie.readEntry(invalidLedgerId, invalidEntryId);
                fail("Expected an exception for invalid ledgerId and entryId, but none was thrown.");
            } catch (IOException | BookieException e) {
                assertTrue("Correct exception for invalid ledgerId and entryId.", true);
                assertEquals(((TestStatsLogger.TestOpStatsLogger) this.bookie.statsLogger.getOpStatsLogger("")).getNumFailedEvent(), 0);
            } catch (Exception e) {
                fail("Unexpected exception thrown: " + e.getClass().getSimpleName());
            }
        }

        @After
        public void teardown() {
            try {
                tmpDirs.cleanup();
                bookie.shutdown();
            } catch (Exception e) {
                fail("Unexpected exception thrown during teardown: " + e.getClass().getSimpleName());
            }
        }
    }*/

    @RunWith(Parameterized.class)
    public static class OptimizedFormatTest {

        private final boolean isInteractive;
        private final boolean force;
        private final boolean expectedResult;
        private final EnumForDir journalDirs;

        private final EnumForDir ledgerDirs;

        private final EnumForDir indexDirs;

        private final EnumForDir gcEntryLogMetadataCachePath;
        private final String input;
        private final boolean expException;

        private ServerConfiguration conf;
        private static InputStream originalSystemIn;

        public OptimizedFormatTest(EnumForDir journalDirs, EnumForDir ledgerDirs, EnumForDir indexDirs, EnumForDir gcEntryLogMetadataCachePath,
                                   boolean isInteractive, boolean force, String input,
                                   boolean expectedResult, boolean expException) {
            this.isInteractive = isInteractive;
            this.force = force;
            this.expectedResult = expectedResult;
            this.journalDirs = journalDirs;
            this.ledgerDirs = ledgerDirs;
            this.indexDirs = indexDirs;
            this.gcEntryLogMetadataCachePath = gcEntryLogMetadataCachePath;
            this.input = input;
            this.expException = expException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesArgument() {
            return Arrays.asList(new Object[][]{

                    {EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, true, false, "Y", true, false},
                    //{EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, EXISTENT_WITH_FILE, true, false, "INVALID", false, true},
                    {EXISTENT_DIR_WITH_SUBDIR_AND_FILE, EXISTENT_WITH_FILE, EMPTY_ARRAY, NON_EXISTENT_DIRS, true, false, "N", false, false},
                    {EXISTENT_DIR_WITH_NON_REMOVABLE_FILE, EXISTENT_DIR_WITH_NON_REMOVABLE_FILE, EXISTENT_DIR_WITH_NON_REMOVABLE_FILE, EXISTENT_DIR_WITH_NON_REMOVABLE_FILE, false, true, "Y", false, false},
                    {EXISTENT_DIR_WITH_NON_REMOVABLE_EMPTY_SUBDIR, NON_EXISTENT_DIRS, EXISTENT_WITH_FILE, EMPTY_ARRAY, false, true, "Y", false, false},
                    {EMPTY_ARRAY, EXISTENT_DIR_WITH_NON_REMOVABLE_EMPTY_SUBDIR, EXISTENT_DIR_WITH_NON_REMOVABLE_EMPTY_SUBDIR, EXISTENT_DIR_WITH_NON_REMOVABLE_EMPTY_SUBDIR, true, false, "Y", false, false},
                    {EMPTY_ARRAY, EMPTY_ARRAY, EMPTY_ARRAY, EMPTY_ARRAY, true, true, "Y", true, false},
                    {NULL, NULL, NULL, EMPTY_ARRAY, false, false, "Y", false, true},
            });
        }

        @Before
        public void setup() throws Exception {
            conf = mock(ServerConfiguration.class);

            File[] journalDirsArray = journalDirs.getDirectories();
            File[] ledgerDirsArray = ledgerDirs.getDirectories();
            File[] indexDirsArray = indexDirs.getDirectories();
            String gcPath = gcEntryLogMetadataCachePath.getGcEntryLogMetadataCachePath();

            when(conf.getJournalDirs()).thenReturn(journalDirsArray);
            when(conf.getLedgerDirs()).thenReturn(ledgerDirsArray);
            when(conf.getIndexDirs()).thenReturn(indexDirsArray);
            when(conf.getGcEntryLogMetadataCachePath()).thenReturn(gcPath);
        }

        @After
        public void cleanup() {
            DeleteTemp.deleteFiles(conf.getJournalDirs(), conf.getIndexDirs(), conf.getLedgerDirs());
            System.setIn(originalSystemIn);
        }

        @Test
        public void formatTest() {
            try {
                originalSystemIn = System.in;

                switch (this.input) {
                    case "Y":
                        System.setIn(new ByteArrayInputStream("Y\nY\nY\n".getBytes()));
                        break;
                    case "N":
                        System.setIn(new ByteArrayInputStream("N\n".getBytes()));
                        break;
                    /*case "INVALID":
                        InputStream failingStream = mock(InputStream.class);
                        when(failingStream.read()).thenThrow(new IOException("Simulated IOException"));
                        System.setIn(failingStream);
                        break;*/
                    default:
                        throw new IllegalArgumentException("Invalid input case: " + this.input);
                }

                /*assertNotNull("Journal dirs should not be null", conf.getJournalDirs());
                assertTrue("Journal dirs should not be empty", conf.getJournalDirs().length > 0);
                for (File dir : conf.getJournalDirs()) {
                    assertNotNull("Each journal dir should not be null", dir);
                }*/



                Assert.assertEquals(expectedResult, BookieImpl.format(conf, isInteractive, force));

                //boolean result = BookieImpl.format(conf, isInteractive, force);

                //assertEquals("Result mismatch", this.expectedResult, result);

                // Controllo sulle directory dei journal
                File[] journalDirs = conf.getJournalDirs();
                if (journalDirs != null && expectedResult) {
                    for (File dir : journalDirs) {
                        assertNotNull("Journal directory should not be null", dir);
                        assertTrue("Journal directory should exist after formatting", dir.exists());
                        assertTrue("Journal directory should be writable after formatting", dir.canWrite());
                        assertTrue("Journal directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                // Controllo sulle directory dei ledger
                File[] ledgerDirs = conf.getLedgerDirs();
                if (ledgerDirs != null && expectedResult) {
                    for (File dir : ledgerDirs) {
                        assertNotNull("Ledger directory should not be null", dir);
                        assertTrue("Ledger directory should exist after formatting", dir.exists());
                        assertTrue("Ledger directory should be writable after formatting", dir.canWrite());
                        assertTrue("Ledger directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                // Controllo sulle directory degli index
                File[] indexDirs = conf.getIndexDirs();
                if (indexDirs != null && expectedResult) {
                    for (File dir : indexDirs) {
                        assertNotNull("Index directory should not be null", dir);
                        assertTrue("Index directory should exist after formatting", dir.exists());
                        assertTrue("Index directory should be writable after formatting", dir.canWrite());
                        assertTrue("Index directory should be empty after formatting", dir.list().length == 0);
                    }
                }

                // Controllo sulla directory di garbage collection
                /*String gcPath = conf.getGcEntryLogMetadataCachePath();
                if (gcPath != null && expectedResult) {
                    File gcDir = new File(gcPath);
                    assertTrue("GC directory should exist after formatting", gcDir.exists());
                    //assertTrue("GC directory should be writable after formatting", gcDir.canWrite());
                    //assertTrue("GC directory should be empty after formatting", gcDir.list().length == 0);
                }*/



                assertFalse("Exception was not expected", this.expException);

                /*assertEquals("Result mismatch", this.expectedResult, result);
                assertEquals(this.expectedResult, result);
                assertFalse("Exception was not expected", this.expException);
                System.out.println(result);*/
            } catch (NullPointerException e) {
                if (!expException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
                assertTrue("Exception 1", this.expException);
            } catch (Exception e) {
                if (!expException) {
                    fail("Unexpected exception thrown: " + e.getMessage());
                }
                assertTrue("Exception 2", this.expException);
            }
        }




    }






}
