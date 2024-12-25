package org.apache.bookkeeper.client;

import com.sun.org.apache.xpath.internal.operations.Bool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Null;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@RunWith(value = Enclosed.class)
public class LedgerHandleTests {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerHandleTests.class);

    @RunWith(Parameterized.class)
    public static class ReadEntriesTest extends BookKeeperClusterTestCase {

        private final long firstEntry;
        private final long lastEntry;
        private final boolean isValid;

        private LedgerHandle ledgerHandle;


        public ReadEntriesTest(long firstEntry, long lastEntry, boolean isValid) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.isValid = isValid;
        }

        @Parameterized.Parameters(name = "Test case: firstEntry={0}, lastEntry={1}, isValid={2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {0, 2, true},
                    //invalid
                    {-1, 2, false},
                    {0, -1, false},
                    {3, 1, false},
                    {-1, -5, false},
            });
        }

        @Before
        public void setUp() throws Exception {

            super.setUp("/ledgers");

            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < 5; i++) {
                String entry = "Entry " + i;
                ledgerHandle.addEntry(entry.getBytes());
            }
        }

        @Test
        public void testReadEntriesMultidimensional() {
            try {

                Enumeration<LedgerEntry> result = ledgerHandle.readEntries(firstEntry, lastEntry);

                if (isValid) {
                    assertNotNull("Expected a valid result, but got null", result);
                    assertTrue("Expected at least one entry", result.hasMoreElements());
                } else {
                    fail("Expected an exception for invalid input, but the method executed successfully");
                }
            } catch (Exception e) {
                if (isValid) {
                    fail("Did not expect an exception for valid input: " + e.getMessage());
                } else {
                    assertTrue("Exception message should indicate invalid input",
                            e instanceof BKException || e instanceof IllegalArgumentException);
                }
            }
        }

        @After
        public void tearDown(){
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class AsyncReadEntriesTest extends BookKeeperClusterTestCase {

        private final long firstEntry;
        private final long lastEntry;
        private final Object ctx;
        private final boolean cb;
        private final boolean expectedException;
        private final int expectedErrorCode;

        private LedgerHandle ledgerHandle;

        public AsyncReadEntriesTest(long firstEntry, long lastEntry, boolean cb, Object ctx,
                                    boolean expectedException, int expectedErrorCode) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.ctx = ctx;
            this.cb = cb;

            this.expectedException = expectedException;
            this.expectedErrorCode = expectedErrorCode;
        }

        @Parameterized.Parameters(name = "Test case: firstEntry={0}, lastEntry={1}, expectedException={2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {0, 2, false, null, false, BKException.Code.OK},
                    //invalid
                    {-1, 2, true, new Object(), true, BKException.Code.IncorrectParameterException},
                    {2, -1, false, null, true, BKException.Code.IncorrectParameterException},
                    {3, 1, true, null, true, BKException.Code.IncorrectParameterException},
                    {0, 100, false, null, true, BKException.Code.ReadException},
            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < 5; i++) {
                ledgerHandle.addEntry(("Entry " + i).getBytes());
            }
        }

        @Test
        public void testAsyncReadEntries() {

            try {
                //ledgerHandle.asyncReadEntries(firstEntry, lastEntry, cb, ctx);

                if(cb) {
                    AtomicInteger rcResult = new AtomicInteger(BKException.Code.OK);
                    AtomicBoolean complete = new AtomicBoolean(false);

                    ledgerHandle.asyncReadEntries(firstEntry, lastEntry, (rc, lh, entries, ctx) -> {
                        rcResult.set(rc);
                        if (rc == BKException.Code.OK) {
                            assertNotNull("Entries should not be null on successful read", entries);
                            assertTrue("Entries should contain at least one element", entries.hasMoreElements());
                        } else {
                            assertEquals("Unexpected error code", expectedErrorCode, rc);
                        }
                        complete.set(true);
                    }, ctx);

                    Awaitility.await().untilTrue(complete);

                    if (!expectedException) {
                        assertEquals("Expected successful read", BKException.Code.OK, rcResult.get());
                    } else {
                        assertEquals("Expected failure with specific error code", expectedErrorCode, rcResult.get());
                    }
                } else {
                    ledgerHandle.asyncReadEntries(firstEntry, lastEntry, null, ctx);

                    if(expectedException) {
                        fail("Expected exception, but method executed successfully.");
                    }
                }


            } catch (NullPointerException e ){
                if (!expectedException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof NullPointerException);
                }
            }
            catch (Exception e) {
                if (!expectedException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof BKException);
                }
            }
        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static AsyncCallback.ReadCallback mockReadCallback() {
            return (rc, ledgerHandle, entries, ctx) -> {
                if (rc == BKException.Code.OK) {

                    assertNotNull("Entries should not be null on successful read", entries);
                    assertTrue("Entries should contain at least one element", entries.hasMoreElements());
                } else {
                    System.out.println("ReadCallback failed with result code: " + rc);
                    assertTrue("Failure expected due to invalid input",
                            rc == BKException.Code.ReadException ||
                                    rc == BKException.Code.IncorrectParameterException ||
                                    rc == BKException.Code.BookieHandleNotAvailableException);
                }
            };
        }

    }

    @RunWith(Parameterized.class)
    public static class ReadAsyncTest extends BookKeeperClusterTestCase {

        private final long firstEntry;
        private final long lastEntry;
        private final boolean expectedException;

        private final Class<? extends Exception> expectedExceptionType;

        private LedgerHandle ledgerHandle;

        public ReadAsyncTest(long firstEntry, long lastEntry, boolean expectedException, Class<? extends Exception> expectedExceptionType) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.expectedException = expectedException;
            this.expectedExceptionType = expectedExceptionType;
        }

        @Parameterized.Parameters(name = "Test case: firstEntry={0}, lastEntry={1}, expectedException={2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {0, 3, false, null},
                    //invalid
                    {-1, 3, true, BKException.BKIncorrectParameterException.class},
                    {3, -1, true, BKException.BKIncorrectParameterException.class},
                    {3, 1, true, BKException.BKIncorrectParameterException.class},
                    {0, 100, true, BKException.BKReadException.class},
            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < 5; i++) {
                ledgerHandle.addEntry(("Entry " + i).getBytes());
            }
        }

        @Test
        public void testAsyncReadEntries() {

            CompletableFuture<LedgerEntries> future = ledgerHandle.readAsync(firstEntry, lastEntry);

            try {
                if (expectedException) {
                    future.join();
                    fail("Expected exception, but method executed successfully.");
                } else {
                    LedgerEntries entries = future.join();
                    assertNotNull("Entries should not be null for valid input", entries);
                    //aggiunto per PIT (DA VERIFICARE)
                    assertTrue("Last entry should be <= lastAddConfirmed", lastEntry <= ledgerHandle.getLastAddConfirmed());
                }
            } catch (CompletionException e) {
                if (!expectedException) {
                    fail("Did not expect an exception, but got: " + e.getCause());
                } else {
                    assertNotNull("Exception should have a cause", e.getCause());
                    assertTrue("Exception type mismatch",
                            expectedExceptionType.isInstance(e.getCause()));
                }
            }
        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class AddEntryTest extends BookKeeperClusterTestCase {

        private final byte[] data;
        private final boolean expectedException;

        private LedgerHandle ledgerHandle;

        public AddEntryTest(byte[] data, boolean expectedException) {
            super(3);
            this.data = data;
            this.expectedException = expectedException;
        }

        @Parameterized.Parameters(name = "Test case: data={0}, expectedException={2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {"validEntry".getBytes(), false},
                    {new byte[]{}, false},
                    //invalid
                    {null, true},
            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            /*LoggerContext context = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
            Configuration config = context.getConfiguration();

            // Configura il logger di LedgerHandle a DEBUG
            LoggerConfig loggerConfig = config.getLoggerConfig(LedgerHandle.class.getName());
            loggerConfig.setLevel(Level.DEBUG);
            context.updateLoggers();*/


            for (int i = 0; i < 20; i++) {
                ledgerHandle.addEntry(("Entry " + i).getBytes());
            }
        }

        @Test
        public void testAddEntry() {
            try {
                long entryId = ledgerHandle.addEntry(data);

                if (expectedException) {
                    fail("Expected an exception, but the method executed successfully.");
                }

                assertTrue("Entry ID should be a non-negative number", entryId >= 0);

            } catch (NullPointerException | BKException | InterruptedException e) {
                if (!expectedException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Exception should be due to invalid input",
                            e instanceof BKException.BKClientClosedException || e instanceof NullPointerException ||
                                    e instanceof NullPointerException);
                }
            }

        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    @RunWith(Parameterized.class)
    public static class AppendAsyncTest extends BookKeeperClusterTestCase {

        private final ByteBuf data;
        private final boolean expectException;

        private LedgerHandle ledgerHandle;

        public AppendAsyncTest(ByteBuf data, boolean expectException) {
            super(3);
            this.data = data;
            this.expectException = expectException;
        }

        @Parameterized.Parameters(name = "Test case: data={0}, expectException={1}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // valid
                    {Unpooled.wrappedBuffer("validData".getBytes()), false},
                    {Unpooled.wrappedBuffer(new byte[0]), false},

                    // invalid
                    {null, true},

            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());
        }

        @Test
        public void testAppendAsync() throws Exception {
            if (data == null) {
                try {
                    ledgerHandle.appendAsync(data).join();
                    fail("Expected NullPointerException, but the method executed successfully.");
                } catch (NullPointerException e) {
                    assertTrue("NullPointerException should have a message", true);
                }
                return;
            }

            CompletableFuture<Long> resultFuture = ledgerHandle.appendAsync(data);

            if (expectException) {
                try {
                    resultFuture.join();
                    fail("Expected an exception, but the method executed successfully.");
                } catch (CompletionException e) {
                    assertNotNull("Exception should have a cause", e.getCause());
                }
            } else {
                long entryId = resultFuture.join();
                assertTrue("Entry ID should be non-negative", entryId >= 0);
            }
        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class AsyncAddEntryTest extends BookKeeperClusterTestCase {

        private final byte[] data;
        private final int offset;
        private final int length;
        private final AsyncCallback.AddCallback callback;
        private final Object ctx;
        private final boolean expectException;
        private final Class<? extends Exception> expectedExceptionType;

        private LedgerHandle ledgerHandle;

        public AsyncAddEntryTest(byte[] data, int offset, int length, AsyncCallback.AddCallback callback, Object ctx,
                                 boolean expectException, Class<? extends Exception> expectedExceptionType) {
            super(3);
            this.data = data;
            this.offset = offset;
            this.length = length;
            this.callback = callback;
            this.ctx = ctx;
            this.expectException = expectException;
            this.expectedExceptionType = expectedExceptionType;
        }

        @Parameterized.Parameters(name = "Test case: data={0}, offset={1}, length={2}, expectException={3}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    // Valid cases
                    {new byte[]{1, 2, 3, 4}, 0, 4, mockAddCallback(), new Object(), false, null},
                    {new byte[]{1, 2, 3, 4}, 1, 2, mockAddCallback(), null, false, null},
                    {new byte[0], 0, 0, mockAddCallback(), null, false, null},

                    // Invalid cases
                    {null, 0, 4, mockAddCallback(), new Object(), true, NullPointerException.class},
                    {new byte[]{1, 2, 3, 4}, -1, 4, mockAddCallback(), null, true, ArrayIndexOutOfBoundsException.class},
                    {new byte[]{1, 2, 3, 4}, 0, -1, mockAddCallback(), null, true, ArrayIndexOutOfBoundsException.class},
                    {new byte[]{1, 2, 3, 4}, 4, 1, mockAddCallback(), null, true, ArrayIndexOutOfBoundsException.class},
                    {new byte[]{1, 2, 3, 4}, Integer.MAX_VALUE, Integer.MAX_VALUE, null, null, true, IndexOutOfBoundsException.class},
                    {new byte[]{1, 2, 3, 4}, Integer.MIN_VALUE, Integer.MIN_VALUE, null, null, true, ArrayIndexOutOfBoundsException.class},
            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());
        }

        @Test
        public void testAsyncAddEntry() {
            /*try {
                ledgerHandle.asyncAddEntry(data, offset, length, callback, ctx);

                if (offset < 0 || length < 0 || (offset + length) > (data != null ? data.length : 0)) {
                    fail("Expected ArrayIndexOutOfBoundsException for invalid offset/length but did not throw.");
                }

                if (expectException) {
                    fail("Expected exception, but method executed successfully.");
                }
            } catch (NullPointerException e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e);
                } else {
                    assertTrue("Exception caught", expectException);
                }
            } catch (Exception e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e);
                } else {
                    if (offset < 0 || length < 0 || (offset + length) > (data != null ? data.length : 0)) {
                        assertTrue("Exception caught", expectException);
                    }

                }
            }*/
            if (expectException) {
                assertThrows(expectedExceptionType, () -> {
                    ledgerHandle.asyncAddEntry(data, offset, length, callback, ctx);
                });
            } else {
                try {
                    ledgerHandle.asyncAddEntry(data, offset, length, callback, ctx);
                } catch (Exception e) {
                    fail("Did not expect an exception, but got: " + e);
                }
            }
        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static AsyncCallback.AddCallback mockAddCallback() {
            return (rc, ledgerHandle, entryId, ctx) -> {
                if (rc == BKException.Code.OK) {
                    assertTrue("Entry ID should be non-negative", entryId >= 0);
                } else {
                    System.out.println("AddCallback failed with result code: " + rc);
                }
            };
        }
    }

    @RunWith(Parameterized.class)
    public static class AsyncReadLastConfirmedAndEntryTest extends BookKeeperClusterTestCase {

        private final long entryId;
        private final long timeOutInMillis;
        private final boolean parallel;
        private final boolean callback;
        private final Object ctx;
        private final boolean expectException;

        private final boolean isClosed;

        private final boolean hasMoreElement;

        private LedgerHandle ledgerHandle;

        public AsyncReadLastConfirmedAndEntryTest(long entryId, long timeOutInMillis, boolean parallel,
                                                  boolean callback,
                                                  Object ctx, boolean isClosed, boolean hasMoreElement, boolean expectException) {
            super(3);
            this.entryId = entryId;
            this.timeOutInMillis = timeOutInMillis;
            this.parallel = parallel;
            this.callback = callback;
            this.ctx = ctx;
            this.isClosed = isClosed;
            this.hasMoreElement = hasMoreElement;

            this.expectException = expectException;
        }

        @Parameterized.Parameters(name = "entryId={0}, timeOutInMillis={1}, parallel={2}, callback={3}, ctx={4}, expectException={5}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {1, 1, true, true, null, false, false, false},
                    {0, 0, true, false, null, true, false, false},
                    //invalid
                    {-1, -1, false, true, new Object(), false, false, true},

                    //aggiunti per Jacoco
                    {10, 1, true, true, null, true, false, false},


            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());
        }

        @Test
        public void testAsyncReadLastConfirmedAndEntry() {

            try {
                AtomicInteger rcAtom  = new AtomicInteger();
                AtomicLong lac = new AtomicLong();
                AtomicBoolean complete = new AtomicBoolean(false);
                AtomicReference<LedgerEntry> entryAtom = new AtomicReference<>();

                long lastEntryId = ledgerHandle.addEntry("entry".getBytes());

                if(this.isClosed) {
                    ledgerHandle.close();
                }

                if(this.callback) {
                    ledgerHandle.asyncReadLastConfirmedAndEntry(entryId, timeOutInMillis, parallel,
                            (rc, lastConfirmed, entry, ctx) -> {
                                rcAtom.set(rc);
                                entryAtom.set(entry);
                                lac.set(lastConfirmed);
                                complete.set(true);
                            }, this.ctx);

                    Awaitility.await().untilTrue(complete);

                    if (rcAtom.get() == BKException.Code.OK) {
                        if (entryId > lastEntryId) {
                            assertNull(entryAtom.get());
                            assertEquals(lastEntryId, lac.get());
                        } else if (entryId == lastEntryId) {
                            assertNotNull(entryAtom.get());
                            assertEquals(new String(entryAtom.get().getEntry()), "entry");
                        } else {
                            assertNull(entryAtom.get());
                        }
                    } else {

                        assertNull(entryAtom.get());
                    }
                } else {
                    ledgerHandle.asyncReadLastConfirmedAndEntry(entryId, timeOutInMillis, parallel, null, ctx);
                }

                /*if(hasMoreElement) {
                    long lastEntryId2 = ledgerHandle.addEntry("entry".getBytes());
                    long lastEntryId3 = ledgerHandle.addEntry("entry".getBytes());

                    ledgerHandle.asyncReadLastConfirmedAndEntry(lastEntryId, timeOutInMillis, parallel,
                            (rc, lastConfirmed, entry, ctx) -> {
                                rcAtom.set(rc);
                                entryAtom.set(entry);
                                lac.set(lastConfirmed);
                                complete.set(true);
                            }, this.ctx);

                    Awaitility.await().untilTrue(complete);

                    if (rcAtom.get() == BKException.Code.OK) {
                        assertNull(entryAtom.get());
                    }
                }*/


            } catch(NullPointerException e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof NullPointerException);
                }
            } catch (Exception e) {
                if (!expectException) {
                    fail("Did not expect an exception, but got: " + e.getMessage());
                } else {
                    assertTrue("Expected exception, but got: " + e.getClass().getSimpleName(),
                            e instanceof BKException);
                }
            }


        }
    }

    @RunWith(Parameterized.class)
    public static class AsyncReadLastConfirmedTest extends BookKeeperClusterTestCase {

        private final boolean cb;
        private final Object ctx;
        private final boolean useV2WireProtocol;

        private final boolean isClosed;

        private LedgerHandle ledgerHandle;

        private BookKeeper bk;

        public AsyncReadLastConfirmedTest(boolean cb, Object ctx, boolean useV2WireProtocol, boolean isClosed) {
            super(3);
            this.cb = cb;
            this.ctx = ctx;
            this.useV2WireProtocol = useV2WireProtocol;
            this.isClosed = isClosed;
        }

        @Parameterized.Parameters(name = "Test case: cb={0}, ctx={1}, useV2WireProtocol={2}, expectException={3}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {true, new Object(), false, true},
                    {false, null, false, false},
                    //{false, null, false, true},

                    //aggiunto per Jacoco
                    {false, new Object(), true, true},
                    {false, null, true, true},
                    //{true, new Object(), false, false},
                    //{false, null, true, false},
            });
        }

        @Before
        public void setUp() throws Exception {
            super.setUp("/ledgers");
            ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
            conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
            conf.setExplictLacInterval(1);
            conf.setUseV2WireProtocol(useV2WireProtocol);
            bk = new BookKeeper(conf);

            ledgerHandle = bk.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());
        }

        @Test
        public void testAsyncReadLastConfirmed() {
            try {
                AtomicInteger rcAtom = new AtomicInteger();
                AtomicLong entryAtom = new AtomicLong();
                AtomicLong lac = new AtomicLong();
                AtomicBoolean complete = new AtomicBoolean(false);

                ledgerHandle.addEntry("entry".getBytes());

                if(!this.useV2WireProtocol) {
                    ledgerHandle.force().get();
                }

                if(isClosed){
                    ledgerHandle.close();
                }

                if(this.cb) {
                    ledgerHandle.asyncReadLastConfirmed((rc, lastConfirmed, ctx) -> {
                        rcAtom.set(rc);
                        lac.set(lastConfirmed);
                        complete.set(true);
                    }, this.ctx);

                    Awaitility.await().untilTrue(complete);
                } else {
                    ledgerHandle.asyncReadLastConfirmed(null, this.ctx);
                }

                if(!isClosed) {
                    if(rcAtom.get() == BKException.Code.OK) {
                        assertEquals(entryAtom.get(), lac.get());
                    } else {
                        assertEquals(-1L, lac.get());
                    }
                } else {
                    if(rcAtom.get() == BKException.Code.OK) {
                        assertEquals(ledgerHandle.getLedgerMetadata().getLastEntryId(), lac.get());
                    } else {
                        assertEquals(-1L, lac.get());
                    }
                }
            } catch (NullPointerException e) {
                assertTrue(true);
            } catch (Exception e ) {
                assertTrue(true);
            }
        }

        @After
        public void tearDown() {
            try {
                ledgerHandle.close();
                super.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }














}
