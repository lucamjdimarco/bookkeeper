package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

@RunWith(value = Enclosed.class)
public class LedgerHandleTests {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerHandleTests.class);

    @RunWith(Parameterized.class)
    public static class ReadEntriesTest extends BookKeeperClusterTestCase {

        private final long firstEntry;
        private final long lastEntry;
        private final boolean isValid;

        private LedgerHandle ledgerHandle;
        private BookKeeper bookKeeper;


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
                    {0, 10, true},
                    //invalid
                    {-1, 10, false},
                    {0, -1, false},
                    {10, 5, false},
                    {-1, -5, false},
            });
        }

        @Before
        public void setUp() throws Exception {

            super.setUp("/ledgers");

            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            for (int i = 0; i < 20; i++) {
                String entry = "Entry " + i;
                ledgerHandle.addEntry(entry.getBytes());
            }
        }

        @Test
        public void testReadEntriesMultidimensional() {
            try {

                Enumeration<LedgerEntry> result = ledgerHandle.readEntries(firstEntry, lastEntry);

                if (isValid) {
                    Assert.assertNotNull("Expected a valid result, but got null", result);
                    Assert.assertTrue("Expected at least one entry", result.hasMoreElements());
                } else {
                    Assert.fail("Expected an exception for invalid input, but the method executed successfully");
                }
            } catch (Exception e) {
                if (isValid) {
                    Assert.fail("Did not expect an exception for valid input: " + e.getMessage());
                } else {
                    Assert.assertTrue("Exception message should indicate invalid input",
                            e instanceof BKException || e instanceof IllegalArgumentException);
                }
            }
        }

        @After
        public void tearDown(){
            try {
                ledgerHandle.close();
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class ReadUnconfirmedEntriesTest extends BookKeeperClusterTestCase {

        private final long firstEntry;
        private final long lastEntry;
        private final boolean isValid;

        private LedgerHandle ledgerHandle;

        public ReadUnconfirmedEntriesTest(long firstEntry, long lastEntry, boolean isValid) {
            super(3);
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.isValid = isValid;
        }

        @Parameterized.Parameters(name = "Test case: firstEntry={0}, lastEntry={1}, isValid={2}")
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    //valid
                    {0, 10, true},   // confermate
                    {5, 15, true},   // entry non confermate
                    //invalid
                    {-1, 10, false},
                    {0, -1, false},
                    {10, 5, false},
                    {-1, -5, false},
            });
        }

        @Before
        @Override
        public void setUp() throws Exception {
            super.setUp("/ledgers");

            ledgerHandle = bkc.createLedger(BookKeeper.DigestType.CRC32, "passwd".getBytes());

            //20 confermate
            for (int i = 0; i < 20; i++) {
                String entry = "Confirmed Entry " + i;
                ledgerHandle.addEntry(entry.getBytes());
            }

            // 5 non confermate
            ledgerHandle.setLastAddConfirmed(19);
            for (int i = 20; i < 25; i++) {
                String entry = "Unconfirmed Entry " + i;
                ledgerHandle.asyncAddEntry(entry.getBytes(), null, null);
            }
            long lastEntry = ledgerHandle.getLastAddConfirmed();
            System.out.println("Last entry: " + lastEntry);
        }

        @Test
        public void testReadUnconfirmedEntriesMultidimensional() {
            try {
                ledgerHandle.setLastAddConfirmed(19);
                Enumeration<LedgerEntry> result = ledgerHandle.readUnconfirmedEntries(firstEntry, lastEntry);

                long lastEntry = ledgerHandle.getLastAddConfirmed();
                System.out.println("Last entry: " + lastEntry);

                if (isValid) {
                    Assert.assertNotNull("Expected a valid result, but got null", result);


                    long currentId = firstEntry;
                    while (result.hasMoreElements()) {
                        LedgerEntry entry = result.nextElement();
                        Assert.assertEquals("Mismatch in entry ID", currentId, entry.getEntryId());
                        String expectedContent = currentId < 20
                                ? "Confirmed Entry " + currentId
                                : "Unconfirmed Entry " + currentId;
                        Assert.assertEquals("Mismatch in entry content", expectedContent, new String(entry.getEntry()));
                        currentId++;
                    }
                    Assert.assertEquals("Mismatch in the number of entries read", lastEntry + 1, currentId);
                } else {
                    Assert.fail("Expected an exception for invalid input, but the method executed successfully");
                }
            } catch (Exception e) {
                if (isValid) {
                    Assert.fail("Did not expect an exception for valid input: " + e.getMessage());
                } else {
                    Assert.assertTrue("Exception message should indicate invalid input: " + e.getMessage(),
                            e instanceof BKException || e instanceof IllegalArgumentException);
                }
            }
        }

        @After
        @Override
        public void tearDown() {
            try {
                if (ledgerHandle != null) {
                    ledgerHandle.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    super.tearDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



}
