package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class BookieITTest extends BookKeeperClusterTestCase {

    public BookieITTest(){
        super(3);
    }

    //test caso positivo
    @Test
    public void testReadSuccessWithQuorum() throws Exception {
        // quorum di scrittura di 3 e un quorum di lettura di 2
        LedgerHandle lh = bkc.createLedger(2, 2, 1, BookKeeper.DigestType.CRC32, "password".getBytes(StandardCharsets.UTF_8));

        String entry = "entryToReadSuccessfully";
        long entryId = lh.addEntry(entry.getBytes(StandardCharsets.UTF_8));

        servers.get(0).getServer().shutdown();

        try {
            String readEntry = new String(lh.read(entryId, entryId).getEntry(entryId).getEntryBytes(), StandardCharsets.UTF_8);
            assertEquals("L'entry letta dovrebbe corrispondere a quella scritta", entry, readEntry);
        } catch (BKException e) {
            fail("La lettura non dovrebbe fallire perché c'è un quorum disponibile");
        }
        lh.close();
    }

    @Test
    public void testWriteFailureWithoutQuorum() throws Exception {

        LedgerHandle lh = bkc.createLedger(3, 3, 2, BookKeeper.DigestType.CRC32, "password".getBytes(StandardCharsets.UTF_8));

        String entry = "entryThatFailsToWrite";

        servers.get(0).getServer().shutdown();
        servers.get(1).getServer().shutdown();

        try {

            lh.addEntry(entry.getBytes(StandardCharsets.UTF_8));
            fail("L'operazione di scrittura non dovrebbe riuscire senza quorum");
        } catch (BKException.BKNotEnoughBookiesException e) {
            assertTrue("L'operazione di scrittura fallisce come previsto", true);
        } finally {
            lh.close();
        }
    }






}
