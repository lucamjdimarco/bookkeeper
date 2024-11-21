package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Enclosed.class)
public class BookieImplTests {




    @RunWith(Parameterized.class)
    public static class TestCheckDirectoryStructure {
        private final boolean exists;
        private final boolean canRead;
        private final boolean canWrite;
        private final boolean isDirectory;
        private final boolean oldLayout; // file vecchio layout
        private final boolean mkdirsSuccess;
        private final boolean expectedException;

        private Path testDir;

        public TestCheckDirectoryStructure(boolean exists, boolean canRead, boolean canWrite, boolean isDirectory, boolean oldLayout,
                                           boolean mkdirsSuccess, boolean expectedException) {
            this.exists = exists;
            this.canRead = canRead;
            this.canWrite = canWrite;
            this.isDirectory = isDirectory;
            this.oldLayout = oldLayout;
            this.mkdirsSuccess = mkdirsSuccess;
            this.expectedException = expectedException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {true, true, true, true, false, true, false},
                    {false, false, false, true, true, false, true},
                    {false, true, true, true, false, true, false},
            });
        }

        @Before
        public void setUp() throws IOException {
            if (exists) {
                testDir = Files.createTempDirectory("testDir");
                if (!canRead) testDir.toFile().setReadable(false);
                if (!canWrite) testDir.toFile().setWritable(false);
                if (oldLayout) {
                    Files.createFile(testDir.resolve("file.txn"));
                }
            } else if (mkdirsSuccess) {
                testDir = Paths.get("nonExistentDir");
            }
        }

        @Test
        public void testCheckDirectoryStructure() {
            try {
                File dir = testDir != null ? testDir.toFile() : new File("nonExistentDir");
                BookieImpl.checkDirectoryStructure(dir);
                if (expectedException) {
                    fail("Expected IOException but none was thrown.");
                }
            } catch (IOException e) {
                if (!expectedException) {
                    fail("Unexpected IOException thrown: " + e.getMessage());
                }
            }
        }

        @After
        public void tearDown() throws IOException {
            if (testDir != null && Files.exists(testDir)) {
                Files.walk(testDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
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
                    {createValidConfig(3181, "127.0.0.1", "eth0", 8080), false},
                    {createInvalidConfig(-1, "127.0.0.1", "eth0", 8080), true},
                    {createInvalidConfig(3181, "", "eth0", 8080), true},
                    {createInvalidConfig(3181, "127.0.0.1", null, 8080), true},
                    {createInvalidConfig(3181, "127.0.0.1", "eth0", -1), true},
                    {null, true},
            });
        }

        private static ServerConfiguration createValidConfig(int bookiePort, String advertisedAddress, String listeningInterface, int httpPort) {
            ServerConfiguration config = new ServerConfiguration();
            config.setBookiePort(bookiePort);
            config.setAdvertisedAddress(advertisedAddress);
            config.setListeningInterface(listeningInterface);
            config.setHttpServerPort(httpPort);
            return config;
        }

        private static ServerConfiguration createInvalidConfig(int bookiePort, String advertisedAddress, String listeningInterface, int httpPort) {
            return createValidConfig(bookiePort, advertisedAddress, listeningInterface, httpPort);
        }

        @Test
        public void testGetBookieId() {
            try {

                BookieId bookieId = BookieImpl.getBookieId(conf);
                if (expectException) {
                    fail("Expected an UnknownHostException, but none was thrown.");
                }
                assertNotNull("BookieId should not be null for valid configuration.", bookieId);
            } catch (UnknownHostException e) {
                if (!expectException) {
                    fail("Unexpected UnknownHostException thrown: " + e.getMessage());
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




}
