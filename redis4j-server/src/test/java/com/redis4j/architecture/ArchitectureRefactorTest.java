package com.redis4j.architecture;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataStoreFactory;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.PartitionedMemoryStore;
import com.redis4j.storage.StorageType;
import com.redis4j.storage.snapshot.DataSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ArchitectureRefactorTest {

    @Test
    void exposesOnlySupportedStorageStrategies() {
        assertEquals(Arrays.asList(StorageType.MEMORY, StorageType.PARTITIONED),
                Arrays.asList(StorageType.values()));
        assertInstanceOf(MemoryStore.class, DataStoreFactory.create(StorageType.MEMORY));
        DataStore partitioned = DataStoreFactory.create(StorageType.PARTITIONED, 4);
        assertInstanceOf(PartitionedMemoryStore.class, partitioned);
        partitioned.close();
    }

    @Test
    void commandMetadataIsTheRegistrationSource() {
        try (DataStore store = new MemoryStore()) {
            var command = new CommandRegistry(store).find("GET");
            assertNotNull(command);
            assertEquals("GET", command.metadata().name());
            assertTrue(command.metadata().arity().accepts(1));
            assertFalse(command.metadata().arity().accepts(2));
            assertInstanceOf(CommandResponse.BulkString.class, command.execute(new String[]{"missing"}));
        }
    }

    @Test
    void snapshotIsStableAfterStoreMutation() {
        try (DataStore store = new MemoryStore()) {
            store.set("key", "before");
            store.rPush("list", "a", "b");
            DataSnapshot snapshot = store.createSnapshot();
            store.set("key", "after");
            store.rPush("list", "c");

            assertEquals("before", snapshot.entries().stream()
                    .filter(entry -> entry.key().equals("key"))
                    .findFirst().orElseThrow().value());
            assertEquals(2, ((java.util.List<?>) snapshot.entries().stream()
                    .filter(entry -> entry.key().equals("list"))
                    .findFirst().orElseThrow().value()).size());
        }
    }

    @Test
    void partitionedStorePreservesPersistentTtlAndExpiryDuringMutation() {
        try (DataStore store = new PartitionedMemoryStore(3)) {
            store.set("persistent", "1");
            assertEquals(-1, store.pttl("persistent"));

            store.setEx("expiring", "1", 30);
            long before = store.pttl("expiring");
            store.incr("expiring");
            long after = store.pttl("expiring");
            assertTrue(before > 0);
            assertTrue(after > 0 && after <= before);
        }
    }
}
