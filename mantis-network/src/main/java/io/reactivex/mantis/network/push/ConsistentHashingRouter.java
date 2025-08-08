/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivex.mantis.network.push;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;


public class ConsistentHashingRouter<K, V> extends Router<KeyValuePair<K, V>> {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashingRouter.class);
    private static final int connectionRepetitionOnRing = 1000;
    private static long validCacheAgeMSec = 5000;
    private HashFunction hashFunction;
    private AtomicReference<SnapshotCache<SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>>>> cachedRingRef = new AtomicReference<>();
    private final Map<String, SortedMap<Long, AsyncConnection<KeyValuePair<K,V>>>> connectionObservables = new HashMap<>();
    private final String name;

    public ConsistentHashingRouter(String name,
                                   Func1<KeyValuePair<K, V>, byte[]> dataEncoder,
                                   HashFunction hashFunction) {
        super("ConsistentHashingRouter_" + name, dataEncoder);
        this.name = name;
        this.hashFunction = hashFunction;
    }

    @Override
    public void route(Set<AsyncConnection<KeyValuePair<K, V>>> connections,
                      List<KeyValuePair<K, V>> chunks) {
        if (connections != null && !connections.isEmpty() &&
                chunks != null && !chunks.isEmpty()) {

            // hash connections into slots
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring =
                    hashConnections(connections);

            routeChunks(chunks, ring);
        }
    }

    public void route(String id, List<KeyValuePair<K, V>> chunks) {
        if (Objects.isNull(chunks)
            || chunks.isEmpty()
            || !connectionObservables.containsKey(id)
            || connectionObservables.get(id).isEmpty()) {
            return;
        }
        routeChunks(chunks, connectionObservables.get(id));
    }

    public void subscribe(String id, Observable<AsyncConnectionChange<KeyValuePair<K, V>>> connectionObservable) {
        if (connectionObservables.containsKey(id)) {
            logger.warn("already subscribed to connection observable with id: {}", id);
            return;
        }
        SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring = new TreeMap<>();
        connectionObservables.put(id, ring);
        connectionObservable.subscribe(conn -> {
           switch (conn.getType()) {
               case ADD:
                   logger.info("adding connection: {} to router: {}", conn.getConnection().getId(), this.name);
                   addConnectionToRing(ring, conn.getConnection());
                   break;
               case REMOVE:
                   logger.info("removing connection: {} from router: {}", conn.getConnection().getId(), this.name);
                   removeConnectionFromRing(ring, conn.getConnection());
                   break;
           }
        }, (err) -> {
            logger.error("error on connection observable for id",  err);
            connectionObservables.remove(id);
        }, () -> {
            logger.info("connection observable completed for id: {}", id);
            connectionObservables.remove(id);
        });

    }

    private void routeChunks(List<KeyValuePair<K, V>> chunks, SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring) {
        int numConnections = ring.size() / connectionRepetitionOnRing;
        int bufferCapacity = (chunks.size() / numConnections) + 1;
        Map<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> writes = new HashMap<>(numConnections);
        // process chunks
        for (KeyValuePair<K, V> kvp : chunks) {
            long hash = kvp.getKeyBytesHashed();
            // lookup slot
            AsyncConnection<KeyValuePair<K, V>> connection = lookupConnection(hash, ring);
            // add to writes
            Func1<KeyValuePair<K, V>, Boolean> predicate = connection.getPredicate();
            if (predicate == null || predicate.call(kvp)) {
                List<byte[]> buffer = writes.get(connection);
                if (buffer == null) {
                    buffer = new ArrayList<>(bufferCapacity);
                    writes.put(connection, buffer);
                }
                buffer.add(encoder.call(kvp));
            }
        }

        // process writes
        if (!writes.isEmpty()) {
            for (Entry<AsyncConnection<KeyValuePair<K, V>>, List<byte[]>> entry : writes.entrySet()) {
                AsyncConnection<KeyValuePair<K, V>> connection = entry.getKey();
                List<byte[]> toWrite = entry.getValue();
                connection.write(toWrite);
                numEventsRouted.increment(toWrite.size());
            }
        }
    }

    private AsyncConnection<KeyValuePair<K, V>> lookupConnection(long hash, SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring) {
        if (!ring.containsKey(hash)) {
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private void computeRing(Set<AsyncConnection<KeyValuePair<K, V>>> connections) {
        SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring = new TreeMap<Long, AsyncConnection<KeyValuePair<K, V>>>();
        for (AsyncConnection<KeyValuePair<K, V>> connection : connections) {
            addConnectionToRing(ring, connection);
        }
        cachedRingRef.set(new SnapshotCache<>(ring));
    }

    private void addConnectionToRing(SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring, AsyncConnection<KeyValuePair<K,V>> connection) {
        for (int i = 0; i < connectionRepetitionOnRing; i++) {
            // hash node on ring
            String connectionId = connection.getSlotId();
            if (connectionId == null) {
                throw new IllegalStateException("Connection must specify an id for consistent hashing");
            }
            byte[] connectionBytes = (connectionId + "-" + i).getBytes();
            long hash = hashFunction.computeHash(connectionBytes);
            if (ring.containsKey(hash)) {
                logger.error("Hash collision when computing ring. {} hashed to a value already in the ring.", connectionId + "-" + i);
            }
            ring.put(hash, connection);
        }
    }

    private void removeConnectionFromRing(SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> ring, AsyncConnection<KeyValuePair<K, V>> connection) {
        for (int i = 0; i < connectionRepetitionOnRing; i++) {
            String connectionId = connection.getSlotId();
            if (connectionId == null) {
                throw new IllegalStateException("Connection must specify an id for consistent hashing");
            }
            byte[] connectionBytes = (connectionId + "-" + i).getBytes();
            long hash = hashFunction.computeHash(connectionBytes);
            ring.remove(hash);
        }
    }

    private SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> hashConnections(Set<AsyncConnection<KeyValuePair<K, V>>> connections) {

        SnapshotCache<SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>>> cache = cachedRingRef.get();

        if (cache == null) {
            logger.info("Recomputing ring due to null reference");
            computeRing(connections);
        } else {
            SortedMap<Long, AsyncConnection<KeyValuePair<K, V>>> cachedRing = cache.getCache();
            // determine if need to recompute cache
            if (cachedRing.size() != (connections.size() * connectionRepetitionOnRing)) {
                // number of connections not equal
                logger.info("Recomputing ring due to difference in number of connections ({}) versus cache size ({}).", connections.size() * connectionRepetitionOnRing, cachedRing.size());
                computeRing(connections);
            } else {
                // number of connections equal, check timestamp
                long timestamp = cache.getTimestamp();
                if (System.currentTimeMillis() - timestamp > validCacheAgeMSec) {
                    computeRing(connections);
                }
            }
        }
        return cachedRingRef.get().getCache();
    }
}
