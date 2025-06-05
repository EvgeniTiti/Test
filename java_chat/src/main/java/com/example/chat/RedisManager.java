package com.example.chat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

// JRedisJSON client
import com.redislabs.modules.rejson.JReJSON;
// JRediSearch client
import com.redislabs.client.rediSearch.Client; // The client for RediSearch
import com.redislabs.client.rediSearch.SearchOptions;
import com.redislabs.client.rediSearch.SearchResult;
import com.redislabs.client.rediSearch.Schema;
import com.redislabs.client.rediSearch.query.Query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Manages all interactions with the Redis server for the chat application.
 * This class encapsulates the setup and management of Redis connections and provides
 * methods to perform various Redis operations related to chat functionalities,
 * including core Redis commands, Redis Streams, RedisJSON, and RediSearch.
 */
public class RedisManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RedisManager.class);

    private final String redisHost;
    private final int redisPort;

    // Lettuce Redis Client and Connection
    private RedisClient lettuceClient;
    private StatefulRedisConnection<String, String> lettuceConnection;
    private RedisCommands<String, String> syncCommands; // For general and stream commands

    // JRedisJSON Client (uses its own connection management, typically Jedis based)
    private JReJSON jsonClient;

    // JRediSearch Client (uses its own connection management, typically Jedis based)
    private Client searchClient; // com.redislabs.client.rediSearch.Client
    public static final String CHAT_MESSAGE_INDEX_NAME = "idx:chat_messages_java";


    /**
     * Constructs a RedisManager and initializes connections to Redis.
     *
     * @param host The hostname or IP address of the Redis server.
     * @param port The port number of the Redis server.
     */
    public RedisManager(String host, int port) {
        this.redisHost = host;
        this.redisPort = port;
        init();
    }

    /**
     * Initializes the Redis client, connections, and module-specific clients.
     * This method sets up:
     * 1. Lettuce client for core Redis operations and Streams.
     * 2. JRedisJSON client for interacting with the RedisJSON module.
     * 3. JRediSearch client for interacting with the RediSearch module.
     */
    private void init() {
        try {
            // 1. Initialize Lettuce Client and Connection
            logger.info("Connecting to Redis (Lettuce) at {}:{}...", redisHost, redisPort);
            RedisURI redisURI = RedisURI.builder().withHost(redisHost).withPort(redisPort).build();
            this.lettuceClient = RedisClient.create(redisURI);
            this.lettuceConnection = this.lettuceClient.connect();
            this.syncCommands = this.lettuceConnection.sync(); // Synchronous commands
            logger.info("Lettuce connection to Redis established successfully.");

            // 2. Initialize JRedisJSON Client
            // JReJSON typically uses JedisPool for connection.
            logger.info("Initializing JRedisJSON client for {}:{}...", redisHost, redisPort);
            this.jsonClient = new JReJSON(redisHost, redisPort);
            logger.info("JRedisJSON client initialized successfully.");


            // 3. Initialize JRediSearch Client
            // The `Client` from com.redislabs.client.rediSearch takes index name, host, port.
            logger.info("Initializing JRediSearch client for {}:{} and index '{}'...", redisHost, redisPort, CHAT_MESSAGE_INDEX_NAME);
            // Note: The index name is passed at client creation for JRediSearch, implying this client instance
            // might be tied to a specific index, or it's a default. The methods later also take index name.
            // We will use a default index name here and ensure it's created by createChatMessageIndex.
            this.searchClient = new Client(CHAT_MESSAGE_INDEX_NAME, redisHost, redisPort);
            logger.info("JRediSearch client initialized successfully.");

        } catch (Exception e) {
            logger.error("Failed to initialize RedisManager and connect to Redis or its modules.", e);
            // Depending on the application's needs, this could throw a runtime exception
            // to prevent the app from starting in a dysfunctional state.
            throw new RuntimeException("Could not initialize RedisManager: " + e.getMessage(), e);
        }
    }

    /**
     * Publishes a message to a Redis Stream.
     *
     * @param streamName      The name of the Redis Stream.
     * @param userId          The ID of the user sending the message.
     * @param messageContent  The content of the message.
     * @return The message ID if successful, null otherwise.
     */
    public String publishToStream(String streamName, String userId, String messageContent) {
        logger.debug("Placeholder: publishToStream called for stream '{}'", streamName);
        // To be implemented using this.syncCommands.xadd(...)
        return null;
    }

    /**
     * Subscribes to a Redis Stream and processes messages using a callback.
     *
     * @param streamName      The name of the Redis Stream.
     * @param messageConsumer A {@link Consumer} to process received messages.
     *                        Each message is a {@code Map<String, String>}.
     */
    public void subscribeToStream(String streamName, Consumer<Map<String, String>> messageConsumer) {
        logger.debug("Placeholder: subscribeToStream called for stream '{}'", streamName);
        // To be implemented using this.syncCommands.xreadgroup(...) or a dedicated subscription connection from Lettuce.
        // This will likely require asynchronous handling or a separate thread for blocking.
    }

    /**
     * Adds a chat message to RedisJSON for persistent storage.
     *
     * @param chatId      An identifier for the chat session (e.g., stream name).
     * @param messageId   The unique ID of the message (often from the stream).
     * @param messageData A map containing the message details (e.g., user_id, message, timestamp).
     * @return True if successful, false otherwise.
     */
    public boolean addChatMessageRedisJson(String chatId, String messageId, Map<String, String> messageData) {
        logger.debug("Placeholder: addChatMessageRedisJson called for chat '{}', messageId '{}'", chatId, messageId);
        // To be implemented using this.jsonClient.set(...)
        return false;
    }

    /**
     * Creates the RediSearch index for chat messages if it doesn't already exist.
     * @return True if index created or already exists, false on error.
     */
    public boolean createChatMessageIndex() {
        logger.debug("Placeholder: createChatMessageIndex called");
        // To be implemented using this.searchClient.createIndex(...)
        return false;
    }

    /**
     * Searches chat messages using RediSearch.
     *
     * @param query The RediSearch query string.
     * @return A list of search results (e.g., List<Map<String, Object>>), or an empty list.
     */
    public List<Map<String, Object>> searchChatMessages(String query) {
        logger.debug("Placeholder: searchChatMessages called with query '{}'", query);
        // To be implemented using this.searchClient.search(...)
        return List.of();
    }

    // --- User and Group Management Placeholders ---

    public void addUserToChatRoom(String roomName, String userId) {
        logger.debug("Placeholder: addUserToChatRoom: room '{}', user '{}'", roomName, userId);
    }

    public void removeUserFromChatRoom(String roomName, String userId) {
        logger.debug("Placeholder: removeUserFromChatRoom: room '{}', user '{}'", roomName, userId);
    }

    public Set<String> getUsersInChatRoom(String roomName) {
        logger.debug("Placeholder: getUsersInChatRoom: room '{}'", roomName);
        return Set.of();
    }

    public void joinGroupChat(String groupName, String userId) {
        logger.debug("Placeholder: joinGroupChat: group '{}', user '{}'", groupName, userId);
    }

    public void leaveGroupChat(String groupName, String userId) {
        logger.debug("Placeholder: leaveGroupChat: group '{}', user '{}'", groupName, userId);
    }

    public Set<String> getGroupChatMembers(String groupName) {
        logger.debug("Placeholder: getGroupChatMembers: group '{}'", groupName);
        return Set.of();
    }


    /**
     * Closes all Redis connections and shuts down the client.
     * This method should be called when the application is shutting down
     * to release resources properly. It implements {@link AutoCloseable#close()}.
     */
    @Override
    public void close() {
        logger.info("Closing Redis connections and shutting down clients...");
        try {
            if (this.lettuceConnection != null && this.lettuceConnection.isOpen()) {
                this.lettuceConnection.close();
                logger.debug("Lettuce connection closed.");
            }
        } catch (Exception e) {
            logger.warn("Error closing Lettuce connection.", e);
        }
        try {
            if (this.lettuceClient != null) {
                this.lettuceClient.shutdown();
                logger.debug("Lettuce client shut down.");
            }
        } catch (Exception e) {
            logger.warn("Error shutting down Lettuce client.", e);
        }

        // JRedisJSON and JRediSearch clients (like com.redislabs.client.rediSearch.Client)
        // often use Jedis pools internally. Check their documentation for specific close/shutdown methods.
        // For JedisPool based clients, pool.destroy() or pool.close() is common.
        // Assuming JReJSON and Client might have close methods or manage resources via underlying JedisPools.
        // For JReJSON, it doesn't seem to have an explicit close(), relies on underlying Jedis.
        // For JRediSearch Client, it takes a Pool<Jedis> or host/port. If it manages a pool, it should be closed.
        // If host/port constructor is used, it might create connections per command.
        // For simplicity, as specific pool management isn't shown in Client constructor:
        logger.debug("JRedisJSON and JRediSearch clients typically manage their own connections (e.g., via JedisPools)."
                   + " Ensure these are handled if manually created pools are passed to them.");
        // If JReJSON or SearchClient were initialized with a JedisPool instance that RedisManager owns,
        // then `jedisPool.destroy()` would be called here. Since they are initialized with host/port,
        // their internal connection handling will take over. Some versions might offer a close() method.
        // e.g. if this.searchClient had a close method: this.searchClient.close();

        logger.info("RedisManager resources cleanup process finished.");
    }
}
