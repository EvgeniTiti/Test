package com.example.chat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

// JRedisJSON client
import com.redislabs.modules.rejson.JReJSON;
// JRediSearch client
import com.redislabs.client.rediSearch.Client; // The client for RediSearch
// Note: SearchOptions, SearchResult, Schema, Query are not directly used in RedisManager connection setup
// but would be used in the search methods. Keep them for when those methods are implemented.
// import com.redislabs.client.rediSearch.SearchOptions;
// import com.redislabs.client.rediSearch.SearchResult;
// import com.redislabs.client.rediSearch.Schema;
// import com.redislabs.client.rediSearch.query.Query;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Manages all interactions with the Redis server for the chat application.
 * This class encapsulates the setup and management of Redis connections and provides
 * methods to perform various Redis operations related to chat functionalities,
 * including core Redis commands, Redis Streams, RedisJSON, and RediSearch.
 * Connection details are loaded from a `config.properties` file.
 */
public class RedisManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RedisManager.class);

    // Default Redis connection details
    private static final String DEFAULT_REDIS_HOST = "localhost";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final String DEFAULT_REDIS_PASSWORD = null; // Or ""

    // Configuration properties
    private String redisHost;
    private int redisPort;
    private String redisPassword;


    // Lettuce Redis Client and Connection
    private RedisClient lettuceClient;
    private transient StatefulRedisConnection<String, String> lettuceConnection; // Marked transient if serialization is a concern
    private transient RedisCommands<String, String> syncCommands; // For general and stream commands

    // JRedisJSON Client (uses its own connection management, typically Jedis based)
    private transient JReJSON jsonClient;

    // JRediSearch Client (uses its own connection management, typically Jedis based)
    private transient Client searchClient; // com.redislabs.client.rediSearch.Client
    public static final String CHAT_MESSAGE_INDEX_NAME = "idx:chat_messages_java";


    /**
     * Constructs a RedisManager.
     * It loads Redis connection details from `config.properties` found in the classpath.
     * If the file or specific properties are not found, defaults are used.
     * After loading configuration, it initializes connections to Redis and its modules.
     */
    public RedisManager() {
        loadConfiguration();
        init();
    }

    /**
     * Loads Redis connection configuration from `config.properties` file.
     * Sets redisHost, redisPort, and redisPassword fields based on the file content
     * or defaults if the file/properties are not found or invalid.
     */
    private void loadConfiguration() {
        Properties props = new Properties();
        try (InputStream input = RedisManager.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                logger.warn("config.properties file not found in classpath. Using default Redis connection settings.");
                this.redisHost = DEFAULT_REDIS_HOST;
                this.redisPort = DEFAULT_REDIS_PORT;
                this.redisPassword = DEFAULT_REDIS_PASSWORD;
                return;
            }
            props.load(input);
            this.redisHost = props.getProperty("redis.host", DEFAULT_REDIS_HOST);
            // Parse port with error handling and fallback
            try {
                this.redisPort = Integer.parseInt(props.getProperty("redis.port", String.valueOf(DEFAULT_REDIS_PORT)));
            } catch (NumberFormatException e) {
                logger.warn("Invalid format for redis.port in config.properties. Using default port {}.", DEFAULT_REDIS_PORT, e);
                this.redisPort = DEFAULT_REDIS_PORT;
            }
            this.redisPassword = props.getProperty("redis.password", DEFAULT_REDIS_PASSWORD);
            if (this.redisPassword != null && this.redisPassword.isEmpty()) {
                this.redisPassword = null; // Treat empty password as no password
            }
            logger.info("Loaded Redis configuration: host='{}', port={}, password_provided={}",
                        this.redisHost, this.redisPort, (this.redisPassword != null && !this.redisPassword.isEmpty()));

        } catch (IOException e) {
            logger.warn("Error loading config.properties. Using default Redis connection settings.", e);
            this.redisHost = DEFAULT_REDIS_HOST;
            this.redisPort = DEFAULT_REDIS_PORT;
            this.redisPassword = DEFAULT_REDIS_PASSWORD;
        }
    }


    /**
     * Initializes the Redis client, connections, and module-specific clients
     * using the loaded configuration (host, port, password).
     * This method sets up:
     * 1. Lettuce client for core Redis operations and Streams.
     * 2. JRedisJSON client for interacting with the RedisJSON module.
     * 3. JRediSearch client for interacting with the RediSearch module.
     *
     * @throws RuntimeException if initialization of essential Redis connections fails.
     */
    private void init() {
        try {
            // 1. Initialize Lettuce Client and Connection
            logger.info("Connecting to Redis (Lettuce) at {}:{}...", this.redisHost, this.redisPort);
            RedisURI.Builder uriBuilder = RedisURI.builder().withHost(this.redisHost).withPort(this.redisPort);
            if (this.redisPassword != null && !this.redisPassword.isEmpty()) {
                uriBuilder.withPassword(this.redisPassword.toCharArray());
            }
            RedisURI redisURI = uriBuilder.build();

            this.lettuceClient = RedisClient.create(redisURI);
            this.lettuceConnection = this.lettuceClient.connect();
            this.syncCommands = this.lettuceConnection.sync(); // Synchronous commands
            logger.info("Lettuce connection to Redis established successfully.");

            // 2. Initialize JRedisJSON Client
            logger.info("Initializing JRedisJSON client for {}:{}...", this.redisHost, this.redisPort);
            // JReJSON constructor using host/port. If Redis requires auth, JReJSON might fail if it
            // doesn't inherit auth context or if the server is strict.
            // For password protected Redis, a JedisPool configured with password should be passed to JReJSON.
            // This is a simplification for now.
            this.jsonClient = new JReJSON(this.redisHost, this.redisPort);
            if (this.redisPassword != null && !this.redisPassword.isEmpty()) {
                 logger.warn("JRedisJSON client initialized with host/port. Password authentication might not be directly supported " +
                             "by this JReJSON constructor. Consider using a password-configured JedisPool with JReJSON if issues arise.");
            }
            logger.info("JRedisJSON client initialized successfully.");


            // 3. Initialize JRediSearch Client
            logger.info("Initializing JRediSearch client for {}:{} and index '{}'...", this.redisHost, this.redisPort, CHAT_MESSAGE_INDEX_NAME);
            // The JRediSearch Client constructor can take password.
            // Using default timeout (500ms) and no specific pool (null).
            this.searchClient = new Client(CHAT_MESSAGE_INDEX_NAME, this.redisHost, this.redisPort, 500, this.redisPassword);
            logger.info("JRediSearch client initialized successfully.");

        } catch (Exception e) {
            logger.error("Failed to initialize RedisManager and connect to Redis or its modules using config: host={}, port={}.",
                         this.redisHost, this.redisPort, e);
            // This exception is critical for the application's ability to function.
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
