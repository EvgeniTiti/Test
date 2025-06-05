package com.example.chat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the Java Redis Chat Application.
 * This class contains the main method to initialize and start the application,
 * primarily by setting up the {@link RedisManager} which handles all Redis interactions.
 */
public class ChatApp {

    private static final Logger logger = LoggerFactory.getLogger(ChatApp.class);

    private static final String REDIS_HOST = "localhost"; // Default Redis host
    private static final int REDIS_PORT = 6379;          // Default Redis port

    /**
     * The main entry point for the Java Redis Chat Application.
     * <p>
     * This method initializes the {@link RedisManager} to establish Redis connections
     * and then would typically proceed to start application logic (e.g., UI, REST API, message listeners).
     * It ensures that Redis connections are closed gracefully on application exit.
     * </p>
     *
     * @param args Command line arguments (not currently used by this application).
     */
    public static void main(String[] args) {
        logger.info("Starting Java Redis Chat Application...");

        RedisManager redisManager = null;
        try {
            // Initialize RedisManager, which attempts to connect to Redis
            // and sets up clients for core commands, JSON, and Search.
            logger.info("Initializing RedisManager to connect to Redis at {}:{}...", REDIS_HOST, REDIS_PORT);
            redisManager = new RedisManager(REDIS_HOST, REDIS_PORT);
            logger.info("RedisManager initialized successfully.");

            // --------------------------------------------------------------------
            // Placeholder for application logic
            // --------------------------------------------------------------------
            // At this point, the application would start its primary tasks:
            // - Start UI (if any)
            // - Start message listeners/subscribers (e.g., using redisManager.subscribeToStream)
            // - Expose APIs for sending messages
            // - Handle user input for chat interactions or search queries
            //
            // Example (conceptual - these methods are currently placeholders in RedisManager):
            // if (redisManager != null) {
            //     // Simulate some operations (these would be driven by user actions or events)
            //     redisManager.createChatMessageIndex(); // Ensure search index is ready
            //     redisManager.publishToStream("group_chat:general", "user123", "Hello everyone from ChatApp!");
            //     // In a real app, subscription would run in a separate thread:
            //     // new Thread(() -> redisManager.subscribeToStream("group_chat:general", msg -> System.out.println("Received: " + msg))).start();
            // }
            // For this subtask, we are only setting up the structure.
            // Actual calls to RedisManager methods will be implemented in later subtasks.
            // --------------------------------------------------------------------

            logger.info("Java Redis Chat Application initialized and ready (placeholder functionality).");
            // Keep the main thread alive for a bit if we were running subscribers, or wait for user input.
            // For now, it will just proceed to the finally block and close.
            // System.out.println("Press Enter to exit...");
            // new java.util.Scanner(System.in).nextLine();


        } catch (Exception e) {
            // This catch block handles exceptions during RedisManager initialization (e.g., connection failure)
            // or any other critical startup errors.
            logger.error("Critical error during application startup: {}", e.getMessage(), e);
            // Depending on the severity, the application might choose to exit here.
        } finally {
            // Ensure RedisManager and its connections are closed when the application exits.
            if (redisManager != null) {
                logger.info("Shutting down RedisManager and closing connections...");
                redisManager.close(); // Calls the close() method in RedisManager
                logger.info("RedisManager shut down complete.");
            }
        }
        logger.info("Java Redis Chat Application finished.");
    }
}
