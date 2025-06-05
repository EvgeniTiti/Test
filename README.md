# Python and Java Chat Application using Redis

## Overview

This project demonstrates a chat application built with both Python and Java, leveraging various Redis features for messaging, data storage, and search capabilities. The goal is to showcase how Redis can be effectively used as a backend for real-time communication systems.

Both applications (Python and Java) aim to provide functionalities for:
*   1-to-1 conversations
*   Group conversations
*   Sending and receiving messages
*   Storing chat history
*   Searching chat history

## Technologies Used

*   **Backend Languages:**
    *   Python
    *   Java
*   **Redis Features:**
    *   **Redis Streams:** For handling real-time message queuing and broadcasting.
    *   **RedisJSON:** For storing chat messages in a structured JSON format, allowing for flexible data manipulation.
    *   **RediSearch:** For creating secondary indexes on chat messages, enabling powerful search and query capabilities.
    *   **Redis Sets:** For managing user presence in chat rooms and group memberships.
*   **Redis Clients:**
    *   **Python:** `redis-py` (with optional `hiredis` for performance)
    *   **Java:** `Lettuce` (for core Redis operations and Streams), `JRedisJSON` (for RedisJSON), `JRediSearch` (for RediSearch)
*   **Build Tools:**
    *   **Java:** Apache Maven

## Repository Structure

```
.
├── java_chat/          # Java Chat Application
│   ├── pom.xml
│   └── src/
├── python_chat/        # Python Chat Application
│   ├── app.py
│   ├── redis_utils.py
│   └── requirements.txt
└── README.md           # This file
```

## Modules

This project is divided into two main modules:

1.  **Python Chat Application (`python_chat/`)**: A command-line based chat application written in Python.
2.  **Java Chat Application (`java_chat/`)**: A command-line based chat application written in Java.

(Further details on setup and usage for each application will be provided below.)
---

## Redis Connection Configuration

To connect to your Redis instance, both the Python and Java applications use external configuration files. This allows you to specify your Redis server's host, port, and password without hardcoding them into the source code. Example configuration files are provided, which you should copy and customize. The actual configuration files containing your local details are ignored by Git.

### Python Application (`python_chat/`)

*   An example configuration file is provided at `python_chat/config.example.ini`.
*   **Action:** Copy `python_chat/config.example.ini` to `python_chat/config.ini`.
*   Edit `python_chat/config.ini` with your Redis server details.
    ```ini
    [Redis]
    host = your_redis_host
    port = your_redis_port
    password = your_redis_password_if_any
    ```
*   The `config.ini` file is listed in `python_chat/.gitignore` and should not be committed to version control.

### Java Application (`java_chat/`)

*   An example configuration file is provided at `java_chat/src/main/resources/config.example.properties`.
*   **Action:** Copy `java_chat/src/main/resources/config.example.properties` to `java_chat/src/main/resources/config.properties`.
*   Edit `java_chat/src/main/resources/config.properties` with your Redis server details.
    ```properties
    redis.host=your_redis_host
    redis.port=your_redis_port
    redis.password=your_redis_password_if_any
    ```
*   The `config.properties` file (when placed in `src/main/resources/`) is listed in `java_chat/.gitignore` and should not be committed to version control.

**Important:** Always ensure your local `config.ini` and `config.properties` files containing potentially sensitive information are not committed to your repository.

---

## Python Chat Application (`python_chat/`)

This section details how to set up and run the Python chat application.

### Prerequisites

*   **Python:** Version 3.7+ is recommended.
*   **Redis Server:**
    *   Redis version 6.x or higher.
    *   Ensure the **RedisJSON** and **RediSearch** modules are installed and loaded on your Redis server.
    *   You can typically check loaded modules with the Redis command `MODULE LIST`.
    *   Configure connection details as described in the "Redis Connection Configuration" section.


### Setup & Installation

1.  **Navigate to the Python application directory:**
    ```bash
    cd python_chat
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    ```

3.  **Activate the virtual environment:**
    *   On macOS and Linux:
        ```bash
        source venv/bin/activate
        ```
    *   On Windows:
        ```bash
        .\venv\Scripts\activate
        ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    The `requirements.txt` file includes `redis` (redis-py client) and `hiredis` (optional C parser for performance).

### Running the Application

The Python application (`app.py`) demonstrates various flows such as user management, sending messages, receiving messages, and searching. The `main()` function in `app.py` is typically configured to run one specific flow at a time for demonstration purposes.

**General Steps:**

1.  **Ensure your Redis server is running** and accessible, and that you have created and configured `python_chat/config.ini` as per the "Redis Connection Configuration" section.
2.  **Open the `python_chat/app.py` file.**
3.  **Modify the `main()` function** to call the specific flow you want to test (e.g., `send_chat_message_flow_differentiated()`, `receive_chat_messages_flow_differentiated()`, `search_messages_flow()`).
    *   For example, to run the message receiving flow:
        ```python
        if __name__ == "__main__":
            # ... (redis_conn setup)
            # manage_user_flow(redis_conn)
            # send_chat_message_flow_differentiated(redis_conn)
            receive_chat_messages_flow_differentiated(redis_conn) # Focus on this
            # manage_group_chat_membership_flow(redis_conn)
            # search_messages_flow(redis_conn)
            # ... (cleanup)
        ```
4.  **Run the application from the `python_chat` directory:**
    ```bash
    python app.py
    ```

**Testing Specific Features:**

*   **Sending and Receiving Messages:**
    *   To test sending and receiving, you'll typically need two terminal instances.
    *   In one terminal, configure `app.py` to run `receive_chat_messages_flow_differentiated()` for a specific chat (e.g., 1-to-1 `chat:userA_userB` or group `group_chat:mygroup`).
    *   In the second terminal, configure `app.py` to run `send_chat_message_flow_differentiated()` to send a message to that *same* chat stream.
    *   The receiving terminal should then display the message.
*   **Searching Messages:**
    *   First, ensure some messages have been sent and stored (which also populates RedisJSON).
    *   Configure `app.py` to run `search_messages_flow()`.
    *   When prompted, enter search queries like `@user_id:some_user_id` or `some_keyword_in_message`.
*   **User and Group Management:**
    *   Configure `app.py` to run `manage_user_flow()` or `manage_group_chat_membership_flow()`.
    *   You can observe changes in Redis using `redis-cli` (e.g., `SMEMBERS users:general_lobby` or `SMEMBERS group_members:your_group_name`).

For more detailed testing scenarios, refer to the comprehensive manual testing guide provided in earlier interactions or embedded within code comments.
---

## Java Chat Application (`java_chat/`)

This section details how to set up and run the Java chat application.
**Note:** The Java application is currently under development. The instructions below cover building and running its current basic structure. Feature parity with the Python application will be added incrementally.

### Prerequisites

*   **Java Development Kit (JDK):** Version 11 or 17 is recommended.
*   **Apache Maven:** For building the project.
*   **Redis Server:**
    *   Redis version 6.x or higher.
    *   Ensure the **RedisJSON** and **RediSearch** modules are installed and loaded on your Redis server.
    *   You can typically check loaded modules with the Redis command `MODULE LIST`.
    *   Configure connection details as described in the "Redis Connection Configuration" section by creating `java_chat/src/main/resources/config.properties`.

### Setup & Building

1.  **Navigate to the Java application directory:**
    ```bash
    cd java_chat
    ```

2.  **Clean and build the project using Maven:**
    ```bash
    mvn clean package
    ```
    This command will compile the source code, run any tests, and package the application into a JAR file (e.g., `target/java_chat-1.0-SNAPSHOT.jar`).

### Running the Application

1.  **Ensure your Redis server is running** and accessible, and that you have created and configured `java_chat/src/main/resources/config.properties` as per the "Redis Connection Configuration" section.

2.  **Run the application using the `exec:java` Maven plugin** (or by directly executing the JAR):
    *   **Using Maven:**
        ```bash
        mvn exec:java -Dexec.mainClass="com.example.chat.ChatApp"
        ```
    *   **Alternatively, running the JAR directly** (after `mvn clean package`):
        ```bash
        java -jar target/java_chat-1.0-SNAPSHOT.jar
        ```
        (Ensure the JAR is executable and the main class is correctly specified in `pom.xml`'s `maven-jar-plugin` if you encounter issues with the latter method).

Currently, running `ChatApp` will initialize the `RedisManager`, attempt to connect to Redis using details from `config.properties` (or defaults if the file is missing/invalid), and then cleanly shut down. As more features (like message sending/receiving flows) are implemented in `ChatApp.java` and `RedisManager.java`, this section will be updated with more specific operational instructions.
---

## Redis Data Structures Utilized

This project leverages several Redis data structures and modules to build its chat functionalities:

*   **Redis Streams:**
    *   **Purpose:** Used as the primary mechanism for real-time message passing and history. When a user sends a message to a 1-to-1 chat or a group chat, the message is added to a specific Redis Stream.
    *   **How it works:** Each chat (1-to-1 or group) typically corresponds to a unique stream. Producers (message senders) use `XADD` to append new messages. Consumers (message receivers) use `XREAD` (often in a blocking manner) to listen for new messages on one or more streams. Streams provide message persistence and allow multiple consumers.

*   **RedisJSON:**
    *   **Purpose:** Used to store the detailed content of each chat message in a structured JSON format. This allows for flexible message attributes and easier querying if direct JSON manipulation is needed.
    *   **How it works:** After a message is published to a Stream and receives a Stream ID, its content (sender, timestamp, message text, etc.) is stored as a JSON document in a Redis key. The key name typically incorporates the chat ID and the message's Stream ID for easy retrieval (e.g., `chat_message:<chat_id>:<message_id>`). Commands like `JSON.SET` are used to store data.

*   **RediSearch:**
    *   **Purpose:** Provides powerful secondary indexing and full-text search capabilities over the chat messages stored in RedisJSON.
    *   **How it works:** A search index is created over the JSON documents (e.g., `idx:chat_messages`). This index can include fields like the sender's ID, the message text, and timestamps. Users can then perform complex queries (e.g., find all messages from a specific user containing certain keywords) using `FT.SEARCH`.

*   **Redis Sets:**
    *   **Purpose:** Used for managing collections of unique elements, primarily for:
        *   Tracking users currently active in a conceptual "chat room" or "lobby."
        *   Managing membership lists for group chats.
    *   **How it works:** User IDs are added to a Redis Set corresponding to a specific room or group using `SADD`. `SREM` removes users, and `SMEMBERS` retrieves all users in a room/group. This provides an efficient way to manage presence and group associations.
