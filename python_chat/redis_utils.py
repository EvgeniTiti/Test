"""
Utility functions for interacting with Redis.

This module provides a collection of functions to manage interactions with a Redis
server for a chat application. It includes functionalities for:
- Connecting to Redis.
- Publishing messages to Redis Streams for real-time communication.
- Subscribing to Redis Streams to receive messages.
- Generating standardized stream names for 1-to-1 and group chats.
- Storing chat messages persistently using RedisJSON.
- Managing group chat memberships using Redis Sets.
- Creating and querying a RediSearch index for chat messages.

It assumes that the Redis server has the RedisJSON and RediSearch modules installed
and enabled for functionalities that depend on them.
"""

# Standard library imports
import datetime
import configparser
import os # For path handling

# Third-party imports
try:
    import redis
    from redis.commands.search.field import TagField, TextField
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType
    from redis.commands.search.query import Query
except ImportError:
    # This allows the module to be imported and basic functions to be recognized
    # even if redis-py or its search components are not installed,
    # though functions relying on them will fail at runtime.
    print("redis-py or its search components are not installed. "
          "Please install it using 'pip install redis hiredis'. "
          "Full functionality, especially RediSearch, might require 'redis[hiredis,json,search]'.")
    redis = None # Make redis None to allow conditional checks in functions.

# --- Connection Management ---

# Define the expected path for the configuration file, relative to this script file.
# Assumes config.ini is in the same directory as redis_utils.py (which is python_chat/)
CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.ini')


def connect_to_redis(db=0):
    """
    Connects to a Redis instance and verifies the connection.
    Connection details (host, port, password) are read from 'config.ini' located
    in the same directory as this script. If the file or specific settings are
    not found, it falls back to default values (localhost:6379, no password).

    Args:
        db (int, optional): The Redis database number to connect to. Defaults to 0.

    Returns:
        redis.Redis or None: A Redis connection object if the connection is successful,
                             None otherwise.
    """
    if not redis:
        print("[ERROR] Redis client (redis-py) is not available. Cannot connect.")
        return None

    config = configparser.ConfigParser()
    host = 'localhost'
    port = 6379
    password = None

    try:
        # Attempt to read the configuration file.
        # read() returns a list of successfully read files. If empty, config was not read.
        if not config.read(CONFIG_FILE_PATH):
            print(f"[WARN] Configuration file '{CONFIG_FILE_PATH}' not found or empty. Using default Redis connection settings.")
        else:
            if 'Redis' in config:
                host = config.get('Redis', 'host', fallback='localhost')
                port = config.getint('Redis', 'port', fallback=6379) # getint handles conversion
                password_from_config = config.get('Redis', 'password', fallback=None)

                if password_from_config and password_from_config.strip():
                    password = password_from_config
                else:
                    password = None # Ensure empty string is treated as no password

                print(f"[INFO] Loaded Redis configuration from '{CONFIG_FILE_PATH}': host={host}, port={port}, password_provided={'yes' if password else 'no'}.")
            else:
                print(f"[WARN] '[Redis]' section not found in '{CONFIG_FILE_PATH}'. Using default Redis connection settings.")
    except configparser.Error as e:
        print(f"[WARN] Error parsing configuration file '{CONFIG_FILE_PATH}': {e}. Using default Redis connection settings.")
    except Exception as e: # Catch other potential errors during config loading, like incorrect types for fallback
        print(f"[WARN] An unexpected error occurred while reading Redis config: {e}. Using default settings.")


    try:
        # Prepare connection arguments for redis.Redis()
        connection_params = {
            'host': host,
            'port': port,
            'db': db,
            'decode_responses': True # Ensures strings are returned, not bytes.
        }
        if password:
            connection_params['password'] = password

        # decode_responses=True is important for commands returning strings/bytes
        r = redis.Redis(**connection_params)
        r.ping()  # Check if the connection is alive and working.
        print(f"[INFO] Successfully connected to Redis at {host}:{port}, db {db}")
        return r
    except redis.exceptions.AuthenticationError as e:
        print(f"[ERROR] Redis authentication failed for {host}:{port}, db {db}. Check password. Error: {e}")
        return None
    except redis.exceptions.ConnectionError as e:
        print(f"[ERROR] Could not connect to Redis at {host}:{port}, db {db}: {e}")
        return None
    except Exception as e:  # Catch other potential redis-py exceptions
        print(f"[ERROR] An error occurred during Redis connection to {host}:{port}, db {db}: {e}")
        return None

# --- Stream Naming Conventions ---
# These comments describe the standard patterns used for naming Redis Streams,
# ensuring consistency across the application.

# For 1-to-1 Chats: 'chat:userA_userB'
#   - User IDs are alphabetically sorted to ensure the stream name is unique
#     and consistent regardless of who initiates the chat.
#   - Example: chat:alice_bob

# For Group Chats: 'group_chat:group_name'
#   - 'group_name' is sanitized (e.g., lowercase, spaces replaced with underscores).
#   - Example: group_chat:tech_enthusiasts

def get_1_to_1_chat_stream_name(user1_id, user2_id):
    """
    Generates a consistent and unique Redis Stream name for 1-to-1 chats.

    It sorts the provided user IDs alphabetically before formatting the stream name.
    This ensures that get_1_to_1_chat_stream_name(userA, userB) and
    get_1_to_1_chat_stream_name(userB, userA) produce the same stream name,
    representing a unique chat session between the two users.

    Example: get_1_to_1_chat_stream_name("Alice", "Bob") returns "chat:Alice_Bob".

    Args:
        user1_id (str): The ID of the first user. Must be a non-empty string.
        user2_id (str): The ID of the second user. Must be a non-empty string.

    Returns:
        str: The formatted 1-to-1 chat stream name (e.g., 'chat:sortedUser1_sortedUser2').

    Raises:
        ValueError: If either user ID is empty or contains only whitespace.
    """
    # Validate inputs: ensure user IDs are provided and are not just whitespace.
    if not all([user1_id, user2_id, str(user1_id).strip(), str(user2_id).strip()]):
        raise ValueError("User IDs for 1-to-1 chat stream name cannot be empty or just whitespace.")

    # Sort user IDs alphabetically. User IDs are stripped of leading/trailing whitespace first.
    # This ensures consistency, e.g., (Alice, Bob) and (Bob, Alice) yield the same stream name.
    sorted_users = sorted([str(user1_id).strip(), str(user2_id).strip()])
    return f"chat:{sorted_users[0]}_{sorted_users[1]}"

def get_group_chat_stream_name(group_name):
    """
    Generates a Redis Stream name for group chats.

    The group name is sanitized by converting it to lowercase and replacing spaces
    with underscores. This helps in creating a valid and consistent key name for Redis.

    Example: get_group_chat_stream_name("Tech Talk") returns "group_chat:tech_talk".

    Args:
        group_name (str): The name of the group. Must be a non-empty string.

    Returns:
        str: The formatted and sanitized group chat stream name (e.g., 'group_chat:my_group_name').

    Raises:
        ValueError: If the group name is empty or contains only whitespace.
    """
    # Validate input: ensure group name is provided and not just whitespace.
    if not group_name or not str(group_name).strip():
        raise ValueError("Group name for group chat stream cannot be empty or just whitespace.")

    # Sanitize group name: convert to string, strip whitespace, replace spaces with underscores, and lowercase.
    # More robust sanitization (e.g., removing special characters not allowed in keys) could be added if necessary.
    safe_group_name = str(group_name).strip().replace(" ", "_").lower()
    return f"group_chat:{safe_group_name}"

# --- Redis Stream Operations ---

def publish_to_stream(redis_conn, stream_name, user_id, message_content):
    """
    Publishes a message to a specified Redis Stream.

    This function constructs a message dictionary containing the user ID, message content,
    and a UTC timestamp. It then uses the Redis XADD command to append this message
    to the stream. Redis auto-generates a unique message ID (timestamp-based with a sequence number).

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        stream_name (str): The name of the Redis Stream to publish to. This name should follow
                           the conventions defined (e.g., via `get_1_to_1_chat_stream_name` or
                           `get_group_chat_stream_name`).
        user_id (str): The ID of the user sending the message.
        message_content (str): The textual content of the message.

    Returns:
        str or None: The unique message ID generated by Redis if successful, None if an error occurs
                     (e.g., connection issue, Redis error).
    """
    if not redis: return None # Should not happen if connect_to_redis succeeded
    if not redis_conn:
        print("Redis connection not available for publishing to stream.")
        return None
    try:
        # Prepare the message data as a dictionary.
        # Timestamp is in ISO 8601 format, using UTC for consistency.
        message_data = {
            'user_id': user_id,
            'message': message_content,
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        # Use XADD command. The '*' for the ID field tells Redis to auto-generate a unique ID.
        message_id = redis_conn.xadd(stream_name, message_data, id='*')

        if message_id:
            # For debugging or logging purposes.
            # print(f"Message {message_id} published to stream '{stream_name}': {message_data}")
            return message_id
        else:
            # This case might be rare if Redis is operational, as XADD usually returns an ID or raises an error.
            print(f"Failed to publish message to stream '{stream_name}'. No message ID returned from XADD.")
            return None
    except redis.exceptions.RedisError as e:
        print(f"Redis error publishing to stream '{stream_name}': {e}")
        return None
    except Exception as e: # Catch any other unexpected errors.
        print(f"An unexpected error occurred during publish_to_stream for stream '{stream_name}': {e}")
        return None

def subscribe_to_stream(redis_conn, stream_name, callback_function, last_id='$', block_ms=None):
    """
    Subscribes to a Redis Stream and processes new messages using a provided callback function.

    This function uses a blocking Redis XREAD command to listen for new messages on the specified stream.
    When a message is received, it's passed to the `callback_function`.
    The subscription loop can be stopped with a KeyboardInterrupt (Ctrl+C).

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        stream_name (str): The name of the Redis Stream to subscribe to.
        callback_function (callable): A function that will be called for each new message.
                                      The callback receives a single argument: a dictionary where the key
                                      is the message ID and the value is another dictionary containing
                                      the message data (e.g., `{'message_id': {'field': 'value', ...}}`).
        last_id (str, optional): The message ID from which to start reading. Defaults to '$',
                                 which means only new messages arriving after the subscription starts
                                 will be processed. Use '0' to process all messages in the stream's history.
        block_ms (int, optional): The time in milliseconds to block while waiting for a message.
                                  If None or 0, XREAD blocks indefinitely. A positive value causes XREAD
                                  to return None if no message arrives within the timeout, allowing the
                                  loop to iterate and check for conditions like KeyboardInterrupt more frequently.

    Returns:
        None. This function loops indefinitely until a KeyboardInterrupt or an unhandled error occurs.
    """
    if not redis: return # Should not happen
    if not redis_conn:
        print("Redis connection not available for subscribing to stream.")
        return

    print(f"Attempting to subscribe to stream '{stream_name}' from message ID '{last_id}'.")
    print("Waiting for messages... Press Ctrl+C to stop listening.")

    # For XREAD, streams must be passed as a dictionary: {stream_name: last_message_id_read}
    streams_dict = {stream_name: last_id}

    try:
        while True:
            # Redis XREAD command:
            # - `streams_dict`: Specifies which stream(s) to read from and the last ID processed for each.
            # - `count=1`: Process one message at a time. Simplifies callback logic and ID tracking.
            # - `block=block_ms`: Specifies the blocking timeout in milliseconds.
            # The response is a list of lists, e.g., [['stream_name', [('msg_id_1', {'f1': 'v1'}), ...]]]
            # or None if XREAD times out (only if block_ms > 0).
            response = redis_conn.xread(streams_dict, count=1, block=block_ms)

            if response:
                # `response` contains data for all streams that had new messages.
                # Since we're listening to one stream, `response` will have one inner list.
                for _stream_name_received, messages in response: # _stream_name_received will be `stream_name`
                    for message_id, message_data in messages:
                        # Invoke the callback with the message ID and its data.
                        callback_function({message_id: message_data})
                        # Update the last ID for this stream to ensure that the next XREAD call
                        # fetches messages after this one.
                        streams_dict[stream_name] = message_id
            # If `response` is None (due to timeout from `block_ms`), the loop simply continues,
            # effectively polling if `block_ms` is set, or waiting if `block_ms` is None.
            # This allows KeyboardInterrupt to be caught more readily if `block_ms` is not indefinite.

    except KeyboardInterrupt:
        print(f"\nSubscription to stream '{stream_name}' stopped by user (Ctrl+C).")
    except redis.exceptions.ConnectionError as e:
        # Handle potential connection errors during the blocking read.
        print(f"Connection error while subscribing to stream '{stream_name}': {e}")
    except redis.exceptions.RedisError as e:
        # Handle other Redis-specific errors.
        print(f"Redis error while subscribing to stream '{stream_name}': {e}")
    except Exception as e:
        # Catch any other unexpected errors.
        print(f"An unexpected error occurred during subscription to stream '{stream_name}': {e}")
    finally:
        # This block executes whether the loop exited normally (not possible here) or due to an exception.
        print(f"Unsubscribed from stream '{stream_name}'.")

# --- RedisJSON Operations for Message Persistence ---

# Prefix for Redis keys that store chat messages in JSON format.
# This prefix is also used by the RediSearch index to identify relevant documents.
CHAT_MESSAGE_PREFIX = "chat_message:"

def add_chat_message_redisjson(redis_conn, chat_id, message_id_from_stream, message_data):
    """
    Adds a chat message (Python dictionary) to RedisJSON for persistent storage.

    The message is stored under a key constructed using `CHAT_MESSAGE_PREFIX`, the `chat_id`
    (which is typically the stream name), and the `message_id_from_stream`.
    This function requires the RedisJSON (ReJSON) module to be available on the Redis server.
    It uses the RedisJSON command `JSON.SET`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        chat_id (str): An identifier for the chat session or context. This should typically be
                       the stream name (e.g., from `get_1_to_1_chat_stream_name` or
                       `get_group_chat_stream_name`) to ensure consistency between stream data
                       and JSON-persisted data.
        message_id_from_stream (str): The message ID obtained from publishing to a Redis Stream.
                                      This ID is used to form part of the unique RedisJSON key.
        message_data (dict): The chat message data (as a Python dictionary) to be stored
                             as a JSON document in Redis.

    Returns:
        bool: True if the message was successfully stored in RedisJSON, False otherwise.
    """
    if not redis: return False
    if not redis_conn:
        print("Redis connection not available for RedisJSON operation.")
        return False
    if not hasattr(redis_conn, 'json'):
        # Check if the `json()` client object, specific to RedisJSON commands via redis-py, is available.
        print("RedisJSON commands are not available on this client. "
              "Ensure redis-py is correctly installed with JSON support "
              "and the Redis server has the ReJSON module loaded.")
        return False

    try:
        # Construct a unique key for the RedisJSON document.
        # Example key: "chat_message:chat:alice_bob:1678886400000-0"
        redis_json_key = f"{CHAT_MESSAGE_PREFIX}{chat_id}:{message_id_from_stream}"

        # Use `redis_conn.json().set()` to store the Python dictionary as a JSON document.
        # The path `.` indicates that the entire `message_data` dictionary should be stored
        # at the root of the JSON document associated with `redis_json_key`.
        result = redis_conn.json().set(redis_json_key, '.', message_data)

        # For modern redis-py, `json().set()` typically returns True on success.
        # Older versions or direct command execution might return "OK".
        if result:
            # print(f"Message {message_id_from_stream} stored in RedisJSON at key '{redis_json_key}'")
            return True
        else:
            # This case might not be common if redis-py raises an error on failure.
            print(f"Failed to store message {message_id_from_stream} in RedisJSON at key '{redis_json_key}'. "
                  f"Command result: {result}")
            return False
    except redis.exceptions.ResponseError as e:
        # This error is common if the RedisJSON module is not loaded on the server
        # (e.g., "ERR unknown command `JSON.SET`") or if there are other command-level errors.
        print(f"RedisJSON ResponseError for message {message_id_from_stream} in chat '{chat_id}': {e}")
        print("Ensure the RedisJSON (ReJSON) module is loaded and enabled on your Redis server.")
        return False
    except redis.exceptions.RedisError as e:
        # Catch other general Redis-related errors.
        print(f"Redis Error storing message {message_id_from_stream} in chat '{chat_id}' using RedisJSON: {e}")
        return False
    except Exception as e:
        # Catch any other unexpected exceptions.
        print(f"An unexpected error occurred during add_chat_message_redisjson for chat '{chat_id}': {e}")
        return False

# --- Redis Search Functionality (RediSearch Module) ---

# Default name for the RediSearch index used for chat messages.
CHAT_MESSAGE_INDEX_NAME = "idx:chat_messages"
# Note: CHAT_MESSAGE_PREFIX is already defined under RedisJSON section.

def create_chat_message_index(redis_conn, index_name=CHAT_MESSAGE_INDEX_NAME, prefix=CHAT_MESSAGE_PREFIX):
    """
    Creates a RediSearch index for chat messages if it doesn't already exist.

    The index is configured to work with JSON documents stored in Redis that have keys
    starting with the specified `prefix` (e.g., "chat_message:").
    It defines a schema to index specific fields from these JSON documents:
    - `user_id`: Indexed as a TAG for exact, case-sensitive matching (e.g., filtering by user).
    - `message`: Indexed as TEXT for full-text search capabilities on message content.
    - `timestamp`: Indexed as TEXT. For actual date/time range queries, storing timestamps
                   as Unix epoch (numeric) and indexing with `NumericField` would be more appropriate.

    This function uses RediSearch commands: FT.CREATE (to create the index) and
    FT.INFO (to check if the index already exists).

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        index_name (str, optional): The name for the search index.
                                    Defaults to `CHAT_MESSAGE_INDEX_NAME`.
        prefix (str, optional): The key prefix for documents that this index should cover.
                                Defaults to `CHAT_MESSAGE_PREFIX`.

    Returns:
        bool: True if the index was successfully created or if it already existed.
              False if an error occurred or if RediSearch capabilities are not available
              (either on the client or server).
    """
    if not redis: return False
    if not redis_conn:
        print("Redis connection not available for creating RediSearch index.")
        return False
    if not hasattr(redis_conn, 'ft'):
        # Check if the `ft()` client object for RediSearch commands is available.
        print("RediSearch commands (ft) are not available on this client. "
              "Ensure redis-py is installed with search support "
              "and the RediSearch (FT) module is loaded on the Redis server.")
        return False

    try:
        # Define the schema for the index, specifying fields from the JSON documents to index.
        # `$` refers to the root of the JSON document. `$.user_id` is the JSONPath to the user_id field.
        # `as_name="user_id"` sets the alias for this field in search queries.
        schema = (
            TagField("$.user_id", as_name="user_id"),       # TAG for exact matches, filtering.
            TextField("$.message", as_name="message"),     # TEXT for full-text search.
            TextField("$.timestamp", as_name="timestamp")  # TEXT for exact string match on timestamp.
                                                           # Consider NumericField for range queries if timestamp is numeric.
        )

        # Define which documents the index applies to:
        # - Keys must have the specified `prefix`.
        # - Documents are of type JSON (`IndexType.JSON`).
        definition = IndexDefinition(prefix=[prefix], index_type=IndexType.JSON)

        # Attempt to get information about the index to check if it already exists.
        # If `ft().info()` executes without error, the index exists.
        try:
            redis_conn.ft(index_name).info()
            # print(f"RediSearch index '{index_name}' already exists. Skipping creation.")
            return True
        except redis.exceptions.ResponseError as e:
            # A ResponseError, especially one indicating "Unknown Index name", means the index does not exist.
            # The exact error message for "unknown index" can vary, so checking common phrases is a heuristic.
            if "unknown index name" in str(e).lower() or "idx:" in str(e).lower(): # More generic check for unknown index
                 pass  # Index does not exist, so proceed to create it.
            else:
                 raise e  # Re-raise other unexpected ResponseErrors not related to index non-existence.

        # If the index does not exist, create it using the defined schema and document definition.
        redis_conn.ft(index_name).create_index(fields=schema, definition=definition)
        print(f"RediSearch index '{index_name}' created successfully for documents with prefix '{prefix}'.")
        return True

    except redis.exceptions.ResponseError as e:
        # This might catch "Index already exists" if the `info()` check was not specific enough,
        # or in a rare race condition (less likely in typical single-threaded Python app initialization).
        if "already exists" in str(e).lower(): # Defensive check
            # print(f"RediSearch index '{index_name}' already exists (caught during create_index attempt).")
            return True # Index exists, so consider it a success for this function's purpose.
        print(f"Redis ResponseError creating search index '{index_name}': {e}")
        print("Ensure the RediSearch (FT) module is loaded and enabled on your Redis server.")
        return False
    except ImportError: # If redis.commands.search components were not imported successfully.
        print("Failed to import RediSearch components from redis-py. "
              "Ensure redis-py is installed with search support (e.g., `redis[search]`).")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while creating RediSearch index '{index_name}': {e}")
        return False

def search_chat_messages(redis_conn, query_string, index_name=CHAT_MESSAGE_INDEX_NAME):
    """
    Searches chat messages stored in RedisJSON using a RediSearch query string.
    This function uses the RediSearch command FT.SEARCH.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        query_string (str): The RediSearch query string.
                            Examples:
                            - "@user_id:Alice" (messages where user_id is Alice)
                            - "hello world" (messages containing "hello" and "world" in the 'message' field)
                            - "@message:'quick brown fox'" (exact phrase in 'message' field)
                            - "@user_id:Bob @message:food" (messages from Bob containing "food")
        index_name (str, optional): The name of the search index to query.
                                    Defaults to `CHAT_MESSAGE_INDEX_NAME`.

    Returns:
        list: A list of message dictionaries matching the query. Each dictionary represents
              a found document and includes its ID (Redis key) and indexed fields.
              Returns an empty list if an error occurs, no results are found, or if
              RediSearch capabilities are not available.
    """
    if not redis: return []
    if not redis_conn:
        print("Redis connection not available for searching messages.")
        return []
    if not hasattr(redis_conn, 'ft'):
        print("RediSearch commands (ft) are not available on this Redis client.")
        return []

    try:
        # Construct a Query object from the provided query string.
        query = Query(query_string)

        # Perform the search using `ft().search()`. This returns a Result object.
        result_object = redis_conn.ft(index_name).search(query)

        # The `result_object.docs` attribute contains a list of Document objects.
        # Each Document object has attributes like 'id' (the Redis key of the document)
        # and other attributes corresponding to the fields defined in the schema
        # (e.g., doc.user_id, doc.message, doc.timestamp).
        # `result_object.total` gives the total number of matching documents in Redis.

        messages = []
        if result_object.docs:
            for doc in result_object.docs:
                # Convert the Document object into a more standard Python dictionary.
                # The 'id' attribute is the Redis key of the document.
                # For JSON documents indexed with IndexType.JSON, fields are often directly
                # accessible as attributes on the `doc` object (e.g., `doc.user_id`).
                msg_dict = {'id': doc.id}

                # Populate the dictionary with known fields from the schema.
                for field_name in ['user_id', 'message', 'timestamp']:
                    if hasattr(doc, field_name):
                        msg_dict[field_name] = getattr(doc, field_name)

                # If payload exists and contains the full JSON, it could be used,
                # but direct field access is cleaner with JSON indexes.
                # if hasattr(doc, 'payload') and doc.payload:
                #     try:
                #         import json # Ensure json is imported if this path is used
                #         payload_dict = json.loads(doc.payload)
                #         msg_dict.update(payload_dict) # Merge payload data
                #     except (ImportError, TypeError, json.JSONDecodeError) as e:
                #         print(f"Error processing payload for doc {doc.id}: {e}")

                messages.append(msg_dict)

        # print(f"Search for '{query_string}' in index '{index_name}' found {result_object.total} results.")
        return messages

    except redis.exceptions.ResponseError as e:
        # This can occur if the index doesn't exist or the query syntax is invalid.
        print(f"Redis ResponseError during search on index '{index_name}' with query '{query_string}': {e}")
        return []
    except ImportError: # If redis.commands.search.query was not imported.
        print("Failed to import RediSearch Query component from redis-py. "
              "Ensure redis-py is installed with search support (e.g., `redis[search]`).")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during search on index '{index_name}' with query '{query_string}': {e}")
        return []

# --- Generic Room-Based User Management (Redis Sets) ---
# These functions manage users in generic "rooms", distinct from the more structured
# "group chats" which have specific naming and membership management.

def add_user_to_chat_room(redis_conn, room_name, user_id):
    """
    Adds a user to a Redis Set representing a generic chat room's occupants.
    This is a general-purpose function for room membership, potentially for features
    not tied to the specific 1-to-1 or group chat stream conventions.
    Uses the Redis SADD command. The key for the set is `room:<room_name>:users`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        room_name (str): The name of the chat room. This name is used directly
                         as part of the Redis key for the Set.
        user_id (str): The ID of the user to add to the room.

    Returns:
        int or None: The number of users added to the Set (1 if the user was new,
                     0 if the user was already a member). Returns None if an error occurs
                     or if the Redis connection is not available.
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for add_user_to_chat_room.")
        return None
    try:
        # Define the Redis key for storing users in this specific room.
        room_key = f"room:{room_name}:users"
        return redis_conn.sadd(room_key, user_id)
    except redis.exceptions.RedisError as e:
        print(f"Redis error adding user '{user_id}' to room '{room_name}': {e}")
        return None

def remove_user_from_chat_room(redis_conn, room_name, user_id):
    """
    Removes a user from a Redis Set representing a generic chat room's occupants.
    Uses the Redis SREM command. The key for the set is `room:<room_name>:users`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        room_name (str): The name of the chat room.
        user_id (str): The ID of the user to remove from the room.

    Returns:
        int or None: The number of users removed from the Set (1 if the user was removed,
                     0 if the user was not a member). Returns None if an error occurs.
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for remove_user_from_chat_room.")
        return None
    try:
        room_key = f"room:{room_name}:users"
        return redis_conn.srem(room_key, user_id)
    except redis.exceptions.RedisError as e:
        print(f"Redis error removing user '{user_id}' from room '{room_name}': {e}")
        return None

def get_users_in_chat_room(redis_conn, room_name):
    """
    Retrieves all users from the Redis Set for a generic chat room.
    Uses the Redis SMEMBERS command. The key for the set is `room:<room_name>:users`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        room_name (str): The name of the chat room.

    Returns:
        set or None: A set of user IDs (strings) currently in the room. Returns an empty set
                     if the room does not exist or has no users. Returns None if a Redis error occurs.
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for get_users_in_chat_room.")
        return None
    try:
        room_key = f"room:{room_name}:users"
        users = redis_conn.smembers(room_key) # Returns a set of member strings.
        return users
    except redis.exceptions.RedisError as e:
        print(f"Redis error getting users from room '{room_name}': {e}")
        return None

# --- Group Chat Membership Management (Redis Sets) ---
# These functions manage user membership within specific, named group chats.
# They use sanitized group names to form consistent Redis keys for Sets.

def join_group_chat(redis_conn, group_name, user_id):
    """
    Adds a user to a Redis Set that stores members of a specific group chat.

    The `group_name` is first sanitized (e.g., lowercased, spaces to underscores) using
    `get_group_chat_stream_name` to derive a consistent part of the Redis key.
    The key for the Set is `group_members:<sanitized_group_name_part>`.
    Uses the Redis SADD command.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        group_name (str): The name of the group chat. This will be sanitized.
        user_id (str): The ID of the user joining the group. Whitespace will be stripped.

    Returns:
        int or None: The number of users added to the group's Set (1 if the user was new,
                     0 if the user was already a member). Returns None if an error occurs
                     (e.g., connection issue, invalid group_name or user_id after validation).
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for join_group_chat.")
        return None
    # Validate that group_name and user_id are provided and not just whitespace.
    if not group_name or not str(group_name).strip() or not user_id or not str(user_id).strip():
        print("Group name and user ID for join_group_chat cannot be empty or just whitespace.")
        return None
    try:
        # Derive the key name from the sanitized group stream name for consistency.
        # `get_group_chat_stream_name` returns "group_chat:sanitized_name", so we extract the part after "group_chat:".
        sanitized_group_name_part = get_group_chat_stream_name(group_name).split(':', 1)[-1]
        group_members_key = f"group_members:{sanitized_group_name_part}"
        return redis_conn.sadd(group_members_key, str(user_id).strip())
    except redis.exceptions.RedisError as e:
        print(f"Redis error adding user '{str(user_id).strip()}' to group '{str(group_name).strip()}': {e}")
        return None
    except ValueError as e: # Catches error from get_group_chat_stream_name if group_name is invalid.
        print(f"Error determining group members key for group '{str(group_name).strip()}': {e}")
        return None


def leave_group_chat(redis_conn, group_name, user_id):
    """
    Removes a user from a Redis Set representing members of a specific group chat.

    The `group_name` is sanitized. Uses the Redis SREM command.
    The key for the Set is `group_members:<sanitized_group_name_part>`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        group_name (str): The name of the group chat. Will be sanitized.
        user_id (str): The ID of the user leaving the group. Whitespace will be stripped.

    Returns:
        int or None: The number of users removed from the group's Set (1 if the user was removed,
                     0 if the user was not a member). Returns None if an error occurs.
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for leave_group_chat.")
        return None
    if not group_name or not str(group_name).strip() or not user_id or not str(user_id).strip():
        print("Group name and user ID for leave_group_chat cannot be empty or just whitespace.")
        return None
    try:
        sanitized_group_name_part = get_group_chat_stream_name(group_name).split(':', 1)[-1]
        group_members_key = f"group_members:{sanitized_group_name_part}"
        return redis_conn.srem(group_members_key, str(user_id).strip())
    except redis.exceptions.RedisError as e:
        print(f"Redis error removing user '{str(user_id).strip()}' from group '{str(group_name).strip()}': {e}")
        return None
    except ValueError as e: # From get_group_chat_stream_name.
        print(f"Error determining group members key for group '{str(group_name).strip()}': {e}")
        return None

def get_group_chat_members(redis_conn, group_name):
    """
    Retrieves all members of a specific group chat from its Redis Set.

    The `group_name` is sanitized. Uses the Redis SMEMBERS command.
    The key for the Set is `group_members:<sanitized_group_name_part>`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
        group_name (str): The name of the group chat. Will be sanitized.

    Returns:
        set or None: A set of user IDs (strings) in the group. Returns an empty set if the
                     group does not exist or has no members. Returns None if an error occurs.
    """
    if not redis: return None
    if not redis_conn:
        print("Redis connection not available for get_group_chat_members.")
        return None
    if not group_name or not str(group_name).strip():
        print("Group name for get_group_chat_members cannot be empty or just whitespace.")
        return None
    try:
        sanitized_group_name_part = get_group_chat_stream_name(group_name).split(':', 1)[-1]
        group_members_key = f"group_members:{sanitized_group_name_part}"
        members = redis_conn.smembers(group_members_key) # Returns a set of strings.
        return members
    except redis.exceptions.RedisError as e:
        print(f"Redis error getting members for group '{str(group_name).strip()}': {e}")
        return None
    except ValueError as e: # From get_group_chat_stream_name.
        print(f"Error determining group members key for group '{str(group_name).strip()}': {e}")
        return None


if __name__ == '__main__':
    # This block is executed when the script `redis_utils.py` is run directly.
    # It serves as a basic test and demonstration suite for the functions defined in this module.
    # It covers:
    #   - Connection to Redis.
    #   - Stream name generation for 1-to-1 and group chats.
    #   - Group membership management (join, list, leave).
    #   - RediSearch index creation and querying.
    #   - Basic Stream publish and subscribe operations.

    print("Executing redis_utils.py directly for testing and demonstration...")
    r_test_conn = connect_to_redis() # Attempt to connect to the local Redis server.

    if r_test_conn:
        print("\n[TESTING] === Stream Naming Conventions ===")
        user_a = "Alice"
        user_b = "Bob"
        user_c = "charlie" # Note: lowercase 'c' for sorting test
        try:
            # Test 1-to-1 stream name generation
            stream1 = get_1_to_1_chat_stream_name(user_a, user_b)
            print(f"  1-to-1 stream for '{user_a}', '{user_b}': {stream1} (Expected: chat:Alice_Bob)")
            assert stream1 == "chat:Alice_Bob", "Test Failed: User A, User B stream name"

            stream2 = get_1_to_1_chat_stream_name(user_b, user_a) # Reversed order should yield same name
            print(f"  1-to-1 stream for '{user_b}', '{user_a}': {stream2} (Expected: chat:Alice_Bob)")
            assert stream2 == "chat:Alice_Bob", "Test Failed: User B, User A stream name (reversed)"

            stream3 = get_1_to_1_chat_stream_name(user_a, user_c) # Test sorting with different cases (Alice, charlie)
            print(f"  1-to-1 stream for '{user_a}', '{user_c}': {stream3} (Expected: chat:Alice_charlie)")
            assert stream3 == "chat:Alice_charlie", "Test Failed: User A, User C stream name"

            # Test invalid (empty) user IDs for 1-to-1 stream names
            print("  Testing invalid user ID for 1-to-1 chat stream (expect ValueError)...")
            try:
                get_1_to_1_chat_stream_name(" ", "test_user_non_empty")
                assert False, "ValueError not raised for empty user ID in 1-to-1 chat name."
            except ValueError as e:
                print(f"  Caught expected ValueError for invalid user ID: {e}")
        except Exception as e:
            print(f"  UNEXPECTED ERROR during 1-to-1 stream naming tests: {e}")

        try:
            # Test group chat stream name generation
            group1_name = "Tech Enthusiasts"
            group_stream1 = get_group_chat_stream_name(group1_name)
            print(f"  Group chat stream for '{group1_name}': {group_stream1} (Expected: group_chat:tech_enthusiasts)")
            assert group_stream1 == "group_chat:tech_enthusiasts", "Test Failed: Group name sanitization (spaces, case)"

            group2_name = " dev corner " # Test with leading/trailing spaces and mixed case
            group_stream2 = get_group_chat_stream_name(group2_name)
            print(f"  Group chat stream for '{group2_name}': {group_stream2} (Expected: group_chat:dev_corner)")
            assert group_stream2 == "group_chat:dev_corner", "Test Failed: Group name sanitization (leading/trailing space)"

            # Test invalid (empty) group name for group chat stream names
            print("  Testing invalid group name for group chat stream (expect ValueError)...")
            try:
                get_group_chat_stream_name("   ")
                assert False, "ValueError not raised for empty group name."
            except ValueError as e:
                print(f"  Caught expected ValueError for invalid group name: {e}")
        except Exception as e:
            print(f"  UNEXPECTED ERROR during group stream naming tests: {e}")


        print("\n[TESTING] === Group Membership Functions ===")
        test_group_name = "Garden Club RUs Test" # Using "RUs" for Redis Utils test
        # Key for group members set, derived similarly to how it's done within the functions for cleanup.
        sanitized_test_group_name_part = get_group_chat_stream_name(test_group_name).split(':', 1)[-1]
        test_group_members_key = f"group_members:{sanitized_test_group_name_part}"

        user_gm1 = "Gardener1"
        user_gm2 = "Planter2"

        r_test_conn.delete(test_group_members_key) # Clean up before test for a fresh state.

        join_group_chat(r_test_conn, test_group_name, user_gm1)
        join_result = join_group_chat(r_test_conn, test_group_name, user_gm2)
        print(f"  Join result for {user_gm2} into '{test_group_name}': {join_result} (Expected: 1, meaning added)")
        members = get_group_chat_members(r_test_conn, test_group_name)
        print(f"  Members in '{test_group_name}': {members}")
        assert user_gm1 in (members or set()) and user_gm2 in (members or set()), "Test Failed: Initial group members not added."

        leave_result = leave_group_chat(r_test_conn, test_group_name, user_gm1)
        print(f"  Leave result for {user_gm1} from '{test_group_name}': {leave_result} (Expected: 1, meaning removed)")
        members_after_leave = get_group_chat_members(r_test_conn, test_group_name)
        print(f"  Members in '{test_group_name}' after {user_gm1} left: {members_after_leave}")
        assert user_gm1 not in (members_after_leave or set()), f"Test Failed: {user_gm1} was not removed from group."

        join_existing_result = join_group_chat(r_test_conn, test_group_name, user_gm2) # Try joining an existing member
        print(f"  Join existing member {user_gm2} result: {join_existing_result} (Expected: 0, as already a member)")
        assert join_existing_result == 0, "Test Failed: Joining an existing member should return 0."

        leave_non_existent_result = leave_group_chat(r_test_conn, test_group_name, "GhostUser") # Try leaving a non-member
        print(f"  Leave non-existent member result: {leave_non_existent_result} (Expected: 0)")
        assert leave_non_existent_result == 0, "Test Failed: Leaving a non-existent member should return 0."

        r_test_conn.delete(test_group_members_key) # Clean up the group members set.
        print(f"  Cleaned up group members key: {test_group_members_key}")


        print("\n[TESTING] === Redis Search Functionality ===")
        index_created_successfully = create_chat_message_index(r_test_conn)
        print(f"  Search index creation status: {index_created_successfully}")

        if not index_created_successfully:
            print("  Skipping RediSearch tests as index creation/verification failed. "
                  "Ensure RediSearch (FT module) is installed and enabled on the Redis server.")
        else:
            # Add sample messages directly as RedisJSON documents for search testing.
            # These messages must have keys prefixed with `CHAT_MESSAGE_PREFIX`.
            search_user_alice = "AliceSearchUser"
            search_user_bob = "BobSearchUser"

            # Sample Message 1 (from Alice)
            # For 1-to-1 chat between AliceSearchUser and SearchPartnerAlpha
            chat_id_search1 = get_1_to_1_chat_stream_name(search_user_alice, "SearchPartnerAlpha")
            msg_data1 = {'user_id': search_user_alice, 'message': 'Hello Bob, Alice here. Discussing world peace and Python.',
                         'timestamp': datetime.datetime.utcnow().isoformat()}
            # For testing, message_id can be arbitrary if not tied to an actual stream message.
            msg_id_json1 = "search_test_msg_1"
            # Construct the full Redis key for the JSON document. This key will be indexed.
            key1 = f"{CHAT_MESSAGE_PREFIX}{chat_id_search1}:{msg_id_json1}"
            add_chat_message_redisjson(r_test_conn, chat_id_search1, msg_id_json1, msg_data1)

            # Sample Message 2 (from Bob)
            # For a group chat "Public Search Group"
            chat_id_search2 = get_group_chat_stream_name("Public Search Group")
            msg_data2 = {'user_id': search_user_bob, 'message': 'Hi Alice, Bob again. Let us find some good food and talk Python.',
                         'timestamp': datetime.datetime.utcnow().isoformat()}
            msg_id_json2 = "search_test_msg_2"
            key2 = f"{CHAT_MESSAGE_PREFIX}{chat_id_search2}:{msg_id_json2}"
            add_chat_message_redisjson(r_test_conn, chat_id_search2, msg_id_json2, msg_data2)

            # Sample Message 3 (from Alice, different chat context)
            # For a group chat "Alice Private Thoughts"
            chat_id_search3 = get_group_chat_stream_name("Alice Private Thoughts")
            msg_data3 = {'user_id': search_user_alice, 'message': 'Alice needs a quiet place to think about Python projects.',
                         'timestamp': datetime.datetime.utcnow().isoformat()}
            msg_id_json3 = "search_test_msg_3"
            key3 = f"{CHAT_MESSAGE_PREFIX}{chat_id_search3}:{msg_id_json3}"
            add_chat_message_redisjson(r_test_conn, chat_id_search3, msg_id_json3, msg_data3)

            import time # Allow a very brief moment for RediSearch to index the new documents.
            time.sleep(0.2) # Increased slightly for reliability in tests

            # Perform various search queries and print results.
            print(f"\n  Searching for messages from user '{search_user_alice}': (@user_id:{search_user_alice})")
            alice_results = search_chat_messages(r_test_conn, f"@user_id:{search_user_alice}")
            for res_doc in alice_results: print(f"    - ID: {res_doc.get('id')}, Message: '{res_doc.get('message')}'")
            assert len(alice_results) >= 2, "Test Failed: Search for Alice's messages"


            print("\n  Searching for messages containing the word 'Python': (Python)")
            python_results = search_chat_messages(r_test_conn, "Python") # Text search for 'Python'
            for res_doc in python_results: print(f"    - ID: {res_doc.get('id')}, Message: '{res_doc.get('message')}'")
            assert len(python_results) >= 3, "Test Failed: Search for 'Python'"


            print(f"\n  Searching for messages from '{search_user_bob}' containing 'food': (@user_id:{search_user_bob} food)")
            bob_food_results = search_chat_messages(r_test_conn, f"@user_id:{search_user_bob} food")
            for res_doc in bob_food_results: print(f"    - ID: {res_doc.get('id')}, Message: '{res_doc.get('message')}'")
            assert len(bob_food_results) >= 1, "Test Failed: Search for Bob's messages with 'food'"

            # Clean up test JSON documents created specifically for these search tests.
            print("\n  Cleaning up RediSearch test JSON documents...")
            r_test_conn.delete(key1, key2, key3)

            # Optional: Drop the index after tests if a completely clean slate is needed for each full test run.
            # For most testing scenarios, the index can be reused. For CI/CD, dropping might be useful.
            # try:
            #     r_test_conn.ft(CHAT_MESSAGE_INDEX_NAME).dropindex()
            #     print(f"  Search index '{CHAT_MESSAGE_INDEX_NAME}' dropped.")
            # except Exception as e:
            #     print(f"  Error dropping index '{CHAT_MESSAGE_INDEX_NAME}': {e}")


        print("\n[TESTING] === Stream Publish and Subscribe (Basic) ===")
        # Test basic publish and subscribe functionality using a 1-to-1 chat stream name.
        sub_test_user1 = "SubscriberMain"
        sub_test_user2 = "PublisherMain"
        test_sub_stream_1to1 = get_1_to_1_chat_stream_name(sub_test_user1, sub_test_user2)

        # A simple callback function for the subscription test.
        _received_messages_for_test = [] # Use a list to capture messages for assertion
        def _test_subscription_callback(message_detail):
            print(f"  [_test_subscription_callback on {test_sub_stream_1to1}] Received: {message_detail}")
            _received_messages_for_test.append(message_detail)

        print(f"  Testing subscription on stream: '{test_sub_stream_1to1}'")
        r_test_conn.delete(test_sub_stream_1to1) # Clean up any old stream data before the test.

        # Publish a message to the test stream.
        test_message_content = "Hello subscriber, this is a message for the subscription test."
        msg_id_sub_test = publish_to_stream(r_test_conn, test_sub_stream_1to1, sub_test_user2, test_message_content)

        if msg_id_sub_test:
            print(f"  Message {msg_id_sub_test} published to {test_sub_stream_1to1}. "
                  f"Subscribing with last_id='0' (will block for up to 1s)...")
            # Subscribe to read the message just published. `block_ms` makes it non-indefinite for testing.
            subscribe_to_stream(r_test_conn, test_sub_stream_1to1, _test_subscription_callback,
                                last_id='0', block_ms=1000)
            assert len(_received_messages_for_test) > 0, "Test Failed: Did not receive message in subscription."
            # Further assertions could check the content of _received_messages_for_test[0]
        else:
            print(f"  Failed to publish initial message for {test_sub_stream_1to1} subscription test.")
            assert False, "Test Failed: Publishing message for subscription test."

        r_test_conn.delete(test_sub_stream_1to1) # Clean up the stream after the test.
        print(f"  Cleaned up stream '{test_sub_stream_1to1}' after subscription test.")

        print("\n[INFO] All redis_utils.py tests and demonstrations complete.")
    else:
        print("[ERROR] Could not connect to Redis. Skipping all tests in redis_utils.py.")
