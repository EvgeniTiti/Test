"""
Main application file for the Python Chat application.

This script serves as the entry point and demonstrator for various chat functionalities
implemented using Redis. It showcases:
- User management in generic rooms.
- Sending and receiving messages in 1-to-1 and group chats using Redis Streams.
- Managing user membership in group chats using Redis Sets.
- Storing chat messages persistently with RedisJSON.
- Searching chat messages using RediSearch.

The script is structured with modular "flow" functions, each demonstrating a specific
aspect of the application. The `main()` function orchestrates these flows.
This file relies on `redis_utils.py` for all Redis interactions.

Note: For real-world applications, sending and receiving messages would typically
      occur in separate processes or threads. Here, they might be simulated sequentially
      or with self-generated messages for demonstration within a single script run.
      Cleanup of test data (streams, JSON docs, sets) is performed within some flows
      to keep the Redis instance tidy during demonstrations.
"""

# Standard library imports
import datetime
# import json # Potentially needed if manually parsing JSON from search results, though current search_utils handles it.

# Local application/library specific imports
import redis_utils


def manage_user_flow(redis_conn):
    """
    Demonstrates basic user management in a generic chat room using Redis Sets.

    This flow simulates:
    1. A user joining a chat room (`redis_utils.add_user_to_chat_room`).
    2. Listing users currently in the room (`redis_utils.get_users_in_chat_room`).
    3. The user leaving the room (`redis_utils.remove_user_from_chat_room`).
    4. Listing users again to see the change.
    It also handles cleanup of the test room data in Redis.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate user management: Redis connection not available.")
        return

    default_room = "general_lobby"  # Example room name
    test_user_id = "user:lobby_tester_001"   # Example user ID

    print(f"\n--- Simulating User Management in Generic Room: '{default_room}' ---")

    # Step 1: Simulate a user joining the room.
    # `add_user_to_chat_room` uses Redis SADD to add user_id to a set like "room:general_lobby:users".
    print(f"  Action: Adding user '{test_user_id}' to room '{default_room}'...")
    add_result = redis_utils.add_user_to_chat_room(redis_conn, default_room, test_user_id)
    if add_result is not None: # SADD returns number of elements added.
        print(f"  Outcome: User '{test_user_id}' successfully added to '{default_room}'.")
    else:
        print(f"  Outcome: Failed to add user '{test_user_id}' to '{default_room}'.")

    # Step 2: Print the list of users in the room.
    # `get_users_in_chat_room` uses Redis SMEMBERS on the room's user set.
    users_in_room = redis_utils.get_users_in_chat_room(redis_conn, default_room)
    if users_in_room is not None:
        print(f"  Current users in '{default_room}': {users_in_room}")
    else:
        print(f"  Could not retrieve users for room '{default_room}'.")

    # Step 3: Simulate the user leaving the room.
    # `remove_user_from_chat_room` uses Redis SREM.
    print(f"  Action: Removing user '{test_user_id}' from room '{default_room}'...")
    remove_result = redis_utils.remove_user_from_chat_room(redis_conn, default_room, test_user_id)
    if remove_result is not None: # SREM returns number of elements removed.
        print(f"  Outcome: User '{test_user_id}' successfully removed from '{default_room}'.")
    else:
        print(f"  Outcome: Failed to remove user '{test_user_id}' from '{default_room}'.")

    # Step 4: Print the updated list of users.
    updated_users_in_room = redis_utils.get_users_in_chat_room(redis_conn, default_room)
    if updated_users_in_room is not None:
        print(f"  Updated users in '{default_room}': {updated_users_in_room}")
    else:
        print(f"  Could not retrieve updated users for room '{default_room}'.")

    # Cleanup: Remove the test room's user set from Redis.
    # This is important for keeping the Redis database clean after tests/demos.
    room_key_to_delete = f"room:{default_room}:users"
    if redis_conn.exists(room_key_to_delete):
        redis_conn.delete(room_key_to_delete)
        print(f"  Cleanup: Test room data for '{default_room}' (key: {room_key_to_delete}) deleted.")
    else:
        # This might happen if the initial add failed or if the set was already empty and auto-deleted by Redis.
        print(f"  Cleanup: No data to clean up for room '{default_room}' (key '{room_key_to_delete}' did not exist).")
    print("--- User Management Simulation Complete ---")


def send_chat_message_flow(redis_conn):
    """
    Demonstrates sending a single chat message to a generic stream and storing it in RedisJSON.
    This is an older flow, `send_chat_message_flow_differentiated` is preferred for new conventions.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate message sending: Redis connection not available.")
        return

    # Define a generic stream name for this older flow.
    stream_name = "main_chat_stream_legacy"
    user_id = "user_legacy_sender_789"
    message_content = (f"Hello from {user_id} at {datetime.datetime.utcnow().isoformat()}! "
                       f"This is a test message to the legacy stream '{stream_name}'.")

    print(f"\n--- Simulating Legacy Message Sending ---")
    print(f"  User '{user_id}' attempting to send message to stream '{stream_name}': \"{message_content[:50]}...\"")

    # Step 1: Publish the message to the Redis Stream.
    # `publish_to_stream` uses XADD and creates a message dictionary with timestamp.
    message_id_from_stream = redis_utils.publish_to_stream(
        redis_conn, stream_name, user_id, message_content
    )

    if message_id_from_stream:
        print(f"  Message published to Stream '{stream_name}' successfully. Message ID: {message_id_from_stream}")

        # Step 2: Retrieve the full message data that was stored in the stream.
        # This ensures that what's stored in RedisJSON accurately reflects the stream content.
        # `xrange` retrieves messages from a stream within a given ID range.
        try:
            message_entries = redis_conn.xrange(stream_name, min=message_id_from_stream, max=message_id_from_stream, count=1)
            if message_entries:
                _retrieved_id, message_data_for_json = message_entries[0] # Extract message data dict.

                # Step 3: Store the message in RedisJSON for persistence and search.
                # `add_chat_message_redisjson` uses JSON.SET. The `chat_id` for RedisJSON key
                # here is the stream_name itself for simplicity in this legacy example.
                print(f"  Attempting to store message in RedisJSON with chat_id='{stream_name}', message_id='{message_id_from_stream}'...")
                store_success = redis_utils.add_chat_message_redisjson(
                    redis_conn,
                    chat_id=stream_name,
                    message_id_from_stream=message_id_from_stream,
                    message_data=message_data_for_json
                )

                if store_success:
                    print("  Message successfully stored in RedisJSON.")
                    # Cleanup: Remove the specific RedisJSON entry created for this test.
                    redis_json_key = f"{redis_utils.CHAT_MESSAGE_PREFIX}{stream_name}:{message_id_from_stream}"
                    if redis_conn.exists(redis_json_key):
                        redis_conn.delete(redis_json_key)
                        print(f"  Cleanup: Test RedisJSON entry '{redis_json_key}' deleted.")
                else:
                    print("  Failed to store message in RedisJSON.")
            else:
                print(f"  Could not retrieve message {message_id_from_stream} from stream to store in RedisJSON.")

        except redis_utils.redis.exceptions.RedisError as e: # redis_utils.redis might not be the right way to access this
            print(f"  Redis error during RedisJSON storage or related stream read: {e}")
        except Exception as e:
            print(f"  Unexpected error during RedisJSON storage part: {e}")

        # Step 4: Cleanup the test stream itself.
        # In a real application, streams might be persistent or managed by size (MAXLEN).
        # For this demonstration, we delete the stream to keep the Redis instance clean.
        if redis_conn.exists(stream_name):
            redis_conn.delete(stream_name)
            print(f"  Cleanup: Test stream '{stream_name}' deleted.")
    else:
        print("  Message publishing to stream failed. Skipping RedisJSON storage.")
    print("--- Legacy Message Sending Simulation Complete ---")


def manage_group_chat_membership_flow(redis_conn):
    """
    Demonstrates managing user membership in a specific group chat using Redis Sets.

    This flow covers:
    1. Users joining a group (`redis_utils.join_group_chat`).
    2. Listing group members (`redis_utils.get_group_chat_members`).
    3. A user leaving the group (`redis_utils.leave_group_chat`).
    4. Listing members again to show the update.
    It also performs cleanup of the group membership data.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate group membership: Redis connection not available.")
        return

    sample_group_name = "Local Gardening Club" # Example group name
    user_alpha = "GardenerAlpha"
    user_beta = "PlanterBeta"

    print(f"\n--- Simulating Group Chat Membership for Group: '{sample_group_name}' ---")

    # Step 1: Users join the group.
    # `join_group_chat` uses SADD on a key like "group_members:local_gardening_club".
    # The group name is sanitized by `redis_utils.get_group_chat_stream_name` for key generation.
    redis_utils.join_group_chat(redis_conn, sample_group_name, user_alpha)
    redis_utils.join_group_chat(redis_conn, sample_group_name, user_beta)
    print(f"  Action: Users '{user_alpha}' and '{user_beta}' attempted to join group '{sample_group_name}'.")

    # Step 2: List members of the group.
    # `get_group_chat_members` uses SMEMBERS.
    members = redis_utils.get_group_chat_members(redis_conn, sample_group_name)
    print(f"  Current members in '{sample_group_name}': {members}")

    # Step 3: A user leaves the group.
    # `leave_group_chat` uses SREM.
    redis_utils.leave_group_chat(redis_conn, sample_group_name, user_alpha)
    print(f"  Action: User '{user_alpha}' attempted to leave group '{sample_group_name}'.")

    # Step 4: List members again to see the change.
    members_after_leave = redis_utils.get_group_chat_members(redis_conn, sample_group_name)
    print(f"  Members in '{sample_group_name}' after user left: {members_after_leave}")

    # Step 5: Cleanup. Remove the remaining user and thus the group members Set.
    # (Redis auto-deletes Set keys when they become empty).
    redis_utils.leave_group_chat(redis_conn, sample_group_name, user_beta)
    # For explicit cleanup, one could construct the key and delete if exists:
    sanitized_group_name_part = redis_utils.get_group_chat_stream_name(sample_group_name).split(':', 1)[-1]
    group_members_key = f"group_members:{sanitized_group_name_part}"
    if redis_conn.exists(group_members_key): # Should be empty and possibly auto-deleted now
        redis_conn.delete(group_members_key)
        print(f"  Cleanup: Group members set for '{sample_group_name}' (key: {group_members_key}) ensured deleted.")
    else:
        print(f"  Cleanup: Group members set for '{sample_group_name}' (key: {group_members_key}) was already removed (likely empty).")
    print("--- Group Membership Simulation Complete ---")


def send_chat_message_flow_differentiated(redis_conn):
    """
    Demonstrates sending messages to both a 1-to-1 chat and a group chat.
    This flow uses the standardized stream naming conventions:
    - 1-to-1 chats: `chat:userA_userB` (via `get_1_to_1_chat_stream_name`)
    - Group chats: `group_chat:group_name` (via `get_group_chat_stream_name`)
    Messages are published to streams and also stored in RedisJSON. Test data is cleaned up.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate differentiated message sending: Redis connection not available.")
        return

    sender_user_id = "UserSenderDemo" # Example sender
    print(f"\n--- Simulating Differentiated Message Sending (Sender: {sender_user_id}) ---")

    # Part 1: Simulate sending a message to a 1-to-1 chat.
    recipient_user_id = "UserReceiverDemo"
    # Generate the unique stream name for this 1-to-1 chat.
    one_to_one_stream_name = redis_utils.get_1_to_1_chat_stream_name(sender_user_id, recipient_user_id)
    one_to_one_message = f"Hello {recipient_user_id}, this is a private message from {sender_user_id} to your 1-to-1 chat."

    print(f"\n  Action: Sending 1-to-1 message to stream: '{one_to_one_stream_name}'...")
    # Publish to the 1-to-1 stream.
    msg_id_1to1 = redis_utils.publish_to_stream(redis_conn, one_to_one_stream_name, sender_user_id, one_to_one_message)
    if msg_id_1to1:
        print(f"  Outcome: 1-to-1 message sent. Stream: '{one_to_one_stream_name}', Message ID: {msg_id_1to1}.")
        # Store the message in RedisJSON for persistence.
        # The `chat_id` for RedisJSON is the stream name itself.
        message_data_1to1 = {'user_id': sender_user_id, 'message': one_to_one_message,
                             'timestamp': datetime.datetime.utcnow().isoformat()}
        redis_utils.add_chat_message_redisjson(redis_conn, one_to_one_stream_name, msg_id_1to1, message_data_1to1)

        # Cleanup for this demonstration: remove the RedisJSON document and the test stream.
        # In a real app, data would persist or be managed by TTL / archival policies.
        redis_json_key_1to1 = f"{redis_utils.CHAT_MESSAGE_PREFIX}{one_to_one_stream_name}:{msg_id_1to1}"
        redis_conn.delete(redis_json_key_1to1)
        redis_conn.delete(one_to_one_stream_name) # Delete the entire stream for this test.
        print(f"  Cleanup: Test JSON doc '{redis_json_key_1to1}' and stream '{one_to_one_stream_name}' deleted.")
    else:
        print(f"  Outcome: Failed to send 1-to-1 message to stream '{one_to_one_stream_name}'.")


    # Part 2: Simulate sending a message to a group chat.
    group_name = "Global Tech Innovators"
    # Generate the sanitized stream name for this group chat.
    group_chat_stream_name = redis_utils.get_group_chat_stream_name(group_name)
    group_message = f"Hello members of '{group_name}', {sender_user_id} has an important update for the group!"

    print(f"\n  Action: Sending group message to stream: '{group_chat_stream_name}'...")
    # Publish to the group chat stream.
    msg_id_group = redis_utils.publish_to_stream(redis_conn, group_chat_stream_name, sender_user_id, group_message)
    if msg_id_group:
        print(f"  Outcome: Group message sent. Stream: '{group_chat_stream_name}', Message ID: {msg_id_group}.")
        # Store in RedisJSON.
        message_data_group = {'user_id': sender_user_id, 'message': group_message,
                              'timestamp': datetime.datetime.utcnow().isoformat()}
        redis_utils.add_chat_message_redisjson(redis_conn, group_chat_stream_name, msg_id_group, message_data_group)

        # Cleanup for this demonstration.
        redis_json_key_group = f"{redis_utils.CHAT_MESSAGE_PREFIX}{group_chat_stream_name}:{msg_id_group}"
        redis_conn.delete(redis_json_key_group)
        redis_conn.delete(group_chat_stream_name)
        print(f"  Cleanup: Test JSON doc '{redis_json_key_group}' and stream '{group_chat_stream_name}' deleted.")
    else:
        print(f"  Outcome: Failed to send group message to stream '{group_chat_stream_name}'.")

    print("\n--- Differentiated Message Sending Simulation Complete ---")


def search_messages_flow(redis_conn):
    """
    Demonstrates searching for chat messages using RediSearch.

    This flow will:
    1. Ensure the RediSearch index for chat messages is created (`create_chat_message_index`).
    2. Prompt the user for a search query.
    3. Execute the search using `redis_utils.search_chat_messages`.
    4. Display the results.
    Relies on messages being stored in RedisJSON with keys prefixed by `CHAT_MESSAGE_PREFIX`
    and matching the schema defined in `create_chat_message_index`.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate message search: Redis connection not available.")
        return

    print(f"\n--- Chat Message Search (using RediSearch) ---")

    # Step 1: Ensure the search index exists.
    # `create_chat_message_index` will create it if not present, or confirm if it exists.
    print("  Action: Ensuring chat message search index exists...")
    if not redis_utils.create_chat_message_index(redis_conn):
        print("  Outcome: Failed to create or verify RediSearch index. Aborting search flow.")
        print("           Please ensure Redis server has the RediSearch (FT) module enabled.")
        return
    print("  Outcome: Search index is ready.")

    # Step 2: (Optional) Add sample messages for searching.
    # For a robust demo, messages should ideally be present from other application flows
    # or from the `redis_utils.py` test block if run prior.
    # This section can be enabled to ensure some searchable data if the DB is empty.
    # existing_message_keys = redis_conn.keys(f"{redis_utils.CHAT_MESSAGE_PREFIX}*")
    # if not existing_message_keys:
    #     print("  Action: No existing messages found. Adding a sample message for search demonstration...")
    #     sample_chat_id = redis_utils.get_group_chat_stream_name("Search Demo Data Group")
    #     sample_msg_id = "search_demo_msg_001"
    #     sample_data = {
    #         'user_id': 'SearchDemoUser',
    #         'message': 'This is a sample message for RediSearch about Python, Redis, and real-time chat applications.',
    #         'timestamp': datetime.datetime.utcnow().isoformat()
    #     }
    #     redis_utils.add_chat_message_redisjson(redis_conn, sample_chat_id, sample_msg_id, sample_data)
    #     import time
    #     time.sleep(0.1) # Brief pause to allow indexing.
    #     print(f"  Outcome: Sample message added with key '{redis_utils.CHAT_MESSAGE_PREFIX}{sample_chat_id}:{sample_msg_id}'.")
    # else:
    #     print(f"  Info: Found {len(existing_message_keys)} existing chat message document(s).")


    # Step 3: Prompt the user for a search query.
    print("\n  Search Query Examples:")
    print("    - To find messages from 'UserSenderDemo': @user_id:UserSenderDemo")
    print("    - To find messages containing 'private' (in the message text): private")
    print("    - To find messages from 'UserSenderDemo' containing 'private': @user_id:UserSenderDemo private")
    print("    - To find messages with the exact phrase 'important update': @message:'important update'")

    try:
        query_string = input("  Enter search query (or press Enter to skip search demonstration): ").strip()
        if not query_string:
            print("  Search demonstration skipped by user.")
            return
    except EOFError: # Handles environments where stdin might not be available (e.g., automated tests).
        print("  No input for search query (EOF detected). Skipping search demonstration.")
        return


    # Step 4: Call the search function from redis_utils.
    print(f"  Action: Searching for: '{query_string}'...")
    results = redis_utils.search_chat_messages(redis_conn, query_string)

    # Step 5: Print the search results.
    if results:
        print(f"\n  Search Results ({len(results)} found):")
        for i, doc_data in enumerate(results):
            # `doc_data` is a dictionary returned by `search_chat_messages`,
            # containing fields like 'id' (Redis key), 'user_id', 'message', 'timestamp'.
            print(f"\n    Result {i+1}:")
            print(f"      ID (Redis Key): {doc_data.get('id')}")
            print(f"      User ID:        {doc_data.get('user_id', 'N/A')}")
            print(f"      Message:        {doc_data.get('message', 'N/A')}")
            print(f"      Timestamp:      {doc_data.get('timestamp', 'N/A')}")
    else:
        print("  No messages found matching your query.")

    print("\n--- Chat Message Search Simulation Complete ---")


def process_message(message_detail):
    """
    Callback function to process and print a single message received from a Redis Stream.

    This function is passed to `redis_utils.subscribe_to_stream`. It's invoked
    for each message received from the subscribed stream.

    Args:
        message_detail (dict): A dictionary where the key is the message ID (str)
                               and the value is another dictionary containing the
                               message fields (e.g., `{'user_id': ..., 'message': ..., 'timestamp': ...}`).
                               Example: `{'1678886400000-0': {'user_id': 'Alice', ...}}`
    """
    # `message_detail` is expected to be a dictionary with a single key (the message ID)
    # and its value being the message data dictionary.
    for message_id, message_data in message_detail.items():
        # Extract fields from message_data, providing defaults if fields are missing.
        user_id = message_data.get('user_id', 'Unknown User')
        text = message_data.get('message', 'No message text')
        timestamp = message_data.get('timestamp', 'No timestamp')

        # Ensure values are strings for printing, as Redis might return bytes
        # if `decode_responses=False` was used at connection (though our `connect_to_redis` sets it to True).
        # This is a defensive measure.
        user_id_str = user_id if isinstance(user_id, str) else user_id.decode('utf-8')
        text_str = text if isinstance(text, str) else text.decode('utf-8')
        timestamp_str = timestamp if isinstance(timestamp, str) else timestamp.decode('utf-8')

        # Print the formatted message to the console.
        print(f"\n[NEW MESSAGE RECEIVED - Stream Msg ID: {message_id}]")
        print(f"  Timestamp: {timestamp_str}")
        print(f"  From User: {user_id_str}")
        print(f"  Message:   {text_str}")
        print("-" * 40) # Separator for readability

def receive_chat_messages_flow_differentiated(redis_conn):
    """
    Demonstrates receiving messages from different types of chat streams (1-to-1 and group).

    For demonstration purposes, this function will:
    1. Define a 1-to-1 chat stream and a group chat stream.
    2. Publish a test message to each of these streams.
    3. Subscribe to each stream sequentially to receive the test message.
    In a real application, message publishing and subscribing would be independent and often
    concurrent operations, with users choosing which chat to listen to.
    The subscription uses `redis_utils.subscribe_to_stream` with a timeout.

    Args:
        redis_conn (redis.Redis): An active Redis connection object.
    """
    if not redis_conn:
        print("Cannot demonstrate message reception: Redis connection not available.")
        return

    print("\n--- Simulating Differentiated Message Reception ---")
    print("Note: This flow will self-publish test messages to streams and then listen.")

    # Part 1: Simulate listening to a 1-to-1 chat stream.
    listener_user_id = "UserListenerDemo"
    sender_user_id_1to1 = "UserSenderDemo" # Should match a sender from a sending flow for interactive tests.
    # Generate the 1-to-1 chat stream name.
    one_to_one_listen_stream = redis_utils.get_1_to_1_chat_stream_name(listener_user_id, sender_user_id_1to1)

    print(f"\n  Attempting to listen to 1-to-1 chat stream: '{one_to_one_listen_stream}'...")
    print(f"  (Will listen for new messages for a short period, e.g., 5 seconds, or until Ctrl+C)")

    # Publish a test message to this stream so the subscriber can receive something.
    # In a real scenario, `sender_user_id_1to1` would send this message independently.
    redis_utils.publish_to_stream(redis_conn, one_to_one_listen_stream, sender_user_id_1to1,
                                  f"Test direct message from {sender_user_id_1to1} to {listener_user_id}.")

    # Subscribe to the 1-to-1 stream. `block_ms` provides a timeout for the blocking read.
    redis_utils.subscribe_to_stream(redis_conn, one_to_one_listen_stream, process_message,
                                    last_id='$', block_ms=5000) # Listen for new messages for up to 5s.
    print(f"  --- Subscription to 1-to-1 stream '{one_to_one_listen_stream}' ended. ---")
    # Cleanup: Delete the test stream used for this part of the demonstration.
    redis_conn.delete(one_to_one_listen_stream)
    print(f"  Cleanup: Test 1-to-1 stream '{one_to_one_listen_stream}' deleted.")


    # Part 2: Simulate listening to a group chat stream.
    group_listen_name = "Global Tech Innovators" # Should match a group name used in sending flow for interactive tests.
    # Generate the group chat stream name.
    group_listen_stream = redis_utils.get_group_chat_stream_name(group_listen_name)

    print(f"\n  Attempting to listen to group chat stream: '{group_listen_stream}'...")
    print(f"  (Will listen for new messages for a short period, e.g., 5 seconds, or until Ctrl+C)")

    # Publish a test message to this group stream.
    redis_utils.publish_to_stream(redis_conn, group_listen_stream, "GroupUpdateService",
                                  f"Important system notification for group '{group_listen_name}'!")

    redis_utils.subscribe_to_stream(redis_conn, group_listen_stream, process_message,
                                    last_id='$', block_ms=5000) # Listen for new messages for up to 5s.
    print(f"  --- Subscription to group stream '{group_listen_stream}' ended. ---")
    # Cleanup: Delete the test group stream.
    redis_conn.delete(group_listen_stream)
    print(f"  Cleanup: Test group stream '{group_listen_stream}' deleted.")
    print("\n--- Differentiated Message Reception Simulation Complete ---")


def main():
    """
    Main function to run the Python Chat application demonstrations.

    This function connects to Redis and then executes various "flow" functions
    to demonstrate the implemented chat functionalities. Each flow focuses on a
    specific aspect like user management, message sending/receiving, group management,
    or message searching.

    The flows are designed to be largely self-contained for demonstration, including
    setup and cleanup of test data where appropriate.
    """
    print("Python Chat Application - Demo Start")

    # Establish a connection to Redis.
    # All subsequent operations will use this connection object `r_conn`.
    r_conn = redis_utils.connect_to_redis()

    if r_conn:
        print("\n[INFO] Successfully connected to Redis. Starting demonstration flows...")

        # --- Demonstrate Generic User Management (Rooms) ---
        # manage_user_flow(r_conn) # Manages users in generic rooms like "general_lobby"

        # --- Demonstrate Group Chat Membership ---
        # manage_group_chat_membership_flow(r_conn) # Manages users in named groups like "Gardening Club"

        # --- Demonstrate Sending Messages (1-to-1 and Group) ---
        # This flow uses the new stream naming conventions.
        # send_chat_message_flow_differentiated(r_conn)

        # --- Demonstrate Receiving Messages (1-to-1 and Group) ---
        # This flow self-publishes messages for demonstration before listening.
        # In a real app, sender and receiver would be independent.
        # print("\n[INFO] The 'receive_chat_messages_flow_differentiated' will now run. "
        #       "It self-publishes messages to test streams and then listens. "
        #       "For interactive testing, run a sender (this script in send mode, or redis-cli) "
        #       "against the streams it listens to in a separate terminal.")
        # receive_chat_messages_flow_differentiated(r_conn)

        # --- Demonstrate Chat Message Search using RediSearch ---
        # This flow ensures the index is created and allows user to input search queries.
        search_messages_flow(r_conn)

        # --- Legacy Message Sending Flow (Optional) ---
        # send_chat_message_flow(r_conn) # Uses a generic stream name "main_chat_stream_legacy"

        print("\n[INFO] All selected demonstration flows are complete.")
    else:
        print("[ERROR] Failed to connect to Redis. Application cannot proceed with demonstrations.")

    print("\nPython Chat Application - Demo End")

if __name__ == "__main__":
    # This block ensures that main() is called only when the script is executed directly.
    main()
