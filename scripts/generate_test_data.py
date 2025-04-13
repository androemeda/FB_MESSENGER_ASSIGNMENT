"""
Script to generate test data for the Messenger application.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    """
    logger.info("Generating test data...")
    
    # Create user IDs (1 to NUM_USERS)
    user_ids = list(range(1, NUM_USERS + 1))
    
    # Create conversations between random pairs of users
    conversations = []
    for i in range(NUM_CONVERSATIONS):
        # Select two random users
        user1, user2 = random.sample(user_ids, 2)
        
        # Create a conversation ID (timestamp-based for simplicity)
        conversation_id = int(datetime.now().timestamp()) + i
        created_at = datetime.now() - timedelta(days=random.randint(1, 30))
        
        # Store conversation
        conversations.append({
            "id": conversation_id,
            "user1_id": min(user1, user2),  # Store in consistent order
            "user2_id": max(user1, user2),
            "created_at": created_at
        })
        
        # Insert into conversation_participants
        session.execute(
            """
            INSERT INTO conversation_participants (
                conversation_id, user1_id, user2_id, created_at
            ) VALUES (%s, %s, %s, %s)
            """,
            (conversation_id, min(user1, user2), max(user1, user2), created_at)
        )
        
        # Generate messages for this conversation
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        last_message_timestamp = None
        last_message_content = None
        
        for j in range(num_messages):
            # Decide sender and receiver
            if random.random() < 0.5:
                sender_id, receiver_id = user1, user2
            else:
                sender_id, receiver_id = user2, user1
            
            # Create a message
            message_id = uuid.uuid4()
            message_timestamp = created_at + timedelta(
                hours=random.randint(1, 72),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            content = f"Test message {j + 1} from User {sender_id} to User {receiver_id}"
            
            # Track the last message
            if last_message_timestamp is None or message_timestamp > last_message_timestamp:
                last_message_timestamp = message_timestamp
                last_message_content = content
            
            # Insert into messages_by_conversation
            session.execute(
                """
                INSERT INTO messages_by_conversation (
                    conversation_id, message_timestamp, message_id, 
                    sender_id, receiver_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, message_timestamp, message_id, sender_id, receiver_id, content)
            )
        
        # Update conversations_by_user for both users
        if last_message_timestamp:
            # For user1
            session.execute(
                """
                INSERT INTO conversations_by_user (
                    user_id, last_message_timestamp, conversation_id, 
                    other_user_id, last_message_content
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (user1, last_message_timestamp, conversation_id, user2, last_message_content)
            )
            
            # For user2
            session.execute(
                """
                INSERT INTO conversations_by_user (
                    user_id, last_message_timestamp, conversation_id, 
                    other_user_id, last_message_content
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (user2, last_message_timestamp, conversation_id, user1, last_message_content)
            )
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()