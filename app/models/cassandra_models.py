"""
Models for interacting with Cassandra tables.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import get_cassandra_client

cassandra_client = get_cassandra_client()

class MessageModel:
    """
    Message model for interacting with the messages table.
    """

    @staticmethod
    async def create_message(
        sender_id: int,
        receiver_id: int,
        content: str,
        conversation_id: int
    ) -> Dict[str, Any]:
        message_id = uuid.uuid4()
        message_timestamp = datetime.now()

        query = """
        INSERT INTO messages_by_conversation (
            conversation_id, message_timestamp, message_id, 
            sender_id, receiver_id, content
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            conversation_id, 
            message_timestamp, 
            message_id, 
            sender_id, 
            receiver_id, 
            content
        )
        cassandra_client.execute(query, params)

        update_conversation_query = """
        INSERT INTO conversations_by_user (
            user_id, last_message_timestamp, conversation_id, 
            other_user_id, last_message_content
        ) VALUES (%s, %s, %s, %s, %s)
        """
        sender_params = (
            sender_id,
            message_timestamp,
            conversation_id,
            receiver_id,
            content
        )
        cassandra_client.execute(update_conversation_query, sender_params)

        receiver_params = (
            receiver_id,
            message_timestamp,
            conversation_id,
            sender_id,
            content
        )
        cassandra_client.execute(update_conversation_query, receiver_params)

        return {
            "id": str(message_id),
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "created_at": message_timestamp
        }

    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        fetch_limit = page * limit

        query = """
        SELECT message_id, conversation_id, sender_id, receiver_id, 
               content, message_timestamp
        FROM messages_by_conversation
        WHERE conversation_id = %s
        LIMIT %s
        """
        params = (conversation_id, fetch_limit)
        rows = cassandra_client.execute(query, params)

        messages = []
        for row in rows:
            messages.append({
                "id": str(row["message_id"]),
                "conversation_id": row["conversation_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["message_timestamp"]
            })

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paged_messages = messages[start_idx:end_idx]

        return {
            "total": len(messages),
            "page": page,
            "limit": limit,
            "data": paged_messages
        }

    @staticmethod
    async def get_messages_before_timestamp(conversation_id: int, before_timestamp: datetime, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        fetch_limit = page * limit

        query = """
        SELECT message_id, conversation_id, sender_id, receiver_id, 
               content, message_timestamp
        FROM messages_by_conversation
        WHERE conversation_id = %s
        AND message_timestamp < %s
        LIMIT %s
        """
        params = (conversation_id, before_timestamp, fetch_limit)
        rows = cassandra_client.execute(query, params)

        messages = []
        for row in rows:
            messages.append({
                "id": str(row["message_id"]),
                "conversation_id": row["conversation_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["message_timestamp"]
            })

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paged_messages = messages[start_idx:end_idx]

        count_query = """
        SELECT COUNT(*) as count
        FROM messages_by_conversation
        WHERE conversation_id = %s
        AND message_timestamp < %s
        """
        count_params = (conversation_id, before_timestamp)
        count_result = cassandra_client.execute(count_query, count_params)
        total = count_result[0]["count"] if count_result else 0

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": paged_messages
        }


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """

    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Dict[str, Any]:
        fetch_limit = page * limit

        query = """
        SELECT user_id, last_message_timestamp, conversation_id, 
               other_user_id, last_message_content
        FROM conversations_by_user
        WHERE user_id = %s
        LIMIT %s
        """
        params = (user_id, fetch_limit)
        rows = cassandra_client.execute(query, params)

        conversations = []
        for row in rows:
            conversation_id = row["conversation_id"]

            participants_query = """
            SELECT user1_id, user2_id, created_at
            FROM conversation_participants
            WHERE conversation_id = %s
            """
            participants_params = (conversation_id,)
            participants_result = cassandra_client.execute(participants_query, participants_params)

            if participants_result:
                participant = participants_result[0]
                conversations.append({
                    "id": conversation_id,
                    "user1_id": participant["user1_id"],
                    "user2_id": participant["user2_id"],
                    "last_message_at": row["last_message_timestamp"],
                    "last_message_content": row["last_message_content"]
                })

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paged_conversations = conversations[start_idx:end_idx]

        return {
            "total": len(conversations),
            "page": page,
            "limit": limit,
            "data": paged_conversations
        }

    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        query = """
        SELECT conversation_id, user1_id, user2_id, created_at
        FROM conversation_participants
        WHERE conversation_id = %s
        """
        params = (conversation_id,)
        rows = cassandra_client.execute(query, params)

        if not rows:
            return None

        last_message_query = """
        SELECT message_timestamp, content
        FROM messages_by_conversation
        WHERE conversation_id = %s
        LIMIT 1
        """
        last_message_params = (conversation_id,)
        last_message_results = cassandra_client.execute(last_message_query, last_message_params)

        conversation = rows[0]

        return {
            "id": conversation["conversation_id"],
            "user1_id": conversation["user1_id"],
            "user2_id": conversation["user2_id"],
            "last_message_at": last_message_results[0]["message_timestamp"] if last_message_results else conversation["created_at"],
            "last_message_content": last_message_results[0]["content"] if last_message_results else None
        }

    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        min_user_id = min(user1_id, user2_id)
        max_user_id = max(user1_id, user2_id)

        conversations_query = """
        SELECT conversation_id
        FROM conversations_by_user
        WHERE user_id = %s
        """
        conversations_params = (min_user_id,)
        conversations = cassandra_client.execute(conversations_query, conversations_params)

        for conversation in conversations:
            conversation_id = conversation["conversation_id"]

            participants_query = """
            SELECT user1_id, user2_id
            FROM conversation_participants
            WHERE conversation_id = %s
            """
            participants_params = (conversation_id,)
            participants = cassandra_client.execute(participants_query, participants_params)

            if participants:
                participant = participants[0]
                if ((participant["user1_id"] == min_user_id and participant["user2_id"] == max_user_id) or
                    (participant["user1_id"] == max_user_id and participant["user2_id"] == min_user_id)):
                    return await ConversationModel.get_conversation(conversation_id)

        conversation_id = int(datetime.now().timestamp())
        created_at = datetime.now()

        create_query = """
        INSERT INTO conversation_participants (
            conversation_id, user1_id, user2_id, created_at
        ) VALUES (%s, %s, %s, %s)
        """
        create_params = (
            conversation_id,
            min_user_id,
            max_user_id,
            created_at
        )
        cassandra_client.execute(create_query, create_params)

        init_conversation_query = """
        INSERT INTO conversations_by_user (
            user_id, last_message_timestamp, conversation_id, 
            other_user_id, last_message_content
        ) VALUES (%s, %s, %s, %s, %s)
        """
        user1_params = (
            min_user_id,
            created_at,
            conversation_id,
            max_user_id,
            None
        )
        cassandra_client.execute(init_conversation_query, user1_params)

        user2_params = (
            max_user_id,
            created_at,
            conversation_id,
            min_user_id,
            None
        )
        cassandra_client.execute(init_conversation_query, user2_params)

        return {
            "id": conversation_id,
            "user1_id": min_user_id,
            "user2_id": max_user_id,
            "last_message_at": created_at,
            "last_message_content": None
        }
