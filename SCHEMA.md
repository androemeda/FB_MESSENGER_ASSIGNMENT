## 1. messages_by_conversation
```
messages_by_conversation (
    conversation_id int,
    message_timestamp timestamp,
    message_id uuid,
    sender_id int,
    receiver_id int,
    content text,
    PRIMARY KEY ((conversation_id), message_timestamp, message_id)
) WITH CLUSTERING ORDER BY (message_timestamp DESC, message_id ASC);
```

##### Partition Key: conversation_id
##### Clustering Keys: message_timestamp, message_id



## 2. conversations_by_user
```
conversations_by_user (
    user_id int,
    last_message_timestamp timestamp,
    conversation_id int,
    other_user_id int,
    last_message_content text,
    PRIMARY KEY ((user_id), last_message_timestamp, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_timestamp DESC, conversation_id ASC);
```


##### Partition Key: user_id
##### Clustering Keys: last_message_timestamp, conversation_id



## 3. conversation_participants
```
conversation_participants (
    conversation_id int,
    user1_id int,
    user2_id int,
    created_at timestamp,
    PRIMARY KEY (conversation_id)
);
```

##### Partition Key: conversation_id