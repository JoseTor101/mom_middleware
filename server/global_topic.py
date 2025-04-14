import redis

class GlobalTopicRegistry:
    def __init__(self, redis_host='localhost', redis_port=6379):
        """Initialize the global topic registry."""
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    def create_topic(self, topic_name, num_partitions=3):
        """Create a new topic with partitions."""
        if not self.redis.exists(topic_name):
            self.redis.sadd("topics", topic_name)
            for partition in range(num_partitions):
                partition_key = f"{topic_name}:partition{partition}"
                self.redis.lpush(partition_key, "")  # Create an empty queue for each partition
            print(f"Topic '{topic_name}' created with {num_partitions} partitions.")
        else:
            print(f"Topic '{topic_name}' already exists.")
    
    def list_topics(self):
        """List all topics."""
        topics = self.redis.smembers("topics")
        return list(topics)

    def delete_topic(self, topic_name):
        """Delete a topic and its partitions."""
        if self.redis.exists(topic_name):
            self.redis.srem("topics", topic_name)
            for key in self.redis.keys(f"{topic_name}:partition*"):
                self.redis.delete(key)
            print(f"Topic '{topic_name}' and its partitions deleted.")
        else:
            print(f"Topic '{topic_name}' does not exist.")

    def enqueue_message(self, topic_name, message):
        """Enqueue a message to a partition of the topic."""
        partitions = self.redis.keys(f"{topic_name}:partition*")
        if partitions:
            # Use round-robin to select a partition
            partition = partitions[hash(message) % len(partitions)]
            self.redis.rpush(partition, message)
            print(f"Message enqueued to {partition}: {message}")
        else:
            print(f"Topic '{topic_name}' does not exist.")

    def dequeue_message(self, topic_name, partition):
        """Dequeue a message from a specific partition."""
        partition_key = f"{topic_name}:partition{partition}"
        if self.redis.exists(partition_key):
            message = self.redis.lpop(partition_key)
            if message:
                print(f"Message dequeued from {partition_key}: {message}")
                return message
            else:
                print(f"No messages in {partition_key}.")
                return None
        else:
            print(f"Partition '{partition_key}' does not exist.")
            return None
        
    def get_partition_count(self, topic_name):
        """Get the number of partitions for a topic."""
        partitions = self.redis.keys(f"{topic_name}:partition*")
        return len(partitions)

    def get_partition_stats(self, topic_name):
        """Get statistics about partitions for a topic."""
        partition_stats = {}
        partitions = self.redis.keys(f"{topic_name}:partition*")
        for partition in partitions:
            partition_id = partition.split('partition')[1]
            message_count = self.redis.llen(partition)
            partition_stats[partition_id] = message_count
        return partition_stats

    def get_message_from_partition(self, topic_name, partition_id):
        """Get a message from a specific partition."""
        partition_key = f"{topic_name}:partition{partition_id}"
        message = self.redis.lpop(partition_key)
        return message