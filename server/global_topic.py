import redis

from .state_manager import StateManager


class GlobalTopicRegistry:
    def __init__(self, redis_host="localhost", redis_port=6379):
        """Initialize the global topic registry and restore state if needed. """
        self.redis = redis.StrictRedis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self.state_manager = StateManager()
        # Restore state from json file if it exists
        self.state_manager.restore_state(self.redis)

    def create_topic(self, topic_name, num_partitions=3):
        """Create a new topic with partitions."""
        if not self.redis.exists(topic_name):
            self.redis.sadd("topics", topic_name)
            for partition in range(num_partitions):
                partition_key = f"{topic_name}:partition{partition}"
                self.redis.delete(partition_key)  # Limpiar si ya existe
            self.state_manager.add_topic(topic_name, num_partitions)
            print(
                f"Topic '{topic_name}' created with {num_partitions} partitions.")
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
            self.state_manager.delete_topic(topic_name)
            print(f"Topic '{topic_name}' and its partitions deleted.")
        else:
            print(f"Topic '{topic_name}' does not exist.")

    def enqueue_message(self, topic_name, message):
        """Enqueue a message to a topic's partition."""
        partitions = self.redis.keys(f"{topic_name}:partition*")
        if partitions:
            partition = partitions[hash(message) % len(partitions)]
            self.redis.rpush(partition, message)
            print(f"Message enqueued to {partition}: {message}")
        else:
            print(f"Topic '{topic_name}' does not exist.")

    def dequeue_message(self, topic_name, partition):
        """Dequeue a message from a topic's partition."""
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
        """ Obtain the number of partitions for a topic. """
        partitions = self.redis.keys(f"{topic_name}:partition*")
        return len(partitions)

    def get_partition_stats(self, topic_name):
        """Get statistics about the partitions of a topic. """
        partition_stats = {}
        partitions = self.redis.keys(f"{topic_name}:partition*")
        for partition in partitions:
            partition_id = partition.split("partition")[1]
            message_count = self.redis.llen(partition)
            partition_stats[partition_id] = message_count
        return partition_stats

    def get_message_from_partition(self, topic_name, partition_id):
        """Obtain a message from a specific partition."""
        partition_key = f"{topic_name}:partition{partition_id}"
        message = self.redis.lpop(partition_key)
        return message
