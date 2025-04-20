import redis

from .state_manager import StateManager


class GlobalTopicRegistry:
    def __init__(self, redis_host="localhost", redis_port=6379):
        """Initialize the global topic registry and restore state if needed. """
        self.redis = redis.StrictRedis(
            host=redis_host, port=redis_port, decode_responses=True
        )
        self.state_manager = StateManager()

        # Intentamos restaurar el estado desde el archivo JSON
        self.state_manager.restore_state(self.redis)

    def create_topic(self, topic_name, num_partitions=3):
        if not self.redis.exists(topic_name):
            self.redis.sadd("topics", topic_name)
            for partition in range(num_partitions):
                partition_key = f"{topic_name}:partition{partition}"
                # Instead of creating and immediately emptying,
                # use Redis SET to ensure the key exists
                self.redis.set(f"{topic_name}:partition_exists:{partition}", "1")
                # Initialize the partition key as an empty list
                # We need to make sure this key exists even if empty
                self.redis.delete(partition_key)
                # We need to ensure the partition key exists even if it's empty
                self.redis.rpush(partition_key, "__init__")
                self.redis.ltrim(partition_key, 1, 0)  # Remove the initialization message
                
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
        """Agregar un mensaje a una partición del tópico."""
        # First check if topic exists
        if not self.redis.sismember("topics", topic_name):
            print(f"Topic '{topic_name}' does not exist.")
            return
            
        # Count partitions based on marker keys
        partition_markers = self.redis.keys(f"{topic_name}:partition_exists:*")
        
        if partition_markers:
            # Get partition number
            partition_num = hash(message) % len(partition_markers)
            partition_key = f"{topic_name}:partition{partition_num}"
            self.redis.rpush(partition_key, message)
            print(f"Message enqueued to {partition_key}: {message}")
        else:
            print(f"Topic '{topic_name}' exists but has no partitions.")

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
            # Skip the existence markers
            if "exists" in partition:
                continue
            partition_id = partition.split("partition")[1]
            message_count = self.redis.llen(partition)
            partition_stats[partition_id] = message_count
        return partition_stats

    def get_message_from_partition(self, topic_name, partition_id):
        """Obtain a message from a specific partition."""
        partition_key = f"{topic_name}:partition{partition_id}"
        message = self.redis.lpop(partition_key)
        return message


    def get_all_messages_from_topic(self, topic_name):
        """Obtener todos los mensajes de todas las particiones de un tópico sin eliminarlos."""
        all_messages = []
        
        # Check if topic exists
        if not self.redis.sismember("topics", topic_name):
            print(f"Topic '{topic_name}' does not exist.")
            return all_messages
        
        # First, get all actual partition keys that match our pattern
        actual_partition_keys = self.redis.keys(f"{topic_name}:partition[0-9]*")
        print(f"DEBUG: Actual partition keys found: {actual_partition_keys}")
        
        if not actual_partition_keys:
            print(f"Topic '{topic_name}' has no partition data.")
            return all_messages
        
        # Directly use the actual keys we found
        for partition_key in actual_partition_keys:
            try:
                # Get all messages from the partition without removing them
                partition_messages = self.redis.lrange(partition_key, 0, -1)
                print(f"DEBUG: Messages from {partition_key}: {partition_messages}")
                
                if partition_messages:
                    all_messages.extend(partition_messages)
            except Exception as e:
                print(f"Error retrieving messages from partition '{partition_key}': {e}")
        
        return all_messages