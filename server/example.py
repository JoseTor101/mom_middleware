from mom_instance import MOMInstance
from round_robin_dispatcher import RoundRobinDispatcher

if __name__ == "__main__":
    # Create MOM instances
    mom1 = MOMInstance("MOM1")
    mom2 = MOMInstance("MOM2")
    mom3 = MOMInstance("MOM3")

    # Create a dispatcher
    dispatcher = RoundRobinDispatcher([mom1, mom2, mom3])

    # Create topics
    mom1.registry.create_topic("topic1")
    mom1.registry.create_topic("topic2")

    # Send messages
    dispatcher.send_message("topic1", "Message 1")
    dispatcher.send_message("topic2", "Message 2")
    dispatcher.send_message("topic1", "Message 3")

    # Start processing messages
    mom1.process_messages("topic1")
    mom2.process_messages("topic2")


