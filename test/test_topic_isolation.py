#!/usr/bin/env python3

import json
import os
import sys
import time
import random
import requests
import threading

# Add parent directory to import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TopicIsolationTest:
    """Test class for verifying topic isolation in MOM Middleware"""

    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.token = None
        self.username = "testuser"
        self.password = "testpass"
        # Use unique topic names for the test
        self.topic1 = f"isolation_topic1_{int(time.time())}"
        self.topic2 = f"isolation_topic2_{int(time.time())}"
        self.message_count = 20
        print(f"MOM Middleware Topic Isolation Test")
        print(f"API URL: {self.base_url}")
        print(f"Testing with topics: {self.topic1}, {self.topic2}")

    def setup(self):
        """Set up authentication and create topics"""
        # Signup
        response = requests.post(
            f"{self.base_url}/signup",
            data={"username": self.username, "password": self.password}
        )
        print(f"Signup: {response.status_code}")

        # Login
        response = requests.post(
            f"{self.base_url}/login",
            data={"username": self.username, "password": self.password}
        )
        
        if response.status_code == 200:
            data = response.json()
            self.token = data.get("access_token")
            print(f"Login successful")
        else:
            print(f"Login failed: {response.text}")
            return False

        # Create topics
        response1 = requests.post(
            f"{self.base_url}/topic/{self.topic1}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        
        response2 = requests.post(
            f"{self.base_url}/topic/{self.topic2}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        
        if response1.status_code == 200 and response2.status_code == 200:
            print(f"Topics created successfully")
            return True
        else:
            print(f"Topic creation failed: {response1.text}, {response2.text}")
            return False

    def send_messages(self, topic, prefix, count):
        """Send a specified number of messages to a topic with a given prefix"""
        success = 0
        for i in range(1, count + 1):
            message = f"{prefix}-{i}"
            payload = {
                "topic_name": topic,
                "message": message
            }
            response = requests.post(
                f"{self.base_url}/message",
                json=payload,
                headers={"Authorization": f"Bearer {self.token}"}
            )
            
            if response.status_code == 200:
                success += 1
            else:
                print(f"Failed to send message {i} to {topic}: {response.text}")
                
        print(f"Sent {success}/{count} messages to {topic}")
        return success == count

    def get_all_messages(self, topic):
        """Get all messages from a topic using the subscribe endpoint"""
        response = requests.post(
            f"{self.base_url}/topic/{topic}/subscribe",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        
        if response.status_code == 200:
            try:
                data = response.json()
                messages = data.get("messages", [])
                return messages
            except Exception as e:
                print(f"Error parsing response: {e}")
                return []
        else:
            print(f"Failed to get messages from {topic}: {response.text}")
            return []

    def verify_isolation(self):
        """Verify that messages sent to one topic do not appear in the other"""
        # Get all messages from both topics
        messages1 = self.get_all_messages(self.topic1)
        messages2 = self.get_all_messages(self.topic2)
        
        print(f"Retrieved {len(messages1)} messages from {self.topic1}")
        print(f"Retrieved {len(messages2)} messages from {self.topic2}")
        
        # Check for topic1 integrity - all messages should start with "T1"
        topic1_integrity = all(msg.startswith("T1") for msg in messages1)
        
        # Check for topic2 integrity - all messages should start with "T2"
        topic2_integrity = all(msg.startswith("T2") for msg in messages2)
        
        print(f"Topic 1 integrity: {'PASS' if topic1_integrity else 'FAIL'}")
        print(f"Topic 2 integrity: {'PASS' if topic2_integrity else 'FAIL'}")
        
        return topic1_integrity and topic2_integrity

    def run_test(self):
        """Run the complete topic isolation test"""
        print("\n=== Starting Topic Isolation Test ===")
        
        # Setup authentication and topics
        if not self.setup():
            return False
            
        # Send messages to both topics concurrently
        print("\n=== Sending Messages ===")
        t1 = threading.Thread(target=self.send_messages, args=(self.topic1, "T1", self.message_count))
        t2 = threading.Thread(target=self.send_messages, args=(self.topic2, "T2", self.message_count))
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Pause to ensure messages are processed
        time.sleep(1)
        
        # Verify isolation
        print("\n=== Verifying Topic Isolation ===")
        result = self.verify_isolation()
        
        print(f"\nTopic Isolation Test: {'PASS' if result else 'FAIL'}")
        return result


if __name__ == "__main__":
    # Accept custom API URL from command line
    api_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    
    tester = TopicIsolationTest(api_url)
    tester.run_test()