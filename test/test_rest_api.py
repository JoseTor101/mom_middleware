#!/usr/bin/env python3

import json
import os
import sys
import time
import requests

# Add parent directory to import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class RestApiTester:
    """Test class for MOM Middleware REST API"""

    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.token = None
        self.username = "testuser"
        self.password = "testpass"
        self.topic_name = "python_test_topic"
        self.partitions = 3
        print(f"MOM Middleware REST API Test (Python)")
        print(f"API URL: {self.base_url}")

    def signup(self):
        """Test user signup endpoint"""
        print("\n=== Testing Signup ===")
        response = requests.post(
            f"{self.base_url}/signup",
            data={"username": self.username, "password": self.password}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def login(self):
        """Test login and token retrieval"""
        print("\n=== Testing Login ===")
        response = requests.post(
            f"{self.base_url}/login",
            data={"username": self.username, "password": self.password}
        )
        print(f"Status code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                self.token = data.get("access_token")
                print(f"Token received: {self.token[:15]}...")
                return True
            except Exception as e:
                print(f"Error parsing response: {e}")
                return False
        else:
            print(f"Login failed: {response.text}")
            return False

    def register_node(self):
        """Test node registration"""
        print("\n=== Testing Node Registration ===")
        import socket
        try:
            # Get local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
        except:
            ip = "127.0.0.1"
            
        response = requests.post(
            f"{self.base_url}/node/register",
            data={"ip": ip}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def create_topic(self):
        """Test topic creation"""
        print(f"\n=== Creating Topic: {self.topic_name} ===")
        response = requests.post(
            f"{self.base_url}/topic/{self.topic_name}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def list_topics(self):
        """Test listing topics"""
        print("\n=== Testing List Topics ===")
        response = requests.post(f"{self.base_url}/list/topics")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                topics = data.get("topics", [])
                if self.topic_name in topics:
                    print(f"Topic {self.topic_name} found in list.")
                    return True
                else:
                    print(f"Topic {self.topic_name} NOT found.")
                    return False
            except Exception as e:
                print(f"Error parsing response: {e}")
                return False
        return False

    def list_nodes(self):
        """Test listing nodes"""
        print("\n=== Testing List Nodes ===")
        response = requests.post(f"{self.base_url}/list/instances")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def send_message(self, message=None):
        """Test sending messages to a topic"""
        if not message:
            message = f"Test message from Python at {time.ctime()}"
            
        print(f"\n=== Sending Message to {self.topic_name} ===")
        payload = {
            "topic_name": self.topic_name,
            "message": message
        }
        response = requests.post(
            f"{self.base_url}/message",
            json=payload,
            headers={"Authorization": f"Bearer {self.token}"}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def get_message_from_partition(self, partition_id=0):
        """Test getting message from a specific partition"""
        print(f"\n=== Getting Message from {self.topic_name}[{partition_id}] ===")
        response = requests.post(
            f"{self.base_url}/message/{self.topic_name}/{partition_id}",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def get_topic_info(self):
        """Test getting topic information"""
        print(f"\n=== Getting Topic Info for {self.topic_name} ===")
        response = requests.post(
            f"{self.base_url}/topic/{self.topic_name}/info",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def get_connection_info(self):
        """Test getting connection info for remote nodes"""
        print("\n=== Getting Connection Info ===")
        response = requests.get(f"{self.base_url}/connect")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def subscribe_to_topic(self):
        """Test subscribing to a topic"""
        print(f"\n=== Subscribing to {self.topic_name} ===")
        response = requests.post(
            f"{self.base_url}/topic/{self.topic_name}/subscribe",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200

    def test_round_robin(self, num_messages=5):
        """Test round-robin message distribution by sending multiple messages"""
        print(f"\n=== Testing Round-Robin with {num_messages} messages ===")
        success_count = 0
        for i in range(1, num_messages + 1):
            message = f"Round-robin test message {i}"
            if self.send_message(message):
                success_count += 1
                
        print(f"Successfully sent {success_count}/{num_messages} messages.")
        return success_count == num_messages

    def run_all_tests(self):
        """Run all tests in sequence"""
        print("\n=== Starting All Tests ===")
        
        # Basic auth and setup
        results = {
            "signup": self.signup(),
            "login": self.login()
        }
        
        # Skip remaining tests if login fails
        if not results["login"]:
            print("Login failed. Skipping remaining tests.")
            return results
        
        # Continue with other tests
        results.update({
            "register_node": self.register_node(),
            "create_topic": self.create_topic(),
            "list_topics": self.list_topics(),
            "list_nodes": self.list_nodes(),
            "send_message": self.send_message(),
            "get_message": self.get_message_from_partition(0),
            "topic_info": self.get_topic_info(),
            "connection_info": self.get_connection_info(),
            "subscribe": self.subscribe_to_topic(),
            "round_robin": self.test_round_robin()
        })
        
        # Show summary
        print("\n=== Test Results Summary ===")
        passed = 0
        for test, result in results.items():
            status = "PASS" if result else "FAIL"
            status_color = "\033[92m" if result else "\033[91m"
            print(f"{status_color}{status}\033[0m: {test}")
            if result:
                passed += 1
                
        print(f"\nPassed {passed}/{len(results)} tests")
        return results


if __name__ == "__main__":
    # Accept custom API URL from command line
    api_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    
    tester = RestApiTester(api_url)
    tester.run_all_tests()