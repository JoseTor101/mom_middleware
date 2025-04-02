import requests

BASE_URL = "http://localhost:8000"

def create_topic(topic_name):
    response = requests.post(f"{BASE_URL}/topic/{topic_name}")
    print(response.json())

def list_topics():
    response = requests.get(f"{BASE_URL}/topics")
    print(response.json())

if __name__ == "__main__":
    create_topic("test_topic")
    list_topics()
