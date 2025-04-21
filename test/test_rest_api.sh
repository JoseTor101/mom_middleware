#!/bin/bash

# Test script for MOM Middleware REST API
# This script tests the various endpoints of the REST API using curl commands

# Configuration
API_URL="http://localhost:8000"
TEST_USER="testuser"
TEST_PASS="testpass"
ACCESS_TOKEN=""
TOPIC_NAME="test_topic"
NODE_IP=$(hostname -I | awk '{print $1}')

# Text colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}MOM Middleware REST API Test Script${NC}"
echo "========================================"
echo "API URL: $API_URL"
echo "Test User: $TEST_USER"
echo "Node IP: $NODE_IP"
echo

# Helper function to check response
check_response() {
    if echo "$1" | grep -q "Success"; then
        echo -e "${GREEN}✓ Success${NC}"
        return 0
    else
        echo -e "${RED}✗ Failed${NC}"
        echo -e "${RED}Response: $1${NC}"
        return 1
    fi
}

# 1. Test Signup
echo "Testing Signup..."
SIGNUP_RESPONSE=$(curl -s -X POST "$API_URL/signup" \
    -d "username=$TEST_USER&password=$TEST_PASS" \
    -H "Content-Type: application/x-www-form-urlencoded")
echo "Response: $SIGNUP_RESPONSE"
echo

# 2. Test Login
echo "Testing Login..."
LOGIN_RESPONSE=$(curl -s -X POST "$API_URL/login" \
    -d "username=$TEST_USER&password=$TEST_PASS" \
    -H "Content-Type: application/x-www-form-urlencoded")

if echo "$LOGIN_RESPONSE" | grep -q "access_token"; then
    echo -e "${GREEN}✓ Login successful${NC}"
    # Extract token
    ACCESS_TOKEN=$(echo $LOGIN_RESPONSE | sed 's/.*"access_token":"\([^"]*\)".*/\1/')
    echo "Token: ${ACCESS_TOKEN:0:15}..."
else
    echo -e "${RED}✗ Login failed${NC}"
    echo -e "${RED}Response: $LOGIN_RESPONSE${NC}"
    exit 1
fi
echo

# 3. Test Register Node
echo "Testing Node Registration..."
NODE_REG_RESPONSE=$(curl -s -X POST "$API_URL/node/register" \
    -d "ip=$NODE_IP" \
    -H "Content-Type: application/x-www-form-urlencoded")
check_response "$NODE_REG_RESPONSE"
echo

# 4. Test Create Topic
echo "Testing Topic Creation..."
TOPIC_CREATE_RESPONSE=$(curl -s -X POST "$API_URL/topic/$TOPIC_NAME" \
    -H "Authorization: Bearer $ACCESS_TOKEN")
check_response "$TOPIC_CREATE_RESPONSE"
echo

# 5. Test List Topics
echo "Testing List Topics..."
TOPICS_RESPONSE=$(curl -s -X POST "$API_URL/list/topics")
echo "Response: $TOPICS_RESPONSE"
if echo "$TOPICS_RESPONSE" | grep -q "$TOPIC_NAME"; then
    echo -e "${GREEN}✓ Topic found in list${NC}"
else
    echo -e "${RED}✗ Topic not found in list${NC}"
fi
echo

# 6. Test List Nodes
echo "Testing List Nodes..."
NODES_RESPONSE=$(curl -s -X POST "$API_URL/list/instances")
echo "Response: $NODES_RESPONSE"
echo

# 7. Test Send Message
echo "Testing Send Message..."
MESSAGE="Test message from CURL at $(date)"
SEND_RESPONSE=$(curl -s -X POST "$API_URL/message" \
    -d "{\"topic_name\":\"$TOPIC_NAME\",\"message\":\"$MESSAGE\"}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $ACCESS_TOKEN")
check_response "$SEND_RESPONSE"
echo

# 8. Test Get Message from Partition
echo "Testing Get Message from Partition..."
for PARTITION in {0..2}; do
    echo "Checking partition $PARTITION..."
    GET_RESPONSE=$(curl -s -X POST "$API_URL/message/$TOPIC_NAME/$PARTITION" \
        -H "Authorization: Bearer $ACCESS_TOKEN")
    echo "Response: $GET_RESPONSE"
    echo
done

# 9. Test Topic Info
echo "Testing Topic Info..."
INFO_RESPONSE=$(curl -s -X POST "$API_URL/topic/$TOPIC_NAME/info" \
    -H "Authorization: Bearer $ACCESS_TOKEN")
echo "Response: $INFO_RESPONSE"
echo

# 10. Test Connection Info
echo "Testing Connection Info..."
CONN_RESPONSE=$(curl -s -X GET "$API_URL/connect")
echo "Response: $CONN_RESPONSE"
echo

# 11. Test Topic Subscription
echo "Testing Topic Subscription..."
SUB_RESPONSE=$(curl -s -X POST "$API_URL/topic/$TOPIC_NAME/subscribe" \
    -H "Authorization: Bearer $ACCESS_TOKEN")
echo "Response: $SUB_RESPONSE"
echo

# Test sending multiple messages to see round robin distribution
echo "Testing multiple messages for round-robin distribution..."
for i in {1..5}; do
    echo "Sending message $i..."
    MESSAGE="Round-robin test message $i"
    SEND_RESPONSE=$(curl -s -X POST "$API_URL/message" \
        -d "{\"topic_name\":\"$TOPIC_NAME\",\"message\":\"$MESSAGE\"}" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ACCESS_TOKEN")
    check_response "$SEND_RESPONSE"
done
echo

echo "All tests completed!"