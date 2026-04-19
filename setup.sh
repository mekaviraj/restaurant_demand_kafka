#!/bin/bash

# ================================================================
# Kafka Delivery Semantics Demo - Automated Setup Script
# ================================================================
# This script automates the installation and startup of the demo
# Usage: bash setup.sh
# ================================================================

set -e  # Exit on error

echo "🚀 Kafka Delivery Semantics Demo - Setup Script"
echo "=================================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Java is installed
echo "📋 Checking prerequisites..."
if ! command -v java &> /dev/null; then
    echo -e "${RED}❌ Java not found. Installing Java 11...${NC}"
    sudo apt update
    sudo apt install -y openjdk-11-jdk
else
    echo -e "${GREEN}✅ Java installed${NC}"
fi

# Check if Python and pip are available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 not found. Please install Python 3.8+${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Python 3 available${NC}"

# Install Python dependencies
echo ""
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo -e "${GREEN}✅ Python dependencies installed${NC}"

# Download and extract Kafka
echo ""
echo "📥 Downloading Kafka 3.4.0..."
mkdir -p ~/kafka_demo
cd ~/kafka_demo

if [ ! -d "kafka_2.13-3.4.0" ]; then
    if [ ! -f "kafka_2.13-3.4.0.tgz" ]; then
        wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
    fi
    tar -xzf kafka_2.13-3.4.0.tgz
    echo -e "${GREEN}✅ Kafka extracted to ~/kafka_demo/kafka_2.13-3.4.0${NC}"
else
    echo -e "${GREEN}✅ Kafka already exists${NC}"
fi

cd - > /dev/null

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Setup Complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo "📖 Next Steps:"
echo "1. Read QUICK_START.md for a quick reference"
echo "2. Read README.md for detailed explanation"
echo "3. Open 6 terminals and run (in order):"
echo ""
echo "   Term 1: cd ~/kafka_demo/kafka_2.13-3.4.0"
echo "           bin/zookeeper-server-start.sh config/zookeeper.properties"
echo ""
echo "   Term 2: cd ~/kafka_demo/kafka_2.13-3.4.0"
echo "           bin/kafka-server-start.sh config/server.properties"
echo ""
echo "   Term 3: cd ~/kafka_demo/kafka_2.13-3.4.0"
echo "           bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3"
echo "           bin/kafka-topics.sh --create --topic metrics --bootstrap-server localhost:9092 --partitions 3"
echo ""
echo "   Term 4: cd ~/restaurant_demand_kafka"
echo "           spark-submit consumer/consumer_spark.py"
echo ""
echo "   Term 5: cd ~/restaurant_demand_kafka"
echo "           python dashboard/app.py"
echo ""
echo "   Term 6: cd ~/restaurant_demand_kafka"
echo "           python producer/producer.py exactly_once"
echo ""
echo "4. Open browser: http://localhost:5000"
echo ""
echo -e "${YELLOW}🎯 Run each producer mode for ~30 seconds and observe the dashboard!${NC}"
echo ""
