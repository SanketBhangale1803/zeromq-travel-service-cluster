# zeromq-travel-service-cluster

A distributed computing system demonstrating leader election, fault tolerance, and polyglot architecture using Python coordination nodes and C++ worker nodes.

## How to Run the System

### Step 1: Install Dependencies

```bash
# Install ZeroMQ and C++ bindings
brew install zeromq cppzmq

# Install Python ZeroMQ bindings
pip install pyzmq
```

### Step 2: Build C++ Workers

```bash
cd cpp_nodes
mkdir -p build
cd build
cmake ..
make
```

### Step 3: Start All Components

Open 6 separate terminals and run:

**Terminal 1 - Availability Worker:**
```bash
cd cpp_nodes/build
./availability_worker
```

**Terminal 2 - Pricing Worker:**
```bash
cd cpp_nodes/build
./pricing_worker
```

**Terminal 3 - Booking Worker:**
```bash
cd cpp_nodes/build
./booking_worker
```

**Terminal 4 - Python Follower:**
```bash
cd python_nodes
python follower.py 1
```

**Terminal 5 - Python Follower:**
```bash
cd python_nodes
python follower.py 2
```

**Terminal 6 - Run Test:**
```bash
python test_complete_system.py
```
