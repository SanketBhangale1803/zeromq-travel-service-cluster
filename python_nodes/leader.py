# The leader coordination node for the Distributed Travel Agency project

# Responsibilities:
# - Act as the current cluster leader.
# - Send periodic heartbeat messages to followers.
# - Respond to follower log sync requests using SQLite-based log storage.
# - (Later) Accept client booking requests, append them to the log, and
#  dispatch work to C++ worker nodes.

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

import json
import logging
import threading
import time
import sys

import zmq

from util.config import NodeConfig
from util.log_store import LogStore

# ---------------------------------------------------------------------------
# Leader Node
# ---------------------------------------------------------------------------
# - Publishes heartbeats to followers.
# - Responds to follower log sync requests via ROUTER socket.
# - Maintains the authoritative log of booking-related events.
class LeaderNode:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.context = zmq.Context()

        self.heartbeat_pub = None  # PUB socket
        self.control_router = None # ROUTER socket
        self.election_responder = None # Election socket

        self.log_store = LogStore(config.DB_PATH)
        self.running = False

        # For extra safety if you add shared state later
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Setup sockets
    # ------------------------------------------------------------------
    def setup_sockets(self) -> None:
        logging.info("Setting up leader sockets...")

        # Heartbeats: PUB bound to address (note: for leader we usually bind, followers connect)
        self.heartbeat_pub = self.context.socket(zmq.PUB)
        # HEARTBEAT_ADDR should use "tcp://*:port" for bind
        self.heartbeat_pub.bind(self.config.HEARTBEAT_ADDR)

        # Control / log sync: ROUTER bound to address, followers use DEALER + connect
        self.control_router = self.context.socket(zmq.ROUTER)
        self.control_router.bind(self.config.CONTROL_ADDR)

        # Election responder (ROUTER)
        my_election_addr = self.config.peer_addresses[self.config.node_id]
        self.election_responder = self.context.socket(zmq.ROUTER)
        self.election_responder.bind(my_election_addr)
        logging.info(f"Leader listening for higher authority on {my_election_addr}")

    # Listens for messages from other nodes
    # 1. If we receive ELECTION from a lower node -> Reply OK (we are still boss).
    # 2. If we receive COORDINATOR from a higher node -> We must step down.
    def _election_loop(self) -> None:
        logging.info("Leader election listener thread started.")
        
        while self.running:
            try:
                # Poll with short timeout so we can check self.running
                if self.election_responder.poll(timeout=100):
                    # ROUTER receives [identity, message]
                    frames = self.election_responder.recv_multipart()
                    if len(frames) == 2:
                        identity, msg_bytes = frames
                    elif len(frames) == 3:
                        identity, _, msg_bytes = frames
                    else:
                        continue

                    msg = json.loads(msg_bytes)
                    msg_type = msg.get("type")
                    sender_id = msg.get("from_node")

                    # CASE 1: A lower node is trying to hold an election.
                    # We assert authority.
                    if msg_type == "ELECTION":
                        logging.info(f"Leader received ELECTION from {sender_id}. Sending OK.")
                        response = {"type": "OK", "from_node": self.config.node_id}
                        self.election_responder.send_multipart([
                            identity,
                            json.dumps(response).encode("utf-8")
                        ])

                    # CASE 2: A higher node is declaring victory.
                    # We must demote ourselves.
                    elif msg_type == "COORDINATOR":
                        if sender_id > self.config.node_id:
                            logging.warning(f"Leader received COORDINATOR from higher node {sender_id}. Stepping down!")
                            self.demoted = True
                            self.running = False # This signals all other loops to stop
                            
            except zmq.ZMQError as e:
                if self.running: 
                    logging.error(f"ZMQ Error in leader election loop: {e}")
            except Exception as e:
                logging.error(f"Error in leader election loop: {e}")
        
        logging.info("Leader election listener thread exiting.")

    # ------------------------------------------------------------------
    # Heartbeats
    # ------------------------------------------------------------------
    # - Background thread: periodically publish heartbeat messages.
    def _heartbeat_loop(self) -> None:
        logging.info("Heartbeat sender thread started.")
        leader_id = self.config.node_id

        while self.running:
            msg = {
                "type": "heartbeat",
                "leader_id": leader_id,
                "timestamp": time.time(),
            }
            try:
                # PUB socket sends string; followers use SUB and recv_string
                self.heartbeat_pub.send_string(json.dumps(msg))
                logging.debug("Sent heartbeat: %s", msg)
            except Exception as e:
                logging.exception("Error sending heartbeat: %s", e)

            time.sleep(self.config.HEARTBEAT_INTERVAL)

        logging.info("Heartbeat sender thread exiting.")

    # ------------------------------------------------------------------
    # Control / log sync handling
    # ------------------------------------------------------------------
    def _control_loop(self) -> None:
        """
        Main loop for handling control messages from followers.

        Protocol example for log sync (matching follower.py):

          follower -> leader (JSON, via DEALER):
            {
              "type": "log_sync_request",
              "from_node": int,
              "last_index": int
            }

          leader -> follower:
            {
              "type": "log_sync_response",
              "entries": [
                { "entry_type": "...", "payload": { ... } },
                ...
              ]
            }
        """
        logging.info("Control (ROUTER) loop started.")

        while self.running:
            try:
                # ROUTER receives: [identity, empty_frame (optional), message]
                # We assume identity, message (no empty frame) for simplicity.
                identity, raw = self.control_router.recv_multipart()
                request = json.loads(raw.decode("utf-8"))

                msg_type = request.get("type")
                if msg_type == "log_sync_request":
                    self._handle_log_sync_request(identity, request)
                else:
                    logging.warning("Unknown control message type: %s", msg_type)
                    self._send_error(identity, f"Unknown message type: {msg_type}")

            except zmq.ZMQError as e:
                if not self.running:
                    # Probably shutting down
                    break
                logging.error("ZMQ error in control loop: %s", e)
            except Exception as e:
                logging.exception("Unexpected error in control loop: %s", e)

        logging.info("Control loop exiting.")

    # Process a log_sync_request from a follower
    def _handle_log_sync_request(self, identity, request: dict) -> None:
        from_node = request.get("from_node")
        last_index = request.get("last_index", 0)

        logging.info(
            "Received log_sync_request from node %s (last_index=%d)",
            from_node,
            last_index,
        )

        with self._lock:
            new_entries = self.log_store.get_entries_after(last_index)

        # Followers only expect entry_type + payload
        stripped_entries = [
            {"entry_type": e["entry_type"], "payload": e["payload"]}
            for e in new_entries
        ]

        response = {
            "type": "log_sync_response",
            "entries": stripped_entries,
        }

        self.control_router.send_multipart(
            [
                identity,
                json.dumps(response).encode("utf-8"),
            ]
        )

        logging.info(
            "Sent log_sync_response to node %s with %d entries.",
            from_node,
            len(stripped_entries),
        )

    def _send_error(self, identity, message: str) -> None:
        response = {
            "type": "error",
            "message": message,
        }
        self.control_router.send_multipart(
            [
                identity,
                json.dumps(response).encode("utf-8"),
            ]
        )

    # ------------------------------------------------------------------
    # C++ Worker Communication
    # ------------------------------------------------------------------
    # Setup connections to C++ worker nodes
    def setup_worker_connections(self):
        self.worker_sockets = {}
        
        # Extract the addresses from the config object
        # The JSON structure is: config["cpp_workers"]["worker_type"]["address"]
        try:
            # 1. Availability Worker
            avail_addr = self.config.worker_addresses['availability']['address']
            self.worker_sockets['availability'] = self.context.socket(zmq.REQ)
            self.worker_sockets['availability'].connect(avail_addr)
            logging.info(f"Connected to Availability Worker at {avail_addr}")

            # 2. Pricing Worker
            pricing_addr = self.config.worker_addresses['pricing']['address']
            self.worker_sockets['pricing'] = self.context.socket(zmq.REQ)
            self.worker_sockets['pricing'].connect(pricing_addr)
            logging.info(f"Connected to Pricing Worker at {pricing_addr}")

            # 3. Booking Worker
            booking_addr = self.config.worker_addresses['booking']['address']
            self.worker_sockets['booking'] = self.context.socket(zmq.REQ)
            self.worker_sockets['booking'].connect(booking_addr)
            logging.info(f"Connected to Booking Worker at {booking_addr}")
            
        except KeyError as e:
            logging.error(f"Missing configuration for worker: {e}")
        except Exception as e:
            logging.error(f"Failed to connect to workers: {e}")

    def send_to_worker(self, worker_type: str, request: str) -> dict:
        """Send request to specific C++ worker and return response"""
        try:
            socket = self.worker_sockets[worker_type]
            socket.send_string(request)
            response = socket.recv_string()
            return {"success": True, "response": response}
        except Exception as e:
            logging.error(f"Error communicating with {worker_type} worker: {e}")
            return {"success": False, "error": str(e)}

    # Process a complete booking request through the distributed system.
    # This demonstrates the full workflow: availability -> pricing -> booking
    def process_booking_request(self, booking_data: dict) -> dict:
        try:
            request_id = booking_data.get("request_id", str(int(time.time())))
            origin = booking_data.get("origin")
            destination = booking_data.get("destination")
            date = booking_data.get("date")
            travel_type = booking_data.get("travel_type", "flight")
            passengers = booking_data.get("passengers", 1)
            customer_id = booking_data.get("customer_id")

            # Step 1: Check availability
            avail_request = f"AVAILABILITY|{request_id}_avail|{origin}|{destination}|{date}"
            avail_result = self.send_to_worker('availability', avail_request)
            
            if not avail_result["success"]:
                return {"success": False, "stage": "availability", "error": avail_result["error"]}
            
            try:
                avail_data = json.loads(avail_result["response"])
                if not avail_data.get("available", False):
                    return {"success": False, "stage": "availability", "message": "No availability"}
            except json.JSONDecodeError:
                return {"success": False, "stage": "availability", "error": "Invalid response format"}

            # Step 2: Get pricing
            price_request = f"PRICING|{request_id}_price|{origin}|{destination}|{date}|{travel_type}"
            price_result = self.send_to_worker('pricing', price_request)
            
            if not price_result["success"]:
                return {"success": False, "stage": "pricing", "error": price_result["error"]}
            
            try:
                price_data = json.loads(price_result["response"])
                price = price_data.get("price", 0)
            except json.JSONDecodeError:
                return {"success": False, "stage": "pricing", "error": "Invalid response format"}

            # Step 3: Make booking
            booking_request = f"BOOKING|{request_id}_book|{customer_id}|{origin}|{destination}|{date}|{travel_type}|{passengers}"
            booking_result = self.send_to_worker('booking', booking_request)
            
            if not booking_result["success"]:
                return {"success": False, "stage": "booking", "error": booking_result["error"]}
            
            try:
                booking_response = json.loads(booking_result["response"])
                
                # Log the successful booking to the distributed log
                log_entry = {
                    "request_id": request_id,
                    "booking_data": booking_data,
                    "availability_response": avail_data,
                    "pricing_response": price_data,
                    "booking_response": booking_response,
                    "timestamp": time.time()
                }
                
                log_idx = self.log_store.append_entry("booking_completed", log_entry)
                logging.info(f"Logged completed booking at index {log_idx}")
                
                return {
                    "success": True,
                    "booking_response": booking_response,
                    "price": price,
                    "log_index": log_idx
                }
                
            except json.JSONDecodeError:
                return {"success": False, "stage": "booking", "error": "Invalid response format"}

        except Exception as e:
            logging.exception(f"Unexpected error in booking process: {e}")
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Client Request Handler
    # ------------------------------------------------------------------
    def setup_client_socket(self):
        """Setup socket to receive client booking requests"""
        self.client_socket = self.context.socket(zmq.REP)
        self.client_socket.bind("tcp://*:5559")  # New port for client requests
        logging.info("Client request handler listening on port 5559")

    def _client_handler_loop(self):
        """Handle incoming client booking requests"""
        logging.info("Client handler loop started.")
        
        while self.running:
            try:
                # Receive client request
                request = self.client_socket.recv_json()
                logging.info(f"Received client request: {request}")
                
                if request.get("type") == "booking_request":
                    # Process the booking through C++ workers
                    result = self.process_booking_request(request.get("data", {}))
                    
                    # Send response back to client
                    response = {
                        "type": "booking_response",
                        "result": result,
                        "timestamp": time.time()
                    }
                    self.client_socket.send_json(response)
                    
                elif request.get("type") == "status_request":
                    # Return system status
                    status = {
                        "type": "status_response",
                        "leader_id": self.config.node_id,
                        "latest_log_index": self.log_store.latest_index(),
                        "timestamp": time.time()
                    }
                    self.client_socket.send_json(status)
                    
                else:
                    error_response = {
                        "type": "error",
                        "message": f"Unknown request type: {request.get('type')}"
                    }
                    self.client_socket.send_json(error_response)
                    
            except zmq.ZMQError as e:
                if not self.running:
                    break
                logging.error(f"ZMQ error in client handler: {e}")
            except Exception as e:
                logging.exception(f"Error in client handler: {e}")
        
        logging.info("Client handler loop exiting.")

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------
    # Start the leader: sockets, threads, and main loops.
    def start(self) -> bool:
        logging.info("Starting leader node (id=%s)...", self.config.node_id)
        self.running = True
        self.demoted = False

        self.setup_sockets()
        self.setup_worker_connections()
        self.setup_client_socket()

        # Start heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="HeartbeatSender",
            daemon=True,
        )

        # Start control loop thread
        control_thread = threading.Thread(
            target=self._control_loop,
            name="ControlLoop",
            daemon=True,
        )

        # Start election loop thread
        election_thread = threading.Thread(
            target=self._election_loop,
            name="LeaderElectionListener",
            daemon=True,
        )
        election_thread.start()

        # Start client handler thread
        client_thread = threading.Thread(
            target=self._client_handler_loop,
            name="ClientHandler",
            daemon=True,
        )

        heartbeat_thread.start()
        control_thread.start()
        client_thread.start()

        # Leader doesn't need a heavy main loop; we just block until killed
        try:
            while self.running:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logging.info("Leader received KeyboardInterrupt, shutting down...")
        finally:
            self.running = False
            time.sleep(0.5)
            self._cleanup()

        return self.demoted

    # Close sockets and terminate context.
    def _cleanup(self) -> None:
        logging.info("Cleaning up leader resources...")
        try:
            if self.heartbeat_pub is not None:
                self.heartbeat_pub.close(0)
            if self.control_router is not None:
                self.control_router.close(0)
            if self.election_responder:
                self.election_responder.close(0)
            if hasattr(self, 'client_socket') and self.client_socket is not None:
                self.client_socket.close(0)
            if hasattr(self, 'worker_sockets'):
                for socket in self.worker_sockets.values():
                    socket.close(0)
            self.context.term()
        except Exception as e:
            logging.error("Error during leader cleanup: %s", e)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
    )

    # --- get node_id from CLI ---
    if len(sys.argv) < 2:
        print("Usage: python leader.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])

    # --- Load Config ---
    # (Ensure path is correct)
    config_path = "../config.json"
    if not os.path.exists(config_path):
        config_path = "config.json" # Fallback if running from different dir

    with open(config_path) as f:
        cfg = json.load(f)

    node_cfg = cfg["nodes"][str(node_id)]
    cluster_cfg = cfg["cluster"]
    timing_cfg = cfg["timing"]
    worker_cfg = cfg["cpp_workers"]

    if node_cfg.get("role") != "leader":
        logging.warning(
            "Node %s is configured as role '%s' but you're running leader.py",
            node_id,
            node_cfg.get("role"),
        )

    config = NodeConfig(
        node_id=node_id,
        all_node_ids=cluster_cfg["all_node_ids"],
        initial_leader_id=cluster_cfg["initial_leader_id"],
        HEARTBEAT_ADDR=node_cfg["network"]["heartbeat_address"],
        CONTROL_ADDR=node_cfg["network"]["control_address"],
        DB_PATH=node_cfg["database"]["path"],
        HEARTBEAT_INTERVAL=timing_cfg["heartbeat_interval"],
        HEARTBEAT_TIMEOUT=timing_cfg["heartbeat_timeout"],
        ELECTION_TIMEOUT=timing_cfg["election_timeout"],
        worker_addresses=worker_cfg
    )

    leader = LeaderNode(config)
    leader.start()


if __name__ == "__main__":
    main()