# Follower coordination node for the Distributed Travel Agency project.

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

import json
import logging
import threading
import time
from typing import Optional

import zmq

from util.config import NodeConfig
from util.log_store import LogStore
from leader import LeaderNode

class FollowerNode:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.context = zmq.Context()

        # Sockets
        self.heartbeat_sub = None 
        self.control_socket = None 
        self.election_responder = None

        # Local state
        self.log_store = LogStore(config.DB_PATH)
        self.current_leader_id: Optional[int] = config.initial_leader_id
        self.last_heartbeat_ts: float = time.time()
        self.running = False
        self.promote_to_leader = False
        self.election_in_progress = False

        # Concurrency
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def setup_sockets(self) -> None:
        logging.info("Setting up follower sockets...")

        # Heartbeats: SUB
        self.heartbeat_sub = self.context.socket(zmq.SUB)
        self.heartbeat_sub.connect(self.config.HEARTBEAT_ADDR)
        self.heartbeat_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        # Control: DEALER
        self.control_socket = self.context.socket(zmq.DEALER)
        self.control_socket.setsockopt_string(zmq.IDENTITY, f"node-{self.config.node_id}")
        self.control_socket.connect(self.config.CONTROL_ADDR)

        # Peer-to-peer election listener (ROUTER)
        my_election_addr = self.config.peer_addresses[self.config.node_id]
        self.election_responder = self.context.socket(zmq.ROUTER)
        self.election_responder.bind(my_election_addr)
        logging.info(f"Listening for elections on {my_election_addr}")

    # ------------------------------------------------------------------
    # Heartbeat handling
    # ------------------------------------------------------------------
    def _heartbeat_listener_loop(self) -> None:
        logging.info("Heartbeat listener thread started.")
        while self.running:
            try:
                if self.heartbeat_sub.poll(timeout=500):
                    msg = self.heartbeat_sub.recv_string()
                    data = json.loads(msg)
                    if data.get("type") == "heartbeat":
                        leader_id = data.get("leader_id")
                        ts = data.get("timestamp", time.time())
                        with self._lock:
                            self.last_heartbeat_ts = ts
                            self.current_leader_id = leader_id
            except zmq.ZMQError:
                time.sleep(0.1)
            except Exception as e:
                logging.exception("Error in heartbeat listener: %s", e)
                time.sleep(0.5)
        logging.info("Heartbeat listener thread exiting.")

    def _leader_watchdog_loop(self) -> None:
        logging.info("Leader watchdog thread started.")
        while self.running:
            now = time.time()
            with self._lock:
                last = self.last_heartbeat_ts
                leader_id = self.current_leader_id

            if leader_id is not None and last > 0:
                delta = now - last
                if delta > self.config.HEARTBEAT_TIMEOUT:
                    logging.warning(f"Leader {leader_id} appears dead (no heartbeat for {delta:.2f}s).")
                    with self._lock:
                        self.current_leader_id = None
                    self._start_bully_election()
            time.sleep(0.5)
        logging.info("Leader watchdog thread exiting.")

    # ------------------------------------------------------------------
    # Election Victory Logic
    # ------------------------------------------------------------------
    def _become_leader(self):
        logging.info("I AM WINNING THE ELECTION !!!")
        logging.info(f"Node {self.config.node_id} is declaring itself the new LEADER.")
        
        self.config.current_leader_id = self.config.node_id
        self.election_in_progress = False

        # Broadcast COORDINATOR
        lower_nodes = [nid for nid in self.config.all_node_ids if nid < self.config.node_id]
        
        for nid in lower_nodes:
            address = self.config.peer_addresses.get(nid)
            if not address: continue
            
            sock = self.context.socket(zmq.DEALER)
            # --- FIX 1: Set LINGER to 0 to prevent hang on close ---
            sock.setsockopt(zmq.LINGER, 0)
            # -------------------------------------------------------
            sock.setsockopt(zmq.SNDTIMEO, 1000)
            sock.connect(address)
            try:
                msg = {"type": "COORDINATOR", "from_node": self.config.node_id}
                sock.send_json(msg)
                logging.info(f"Sent COORDINATOR to node {nid}")
            except Exception as e:
                logging.warning(f"Could not send COORDINATOR to {nid}: {e}")
            finally:
                sock.close()

        logging.info("Stopping Follower loop to transition to Leader role...")
        self.promote_to_leader = True
        self.running = False

    # ------------------------------------------------------------------
    # Bully election
    # ------------------------------------------------------------------
    def _start_bully_election(self) -> None:
        logging.info("Starting Bully election (node_id=%s)...", self.config.node_id)
        
        higher_nodes = [nid for nid in self.config.all_node_ids if nid > self.config.node_id]

        if not higher_nodes:
            self._become_leader()
            return

        logging.info(f"Sending ELECTION to higher nodes: {higher_nodes}")
        received_ok = False

        for nid in higher_nodes:
            address = self.config.peer_addresses.get(nid)
            if not address: continue

            temp_sock = self.context.socket(zmq.DEALER)
            # --- FIX 2: Set LINGER to 0 to prevent hang if node is dead ---
            temp_sock.setsockopt(zmq.LINGER, 0)
            # --------------------------------------------------------------
            temp_sock.setsockopt(zmq.IDENTITY, str(self.config.node_id).encode())
            temp_sock.connect(address)

            try:
                msg = {"type": "ELECTION", "from_node": self.config.node_id}
                temp_sock.send_json(msg)
                
                if temp_sock.poll(timeout=1000):
                    reply = temp_sock.recv_json()
                    if reply.get("type") == "OK":
                        logging.info(f"Received OK from node {nid}")
                        received_ok = True
            except Exception as e:
                logging.warning(f"Failed to communicate with node {nid}: {e}")
            finally:
                temp_sock.close()
            
            if received_ok:
                break

        if not received_ok:
            logging.info("No higher nodes responded (or they are dead).")
            self._become_leader()
        else:
            logging.info("Higher node responded. Waiting for COORDINATOR message.")
            self.election_in_progress = True

    # ------------------------------------------------------------------
    # Handle incoming election messages
    # ------------------------------------------------------------------
    def _handle_election_messages(self):
        try:
            if self.election_responder.poll(timeout=100): 
                frames = self.election_responder.recv_multipart()
                if len(frames) == 2:
                    identity, msg_bytes = frames
                elif len(frames) == 3:
                    identity, empty, msg_bytes = frames
                else:
                    return

                msg = json.loads(msg_bytes)
                msg_type = msg.get("type")
                sender_id = msg.get("from_node")

                if msg_type == "ELECTION":
                    logging.info(f"Received ELECTION from {sender_id}. Sending OK.")
                    response = {"type": "OK", "from_node": self.config.node_id}
                    self.election_responder.send_multipart([
                        identity, 
                        json.dumps(response).encode()
                    ])
                    
                    if not self.election_in_progress:
                        threading.Thread(target=self._start_bully_election).start()

                elif msg_type == "COORDINATOR":
                    logging.info(f"Received COORDINATOR from {sender_id}. New Leader elected.")
                    self.config.current_leader_id = sender_id
                    self.election_in_progress = False
                    with self._lock:
                        self.last_heartbeat_ts = time.time()

        except zmq.ZMQError:
            pass 
        except Exception as e:
            logging.error(f"Error in election handler: {e}")

    def _election_listener_loop(self):
        logging.info("Election listener thread started.")
        while self.running:
            try:
                self._handle_election_messages()
            except Exception as e:
                logging.error(f"Error in election listener: {e}")
                time.sleep(1.0)

    # ------------------------------------------------------------------
    # Log replication
    # ------------------------------------------------------------------
    def request_log_sync(self) -> None:
        last_idx = self.log_store.latest_index()
        request = {
            "type": "log_sync_request",
            "from_node": self.config.node_id,
            "last_index": last_idx,
        }

        try:
            self.control_socket.send_json(request)

            if self.control_socket.poll(timeout=2000): 
                reply = self.control_socket.recv_json()
                if reply.get("type") == "log_sync_response":
                    new_entries = reply.get("entries", [])
                    for entry in new_entries:
                        etype = entry.get("entry_type", "unknown")
                        payload = entry.get("payload", {})
                        idx = self.log_store.append_entry(etype, payload)
                        logging.info(f"Synced entry {idx}: {etype}")
        except zmq.ZMQError:
            pass
        except Exception as e:
            logging.exception("Unexpected error during log sync: %s", e)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def start(self) -> bool:
        logging.info("Starting follower node (id=%s)...", self.config.node_id)
        self.running = True

        self.setup_sockets()

        heartbeat_thread = threading.Thread(target=self._heartbeat_listener_loop, daemon=True)
        watchdog_thread = threading.Thread(target=self._leader_watchdog_loop, daemon=True)
        election_thread = threading.Thread(target=self._election_listener_loop, daemon=True)

        heartbeat_thread.start()
        watchdog_thread.start()
        election_thread.start()

        try:
            while self.running:
                if self.current_leader_id is not None:
                    self.request_log_sync()
                
                # --- FIX 3: Efficient sleep check (from previous step) ---
                # Check repeatedly so we can exit immediately if self.running becomes False
                sync_interval = 5.0
                steps = int(sync_interval * 10) 
                for _ in range(steps):
                    if not self.running:
                        break
                    time.sleep(0.1)
                # ---------------------------------------------------------

        except KeyboardInterrupt:
            logging.info("Follower received KeyboardInterrupt, shutting down...")
        finally:
            self.running = False
            time.sleep(0.5) 
            self._cleanup()

        return self.promote_to_leader

    def _cleanup(self) -> None:
        logging.info("Cleaning up follower resources...")
        try:
            # We use close(0) here, which implies LINGER=0
            if self.heartbeat_sub: self.heartbeat_sub.close(0)
            if self.control_socket: self.control_socket.close(0)
            if hasattr(self, 'election_responder'): self.election_responder.close(0)
            self.context.term()
        except Exception as e:
            logging.error("Error during cleanup: %s", e)

# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python follower.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])

    config_path = "../config.json"
    if not os.path.exists(config_path):
        config_path = "config.json"

    with open(config_path) as f:
        cfg = json.load(f)

    peer_map = {int(k): v["network"]["election_address"] for k, v in cfg["nodes"].items()}
    
    config = NodeConfig(
        node_id=node_id,
        all_node_ids=cfg["cluster"]["all_node_ids"],
        initial_leader_id=cfg["cluster"]["initial_leader_id"],
        HEARTBEAT_ADDR=cfg["nodes"][str(node_id)]["network"]["heartbeat_address"],
        CONTROL_ADDR=cfg["nodes"][str(node_id)]["network"]["control_address"],
        DB_PATH=cfg["nodes"][str(node_id)]["database"]["path"],
        HEARTBEAT_INTERVAL=cfg["timing"]["heartbeat_interval"],
        HEARTBEAT_TIMEOUT=cfg["timing"]["heartbeat_timeout"],
        ELECTION_TIMEOUT=cfg["timing"]["election_timeout"],
        peer_addresses=peer_map,
        worker_addresses=cfg["cpp_workers"]
    )

    current_role = "follower"
    
    while True:
        if current_role == "follower":
            logging.info(f"=== Running as FOLLOWER (Node {node_id}) ===")
            follower = FollowerNode(config)
            should_promote = follower.start()
            
            if should_promote:
                logging.info("Promoting self to LEADER...")
                current_role = "leader"
                time.sleep(1.0) 
            else:
                break

        elif current_role == "leader":
            logging.info(f"=== Running as LEADER (Node {node_id}) ===")
            try:
                leader = LeaderNode(config)
                # If this returns True, we were demoted gracefully by a higher node
                was_demoted = leader.start()
                
                if was_demoted:
                    logging.info("Leader was demoted. Switching back to FOLLOWER.")
                    current_role = "follower"
                    time.sleep(1.0) 
                else:
                    # leader.start() returned False, meaning we are shutting down completely
                    break 

            except zmq.ZMQError as e:
                logging.error(f"Failed to start Leader (Port in use?): {e}")
                logging.info("Demoting to FOLLOWER to check for existing leader.")
                current_role = "follower"
                time.sleep(2.0) # Wait a bit for the real leader to stabilize

if __name__ == "__main__":
    main()