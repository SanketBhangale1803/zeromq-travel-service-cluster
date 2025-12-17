from dataclasses import dataclass
from typing import List, Optional

@dataclass
class NodeConfig:
    node_id: int
    all_node_ids: List[int]
    HEARTBEAT_ADDR: str
    CONTROL_ADDR: str
    
    # Optional initial leader
    initial_leader_id: Optional[int] = None

    # Database
    DB_PATH: str = "node_log.db"

    # C++ Workers
    worker_addresses: dict = None

    # Map of node_id -> election_address (e.g., {1: "tcp://localhost:5561", ...})
    peer_addresses: dict = None

    # Timing
    HEARTBEAT_INTERVAL: float = 1.0
    HEARTBEAT_TIMEOUT: float = 3.0
    ELECTION_TIMEOUT: float = 2.0