import zmq
import time
import random
import csv
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
LEADER_CLIENT_URL = "tcp://localhost:5559"
CSV_FILENAME = "experiment_results.csv"

# ==============================================================================
# CLIENT CLASS
# ==============================================================================
class TravelAgencyClient:
    def __init__(self, timeout_ms=5000):
        """
        Initializes a client. 
        Args:
            timeout_ms: Timeout for receiving a reply. Essential for Fault Tolerance testing
                        so the client doesn't hang forever if the Leader dies.
        """
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(LEADER_CLIENT_URL)
        # Set receive timeout (important for detecting downtime)
        self.socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        self.socket.setsockopt(zmq.LINGER, 0)
        
    def send_booking_request(self, origin, destination, date, travel_type="flight", passengers=1, customer_id="TEST001"):
        request_id = f"REQ_{random.randint(1000, 9999)}_{int(time.time()*1000)}"
        request = {
            "type": "booking_request",
            "data": {
                "customer_id": customer_id,
                "origin": origin,
                "destination": destination,
                "date": date,
                "travel_type": travel_type,
                "passengers": passengers,
                "request_id": request_id
            }
        }
        
        start_time = time.time()
        try:
            self.socket.send_json(request)
            response = self.socket.recv_json()
            end_time = time.time()
            return {
                "success": True, 
                "latency": (end_time - start_time) * 1000, # ms
                "response": response,
                "start_ts": start_time,
                "request_id": request_id
            }
        except zmq.Again:
            return {
                "success": False, 
                "latency": (time.time() - start_time) * 1000,
                "error": "Timeout",
                "start_ts": start_time,
                "request_id": request_id
            }
        except Exception as e:
            return {
                "success": False, 
                "latency": 0, 
                "error": str(e),
                "start_ts": start_time,
                "request_id": request_id
            }
    
    def get_system_status(self):
        try:
            self.socket.send_json({"type": "status_request"})
            return self.socket.recv_json()
        except Exception as e:
            return {"error": str(e)}
    
    def close(self):
        self.socket.close()
        self.context.term()

# ==============================================================================
# PERFORMANCE SUITE
# ==============================================================================
class PerformanceExperiment:
    def __init__(self):
        # Prepare CSV file for raw data (Metrics collected requirement)
        with open(CSV_FILENAME, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["Experiment", "Timestamp", "Concurrency", "Request_ID", "Status", "Latency_ms", "Error"])

    def log_result(self, experiment_name, concurrency, result):
        with open(CSV_FILENAME, mode='a', newline='') as file:
            writer = csv.writer(file)
            status = "Success" if result["success"] else "Failed"
            error_msg = result.get("error", "")
            writer.writerow([
                experiment_name, 
                result["start_ts"], 
                concurrency, 
                result["request_id"], 
                status, 
                f"{result['latency']:.2f}", 
                error_msg
            ])

    def _worker_task(self):
        """Single task for a thread: create client, send request, close."""
        client = TravelAgencyClient(timeout_ms=5000)
        # Randomize inputs to simulate real traffic
        origins = ["NYC", "SFO", "LON", "PAR", "TOK"]
        dests = ["MIA", "LAX", "BER", "MAD", "SEO"]
        
        try:
            res = client.send_booking_request(
                origin=random.choice(origins),
                destination=random.choice(dests),
                date="2025-12-15",
                passengers=random.randint(1, 4)
            )
            return res
        finally:
            client.close()

    def run_scalability_test(self):
        """
        Experiment: Scalability & Throughput.
        Runs batches of requests with increasing concurrency levels.
        """
        print("\n=== Running Scalability/Throughput Experiment ===")
        print("Objective: Measure how latency and throughput change as client load increases.")
        
        concurrency_levels = [1, 5, 10, 20, 40]
        requests_per_level = 50 
        
        results_summary = []

        for level in concurrency_levels:
            print(f"\n--- Testing Concurrency Level: {level} ---")
            latencies = []
            success_count = 0
            start_batch = time.time()
            
            with ThreadPoolExecutor(max_workers=level) as executor:
                futures = [executor.submit(self._worker_task) for _ in range(requests_per_level)]
                
                for future in as_completed(futures):
                    res = future.result()
                    self.log_result("Scalability", level, res)
                    if res["success"]:
                        latencies.append(res["latency"])
                        success_count += 1
            
            total_time = time.time() - start_batch
            throughput = success_count / total_time if total_time > 0 else 0
            avg_lat = statistics.mean(latencies) if latencies else 0
            p95_lat = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else avg_lat
            
            print(f"   Completed {requests_per_level} requests in {total_time:.2f}s")
            print(f"   Throughput: {throughput:.2f} req/sec")
            print(f"   Avg Latency: {avg_lat:.2f}ms")
            print(f"   P95 Latency: {p95_lat:.2f}ms")
            
            results_summary.append({
                "concurrency": level,
                "throughput": throughput,
                "avg_latency": avg_lat
            })

        print("\n--- Scalability Summary ---")
        print(f"{'Clients':<10} | {'Throughput (RPS)':<18} | {'Avg Latency (ms)':<15}")
        for r in results_summary:
            print(f"{r['concurrency']:<10} | {r['throughput']:<18.2f} | {r['avg_latency']:<15.2f}")

    def run_fault_tolerance_test(self):
        """
        Experiment: Fault Tolerance.
        Sends requests continuously while prompting user to kill the Leader.
        """
        print("\n=== Running Fault Tolerance Experiment ===")
        print("Objective: Measure system behavior during leader failure and election.")
        print("INSTRUCTIONS: When you see 'Traffic flowing...', MANUALLY KILL THE LEADER PROCESS.")
        print("The system should timeout briefly, then recover when a Follower takes over.")
        
        duration = 30 # seconds
        print(f"Starting {duration} second continuous load test...")
        
        start_time = time.time()
        consecutive_failures = 0
        recovery_start = None
        recovery_time = 0
        
        while (time.time() - start_time) < duration:
            # Run one request at a time sequentially for clear timeline
            res = self._worker_task()
            self.log_result("FaultTolerance", 1, res)
            
            if res["success"]:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Success ({res['latency']:.0f}ms)")
                if consecutive_failures > 0:
                    # We just recovered
                    recovery_end = time.time()
                    if recovery_start:
                        recovery_time = recovery_end - recovery_start
                        print(f"\n*** SYSTEM RECOVERED! Downtime: {recovery_time:.2f} seconds ***\n")
                    consecutive_failures = 0
                    recovery_start = None
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Failed: {res.get('error')}")
                if consecutive_failures == 0:
                    recovery_start = time.time()
                    print("\n*** OUTAGE DETECTED ***")
                consecutive_failures += 1
            
            # Small sleep to make logs readable
            time.sleep(0.5)

# ==============================================================================
# MAIN MENU
# ==============================================================================
def run_functional_test():
    """Original functional verification"""
    client = TravelAgencyClient()
    try:
        print("1. System Status:", client.get_system_status())
        print("2. Booking Flight:", client.send_booking_request("SFO", "LAX", "2025-01-01"))
        print("3. Booking Hotel:", client.send_booking_request("NYC", "MIA", "2025-01-01", "hotel"))
    finally:
        client.close()

if __name__ == "__main__":
    print("Distributed Travel Agency - System Test Suite")
    print("---------------------------------------------")
    print("1. Run Functional Verification (Single Check)")
    print("2. Run Scalability Experiment (Load Test)")
    print("3. Run Fault Tolerance Experiment (Recovery Test)")
    
    choice = input("\nEnter choice (1-3): ")
    
    if choice == "1":
        run_functional_test()
    elif choice == "2":
        exp = PerformanceExperiment()
        exp.run_scalability_test()
        print(f"\nRaw data saved to {CSV_FILENAME}. Import this into Excel/Python for graphs.")
    elif choice == "3":
        exp = PerformanceExperiment()
        exp.run_fault_tolerance_test()
        print(f"\nRaw data saved to {CSV_FILENAME}.")
    else:
        print("Invalid choice.")