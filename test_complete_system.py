# Client script to test the complete distributed travel agency system.
# This demonstrates how Python nodes coordinate with C++ workers.

import zmq
import time

class TravelAgencyClient:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:5559") # Connect to leader's client port
        
    # Send a booking request to the distributed system
    def send_booking_request(self, origin, destination, date, travel_type="flight", passengers=1, customer_id="TEST001"):
        request = {
            "type": "booking_request",
            "data": {
                "customer_id": customer_id,
                "origin": origin,
                "destination": destination,
                "date": date,
                "travel_type": travel_type,
                "passengers": passengers,
                "request_id": f"REQ_{int(time.time())}"
            }
        }
        
        print(f"Sending booking request: {origin} → {destination} on {date}")
        self.socket.send_json(request)
        
        response = self.socket.recv_json()
        return response
    
    # Get status from the leader node
    def get_system_status(self):
        request = {"type": "status_request"}
        
        self.socket.send_json(request)
        response = self.socket.recv_json()
        return response
    
    def close(self):
        self.socket.close()
        self.context.term()

def main():
    client = TravelAgencyClient()
    
    try:
        print("=== Testing Distributed Travel Agency System ===\n")
        
        # Test 1: Get system status
        print("1. Getting system status...")
        status = client.get_system_status()
        print(f"   Raw response: {status}")
        
        # Handle different response formats
        if 'type' in status and status['type'] == 'status_response':
            print(f"   Leader ID: {status.get('leader_id', 'Unknown')}")
            print(f"   Log Index: {status.get('latest_log_index', 'Unknown')}")
        else:
            print(f"   Unexpected status response format: {status}")
        print()
        
        # Test 2: Book a flight
        print("2. Booking a flight...")
        result = client.send_booking_request(
            origin="SFO",
            destination="LAX", 
            date="2025-12-15",
            travel_type="flight",
            passengers=2,
            customer_id="CUST001"
        )
        
        print(f"   Raw booking response: {result}")
        
        # Handle different response formats
        if 'type' in result and result['type'] == 'booking_response':
            booking_result = result.get('result', {})
            if booking_result.get('success'):
                booking_resp = booking_result.get('booking_response', {})
                price = booking_result.get('price', 0)
                print(f"   ✅ Booking successful!")
                print(f"   Confirmation: {booking_resp.get('confirmation_code', 'N/A')}")
                print(f"   Price: ${price:.2f}")
                print(f"   Log index: {booking_result.get('log_index', 'N/A')}")
            else:
                print(f"   ❌ Booking failed: {booking_result}")
        else:
            print(f"   Unexpected booking response format: {result}")
        print()
        
        # Test 3: Book a hotel
        print("3. Booking a hotel...")
        result = client.send_booking_request(
            origin="NYC",
            destination="MIA",
            date="2025-12-20",
            travel_type="hotel",
            passengers=1,
            customer_id="CUST002"
        )
        
        if 'type' in result and result['type'] == 'booking_response':
            booking_result = result.get('result', {})
            if booking_result.get('success'):
                booking_resp = booking_result.get('booking_response', {})
                price = booking_result.get('price', 0)
                print(f"   ✅ Booking successful!")
                print(f"   Confirmation: {booking_resp.get('confirmation_code', 'N/A')}")
                print(f"   Price: ${price:.2f}")
            else:
                print(f"   ❌ Booking failed: {booking_result}")
        else:
            print(f"   Unexpected booking response format: {result}")
        print()
        
        # Test 4: Check status again to see log growth
        print("4. Final system status...")
        status = client.get_system_status()
        if 'type' in status and status['type'] == 'status_response':
            print(f"   Final Log Index: {status.get('latest_log_index', 'Unknown')}")
        
        print("\n=== Test completed! ===")
        print("Check the leader and follower logs to see distributed coordination in action.")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure all components are running:")
        print("1. C++ workers (availability, pricing, booking)")
        print("2. Python leader node")
        print("3. Python follower node(s)")
    finally:
        client.close()

if __name__ == "__main__":
    main()