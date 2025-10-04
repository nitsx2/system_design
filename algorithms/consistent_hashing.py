"""
Consistent Hashing Algorithm - Complete Guide and Implementation
===============================================================

This script explains what consistent hashing is and demonstrates how it works
to assign work/data to servers in a distributed system with detailed step-by-step explanations.
"""

import hashlib
import bisect
from typing import Dict, List, Optional, Set
import random

class ConsistentHashing:
    """
    WHAT THIS CLASS DOES:
    =====================
    This class implements a consistent hashing ring that distributes keys (data/requests) 
    across multiple server nodes with minimal redistribution when nodes are added/removed.
    
    KEY COMPONENTS:
    - ring: Dictionary mapping hash values to server nodes
    - sorted_hashes: Sorted list of all hash positions for quick binary search
    - nodes: Set of all physical server nodes
    - replicas: Number of virtual nodes per physical node for better distribution
    
    HOW IT WORKS STEP-BY-STEP:
    1. Creates a circular hash ring (0 to 2^256-1 using SHA-256)
    2. Places virtual copies of each server at different positions on the ring
    3. For any key, finds the first server clockwise from the key's hash position
    4. Uses binary search for O(log n) lookup performance
    """

    def __init__(self, nodes: List[str] = None, replicas: int = 3):
        """
        STEP-BY-STEP INITIALIZATION PROCESS:
        ===================================
        
        Step 1: Initialize empty data structures
        - ring: Maps hash positions to server nodes
        - sorted_hashes: Maintains sorted order for binary search
        - nodes: Tracks all physical servers
        
        Step 2: Set replica count (virtual nodes per physical node)
        - More replicas = better load distribution
        - Typical values: 3-10 replicas per node
        
        Step 3: Add initial nodes if provided
        - Calls add_node() for each server
        """
        print("🔧 INITIALIZING CONSISTENT HASH RING")
        print(f"   Virtual nodes per server: {replicas}")

        self.replicas = replicas
        self.ring = {}  # hash_value -> node_name mapping
        self.sorted_hashes = []  # sorted list for binary search O(log n)
        self.nodes = set()  # set of all physical nodes

        if nodes:
            print(f"   Adding {len(nodes)} initial nodes...")
            for node in nodes:
                self.add_node(node)

        print("✅ Hash ring initialized successfully")

    def _hash(self, key: str) -> int:
        """
        HASH FUNCTION EXPLANATION:
        =========================
        
        PURPOSE: Convert any string (server name, data key) to a position on the hash ring
        
        STEP-BY-STEP PROCESS:
        1. Take input string (e.g., "Server_A:0" or "user_data_123")
        2. Encode to UTF-8 bytes
        3. Apply SHA-256 cryptographic hash function
        4. Convert hexadecimal result to integer
        5. Return integer position on ring (0 to 2^256-1)
        
        WHY SHA-256?
        - Produces uniform distribution across hash space
        - 256-bit output provides huge ring space (prevents collisions)
        - Deterministic: same input always gives same output
        - Fast computation
        """
        hash_bytes = hashlib.sha256(key.encode('utf-8')).hexdigest()
        hash_int = int(hash_bytes, 16)
        print(f"   Hash '{key}' -> {hash_int} (hex: {hash_bytes[:8]}...)")
        return hash_int

    def add_node(self, node: str) -> None:
        """
        ADD NODE PROCESS (STEP-BY-STEP):
        ===============================
        
        This function adds a new server to the consistent hash ring.
        
        DETAILED STEPS:
        
        Step 1: Validate and track the new node
        - Add to self.nodes set to track all physical servers
        
        Step 2: Create virtual nodes (replicas)
        - Purpose: Better load distribution across the ring
        - Each physical node gets multiple positions on the ring
        - Virtual key format: "ServerName:ReplicaNumber" (e.g., "Server_A:0", "Server_A:1")
        
        Step 3: For each virtual node:
        a) Generate unique virtual key ("node_name:replica_index")
        b) Hash the virtual key to get ring position
        c) Store mapping: hash_position -> physical_node_name
        d) Insert hash position into sorted list (for binary search)
        
        Step 4: Update data structures
        - ring: Maps hash positions to nodes
        - sorted_hashes: Maintains sorted order for fast lookups
        
        MATHEMATICAL INSIGHT:
        With N physical nodes and R replicas each:
        - Total virtual nodes = N × R
        - Each physical node handles ~1/N of the load
        - More replicas = more even distribution
        """
        print(f"\n🔄 ADDING NODE: {node}")
        print(f"   Creating {self.replicas} virtual nodes...")

        self.nodes.add(node)
        added_positions = []

        # Create virtual nodes for better distribution
        for replica_index in range(self.replicas):
            # Step 1: Create unique virtual node identifier
            virtual_key = f"{node}:{replica_index}"

            # Step 2: Hash virtual node to get ring position
            hash_position = self._hash(virtual_key)

            # Step 3: Store mapping and maintain sorted order
            self.ring[hash_position] = node
            bisect.insort(self.sorted_hashes, hash_position)
            added_positions.append(hash_position)

            print(f"   Virtual node {virtual_key} placed at position {hash_position}")

        print(f"✅ Node {node} added with {self.replicas} virtual nodes")
        print(f"   Ring now has {len(self.ring)} total virtual nodes")

    def remove_node(self, node: str) -> None:
        """
        REMOVE NODE PROCESS (STEP-BY-STEP):
        ==================================
        
        This function removes a server from the consistent hash ring.
        
        DETAILED STEPS:
        
        Step 1: Validate node exists
        - Check if node is in self.nodes set
        - Return early if node doesn't exist
        
        Step 2: Remove from physical nodes tracking
        - Remove from self.nodes set
        
        Step 3: Remove all virtual nodes
        - For each replica (0 to replicas-1):
          a) Reconstruct virtual key ("node_name:replica_index")
          b) Hash virtual key to get position
          c) Remove from ring dictionary
          d) Remove from sorted_hashes list
        
        Step 4: Data redistribution happens automatically
        - Keys previously handled by removed node
        - Now handled by next node clockwise on ring
        - No explicit data movement in this implementation
        
        FAILURE IMPACT:
        - Only keys mapped to failed node are affected
        - Represents ~1/N of total data (where N = number of nodes)
        - Other nodes unaffected
        """
        if node not in self.nodes:
            print(f"❌ Node {node} not found in ring")
            return

        print(f"\n🗑️  REMOVING NODE: {node}")
        self.nodes.remove(node)
        removed_positions = []

        # Remove all virtual nodes for this physical node
        for replica_index in range(self.replicas):
            virtual_key = f"{node}:{replica_index}"
            hash_position = self._hash(virtual_key)

            if hash_position in self.ring:
                del self.ring[hash_position]
                self.sorted_hashes.remove(hash_position)
                removed_positions.append(hash_position)
                print(f"   Removed virtual node {virtual_key} from position {hash_position}")

        print(f"✅ Node {node} removed completely")
        print(f"   Ring now has {len(self.ring)} total virtual nodes")

    def get_node(self, key: str) -> Optional[str]:
        """
        KEY LOOKUP PROCESS (STEP-BY-STEP):
        =================================
        
        This is the CORE function that determines which server handles a given key.
        
        DETAILED ALGORITHM:
        
        Step 1: Handle empty ring
        - If no servers exist, return None
        
        Step 2: Hash the key
        - Convert key (e.g., "user_123") to ring position
        - Uses same hash function as servers for consistency
        
        Step 3: Find responsible server (CLOCKWISE SEARCH)
        - Use binary search on sorted_hashes list
        - Find first server position >= key's hash position
        - If key hashes beyond last server, wrap around to first server
        
        Step 4: Return the physical server name
        - Look up server name from ring dictionary
        
        WHY BINARY SEARCH?
        - sorted_hashes maintains ring positions in order
        - bisect.bisect_right() finds insertion point in O(log n) time
        - Much faster than linear search O(n)
        
        THE CLOCKWISE RULE:
        - Key goes to first server found moving clockwise
        - Ensures consistent assignment
        - Creates responsibility ranges for each server
        
        MATHEMATICAL EXAMPLE:
        Ring positions: [100, 300, 500, 800]
        Key hashes to 250:
        - Binary search finds position 1 (value 300)
        - Server at position 300 handles this key
        
        Key hashes to 900:
        - Binary search returns position 4 (past end)
        - Wrap around: use position 0 (value 100)
        """
        if not self.ring:
            print(f"   ❌ No servers available for key '{key}'")
            return None

        # Step 1: Hash the key to get ring position
        key_hash = self._hash(key)

        # Step 2: Binary search for first server >= key_hash (clockwise)
        # bisect_right returns insertion point for key_hash
        server_index = bisect.bisect_right(self.sorted_hashes, key_hash)

        # Step 3: Handle wrap-around (circular ring)
        if server_index == len(self.sorted_hashes):
            server_index = 0  # Wrap to beginning of ring

        # Step 4: Get server position and look up server name
        server_hash_position = self.sorted_hashes[server_index]
        responsible_server = self.ring[server_hash_position]

        print(f"   📍 Key '{key}' (hash: {key_hash}) -> {responsible_server}")
        print(f"      Server position: {server_hash_position}")

        return responsible_server

    def get_nodes_for_keys(self, keys: List[str]) -> Dict[str, str]:
        """
        BATCH KEY LOOKUP:
        ================
        
        PURPOSE: Efficiently process multiple keys at once
        
        PROCESS:
        1. Iterate through all keys
        2. Call get_node() for each key
        3. Return dictionary mapping key -> server
        
        USEFUL FOR:
        - Analyzing load distribution
        - Batch operations
        - System monitoring
        """
        print(f"\n📊 PROCESSING BATCH OF {len(keys)} KEYS")
        result = {}
        for key in keys:
            result[key] = self.get_node(key)
        return result

    def print_ring_status(self):
        """
        RING ANALYSIS AND STATISTICS:
        ============================
        
        This function provides comprehensive information about the hash ring state.
        
        ANALYSIS PERFORMED:
        
        1. Basic Ring Statistics:
        - Physical nodes count
        - Virtual nodes count  
        - Replicas per node
        
        2. Load Distribution Analysis:
        - Generate 1000 test keys
        - Assign each key to responsible server
        - Calculate percentage load per server
        - Identify load imbalances
        
        3. Distribution Quality Assessment:
        - Ideal load: 100% / number_of_servers
        - Measure deviation from ideal
        - Report distribution quality
        
        WHY 1000 TEST KEYS?
        - Large enough sample for statistical accuracy
        - Small enough for fast computation
        - Good balance for load testing
        """
        print("\n" + "="*60)
        print("📈 CONSISTENT HASH RING ANALYSIS")
        print("="*60)
        print(f"Physical Servers: {len(self.nodes)}")
        print(f"Virtual Nodes (Total): {len(self.ring)}")
        print(f"Replicas per Server: {self.replicas}")
        print(f"Active Servers: {', '.join(sorted(self.nodes))}")

        if not self.nodes:
            print("⚠️  No servers in ring!")
            return

        # Load distribution analysis with sample keys
        print(f"\n🧪 LOAD DISTRIBUTION TEST (1000 sample keys):")
        test_keys = [f"test_key_{i}" for i in range(1000)]
        server_counts = {}

        for key in test_keys:
            server = self.get_node(key)
            server_counts[server] = server_counts.get(server, 0) + 1

        # Calculate and display load percentages
        ideal_load = 1000 / len(self.nodes)
        total_deviation = 0

        for server in sorted(self.nodes):
            count = server_counts.get(server, 0)
            percentage = (count / 1000) * 100
            deviation = abs(count - ideal_load)
            total_deviation += deviation

            # Visual load bar
            bar_length = int(percentage // 2)
            bar = "█" * bar_length

            print(f"  {server:15}: {count:3d} keys ({percentage:5.1f}%) {bar}")

        # Distribution quality assessment
        avg_deviation = total_deviation / len(self.nodes)
        if avg_deviation < ideal_load * 0.1:
            quality = "🟢 Excellent"
        elif avg_deviation < ideal_load * 0.2:
            quality = "🟡 Good"
        else:
            quality = "🔴 Poor"

        print(f"\n📊 DISTRIBUTION QUALITY: {quality}")
        print(f"   Ideal load per server: {ideal_load:.1f} keys")
        print(f"   Average deviation: {avg_deviation:.1f} keys")


def explain_consistent_hashing():
    """
    EDUCATIONAL EXPLANATION FUNCTION:
    ================================
    
    PURPOSE: Provide comprehensive theoretical background
    
    COVERS:
    1. What consistent hashing solves
    2. How it works conceptually
    3. Key benefits and use cases
    4. Real-world applications
    5. Comparison with traditional hashing
    
    TEACHING APPROACH:
    - Visual formatting with boxes and symbols
    - Step-by-step breakdown
    - Real-world examples
    - Problem-solution structure
    """
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                    WHAT IS CONSISTENT HASHING?                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    
    🎯 THE CORE PROBLEM IT SOLVES:
    Traditional hashing: server = hash(key) % num_servers
    
    ISSUES:
    ❌ Adding/removing servers requires redistributing ALL data
    ❌ System becomes unstable during scaling
    ❌ Cache invalidation nightmares
    ❌ Hot spots and uneven load distribution
    
    🔄 CONSISTENT HASHING SOLUTION:
    
    1. CIRCULAR HASH RING (0 to 2^256-1 using SHA-256)
       - Imagine a clock face with billions of positions
       - Both servers and keys are placed on this ring
    
    2. CLOCKWISE ASSIGNMENT RULE
       - Each key goes to the first server found clockwise
       - Creates responsibility ranges for each server
    
    3. VIRTUAL NODES (REPLICAS)
       - Each physical server appears multiple times on ring
       - Better load distribution and fault tolerance
    
    4. MINIMAL REDISTRIBUTION
       - Only keys between old and new server positions move
       - Approximately 1/N of data affected (N = number of servers)
    
    ⚡ KEY BENEFITS:
    ✅ Only 1/N keys move when scaling (vs 100% in traditional)
    ✅ System remains stable during server changes
    ✅ Even load distribution with virtual nodes
    ✅ Fault tolerance - failed server's load spreads to neighbors
    ✅ No single point of failure
    
    🏢 REAL-WORLD USAGE:
    - Amazon DynamoDB (data partitioning)
    - Apache Cassandra (data distribution)
    - Redis Cluster (sharding)
    - CDNs (content distribution)
    - Load balancers (request routing)
    - Distributed caches (Memcached)
    """)


def demonstrate_traditional_vs_consistent():
    """
    COMPARISON DEMONSTRATION FUNCTION:
    ================================
    
    PURPOSE: Show dramatic difference between hashing approaches
    
    DEMONSTRATION PROCESS:
    
    1. TRADITIONAL HASHING SIMULATION:
    - Use modulo operation: hash(key) % num_servers
    - Start with 3 servers
    - Add 4th server
    - Count how many keys change servers
    
    2. CONSISTENT HASHING SIMULATION:
    - Use consistent hash ring
    - Start with same 3 servers
    - Add same 4th server
    - Count key movements
    
    3. COMPARISON ANALYSIS:
    - Calculate percentages
    - Show visual difference
    - Explain why consistent hashing is better
    
    EXPECTED RESULTS:
    - Traditional: ~75% of keys move
    - Consistent: ~25% of keys move
    """
    print("\n" + "="*70)
    print("🔬 TRADITIONAL vs CONSISTENT HASHING COMPARISON")
    print("="*70)

    # Test dataset
    test_keys = [f"user_data_{i}" for i in range(100, 120)]

    print("\n1️⃣  TRADITIONAL HASHING (hash % num_servers):")
    print("-" * 50)

    # Traditional hashing with 3 servers
    servers_3 = ["Server_A", "Server_B", "Server_C"]
    print("📊 Initial state with 3 servers:")
    traditional_mapping_3 = {}

    for key in test_keys:
        # Traditional hash: use built-in hash function + modulo
        hash_val = hash(key)  # Python's built-in hash
        server_idx = hash_val % 3
        server = servers_3[server_idx]
        traditional_mapping_3[key] = server
        print(f"   {key} -> {server}")

    # Add fourth server
    servers_4 = ["Server_A", "Server_B", "Server_C", "Server_D"]
    print(f"\n🔄 After adding Server_D (now 4 servers):")
    traditional_changes = 0

    for key in test_keys:
        hash_val = hash(key)
        server_idx = hash_val % 4  # Now modulo 4
        new_server = servers_4[server_idx]

        if traditional_mapping_3[key] != new_server:
            print(f"   {key} -> {new_server} (❌ MOVED from {traditional_mapping_3[key]})")
            traditional_changes += 1
        else:
            print(f"   {key} -> {new_server} (✅ stayed)")

    traditional_percentage = (traditional_changes / len(test_keys)) * 100
    print(f"\n❌ TRADITIONAL RESULT: {traditional_changes}/{len(test_keys)} keys moved ({traditional_percentage:.1f}%)")

    print("\n2️⃣  CONSISTENT HASHING:")
    print("-" * 50)

    # Consistent hashing with 3 servers
    ch = ConsistentHashing(["Server_A", "Server_B", "Server_C"], replicas=5)
    print("📊 Initial state with 3 servers:")
    consistent_mapping_3 = {}

    for key in test_keys:
        server = ch.get_node(key)
        consistent_mapping_3[key] = server
        print(f"   {key} -> {server}")

    # Add fourth server
    print(f"\n🔄 After adding Server_D:")
    ch.add_node("Server_D")
    consistent_changes = 0

    for key in test_keys:
        new_server = ch.get_node(key)
        if consistent_mapping_3[key] != new_server:
            print(f"   {key} -> {new_server} (❌ MOVED from {consistent_mapping_3[key]})")
            consistent_changes += 1
        else:
            print(f"   {key} -> {new_server} (✅ stayed)")

    consistent_percentage = (consistent_changes / len(test_keys)) * 100
    print(f"\n✅ CONSISTENT RESULT: {consistent_changes}/{len(test_keys)} keys moved ({consistent_percentage:.1f}%)")

    # Summary comparison
    print(f"\n📊 COMPARISON SUMMARY:")
    print(f"   Traditional Hashing: {traditional_percentage:.1f}% of keys moved")
    print(f"   Consistent Hashing:  {consistent_percentage:.1f}% of keys moved")
    print(f"   Improvement: {traditional_percentage - consistent_percentage:.1f} percentage points better!")

    if consistent_percentage < traditional_percentage:
        print("🎉 Consistent hashing significantly reduces data movement!")
    else:
        print("⚠️  This sample shows variable results - larger datasets show clearer benefits")


def demonstrate_load_balancing():
    """
    LOAD BALANCING DEMONSTRATION:
    ============================
    
    PURPOSE: Show how consistent hashing distributes requests across servers
    
    SIMULATION PROCESS:
    
    1. CREATE REALISTIC SCENARIO:
    - 4 web servers
    - 5 virtual nodes per server (better distribution)
    - 4000 diverse requests (user profiles, products, searches, API calls)
    
    2. DISTRIBUTE REQUESTS:
    - Hash each request
    - Find responsible server using clockwise rule
    - Count requests per server
    
    3. ANALYZE DISTRIBUTION:
    - Calculate load percentages
    - Create visual bar chart
    - Assess distribution quality
    
    EXPECTED OUTCOME:
    - Each server handles ~25% of requests
    - Deviation should be minimal (within 5%)
    - Visual confirmation of even distribution
    """
    print("\n" + "="*70)
    print("⚖️  LOAD BALANCING DEMONSTRATION")
    print("="*70)

    # Create realistic web server setup
    servers = ["WebServer_01", "WebServer_02", "WebServer_03", "WebServer_04"]
    ch = ConsistentHashing(servers, replicas=5)  # More replicas = better distribution

    print(f"🖥️  Simulating load balancing across {len(servers)} web servers")
    print(f"🔄 Using {ch.replicas} virtual nodes per server for even distribution")

    # Generate diverse request types
    requests = []
    request_categories = [
        "user_profile", "product_page", "search_query", "api_call",
        "login_request", "checkout_process", "image_upload", "data_export"
    ]

    for i in range(500):  # 500 requests per category
        for category in request_categories:
            request_id = f"{category}_{i}_{random.randint(1000, 9999)}"
            requests.append(request_id)

    print(f"📊 Processing {len(requests)} requests...")

    # Distribute requests to servers
    server_loads = {server: 0 for server in servers}
    request_distribution = {}

    for request in requests:
        assigned_server = ch.get_node(request)
        server_loads[assigned_server] += 1
        request_distribution[request] = assigned_server

    # Display load distribution
    print(f"\n📈 LOAD DISTRIBUTION RESULTS:")
    print(f"{'Server':<15} {'Requests':<10} {'Percentage':<12} {'Visual'}")
    print("-" * 60)

    total_requests = len(requests)
    ideal_load = total_requests / len(servers)
    load_values = []

    for server in sorted(servers):
        load = server_loads[server]
        percentage = (load / total_requests) * 100
        load_values.append(load)

        # Create visual bar (each █ represents ~2%)
        bar_length = int(percentage // 2)
        bar = "█" * bar_length

        print(f"{server:<15} {load:<10} {percentage:>6.1f}%     {bar}")

    # Statistical analysis
    avg_load = sum(load_values) / len(load_values)
    max_load = max(load_values)
    min_load = min(load_values)
    load_range = max_load - min_load

    print(f"\n📊 STATISTICAL ANALYSIS:")
    print(f"   Total requests processed: {total_requests:,}")
    print(f"   Average load per server: {avg_load:.1f} requests")
    print(f"   Ideal load per server: {ideal_load:.1f} requests")
    print(f"   Load range (max - min): {load_range} requests")
    print(f"   Load deviation: {(load_range/avg_load)*100:.1f}%")

    # Quality assessment
    if load_range < avg_load * 0.1:
        quality = "🟢 EXCELLENT - Very even distribution"
    elif load_range < avg_load * 0.2:
        quality = "🟡 GOOD - Acceptable distribution"
    else:
        quality = "🔴 POOR - Uneven distribution"

    print(f"   Distribution quality: {quality}")


def demonstrate_fault_tolerance():
    """
    FAULT TOLERANCE DEMONSTRATION:
    =============================
    
    PURPOSE: Show how consistent hashing handles server failures gracefully
    
    FAILURE SIMULATION PROCESS:
    
    1. INITIAL SETUP:
    - 5 database servers
    - 20 data keys distributed across servers
    - Record initial distribution
    
    2. SIMULATE SERVER FAILURE:
    - Remove one server from ring
    - Redistribute affected keys to next clockwise server
    - Measure impact
    
    3. ANALYZE IMPACT:
    - Count affected vs unaffected keys
    - Show minimal disruption
    - Demonstrate automatic failover
    
    4. SIMULATE RECOVERY:
    - Add replacement server
    - Show load rebalancing
    
    KEY INSIGHTS:
    - Only keys from failed server are affected
    - No cascading failures
    - System continues operating
    - Easy recovery process
    """
    print("\n" + "="*70)
    print("🛡️  FAULT TOLERANCE DEMONSTRATION")
    print("="*70)

    # Setup distributed database scenario
    database_servers = ["DB_Primary", "DB_Secondary", "DB_Backup", "DB_Analytics", "DB_Cache"]
    ch = ConsistentHashing(database_servers, replicas=3)

    print(f"🗄️  Database cluster with {len(database_servers)} servers")
    print(f"📊 Initial server distribution:")

    # Sample data distribution
    data_keys = [
        "customer_profiles", "order_history", "product_catalog", "user_sessions",
        "payment_data", "inventory_levels", "analytics_data", "search_indexes",
        "cache_objects", "log_files", "backup_metadata", "system_configs",
        "user_preferences", "transaction_logs", "audit_trails", "performance_metrics",
        "security_tokens", "api_keys", "notification_queue", "temp_storage"
    ]

    # Record initial distribution
    initial_distribution = {}
    server_data_count = {}

    for key in data_keys:
        server = ch.get_node(key)
        initial_distribution[key] = server
        server_data_count[server] = server_data_count.get(server, 0) + 1
        print(f"   📁 {key:<20} -> {server}")

    # Show initial load distribution
    ch.print_ring_status()

    # SIMULATE SERVER FAILURE
    failed_server = "DB_Secondary"
    print(f"\n🚨 CRITICAL FAILURE: {failed_server} has crashed!")
    print(f"⚠️  Removing {failed_server} from cluster...")

    ch.remove_node(failed_server)

    # Analyze failure impact
    print(f"\n🔄 AUTOMATIC FAILOVER - Redistributing data...")
    affected_keys = []
    unaffected_keys = []
    new_distribution = {}

    for key in data_keys:
        new_server = ch.get_node(key)
        new_distribution[key] = new_server

        if initial_distribution[key] != new_server:
            affected_keys.append(key)
            print(f"   📁 {key:<20} -> {new_server} (🔄 MOVED from {initial_distribution[key]})")
        else:
            unaffected_keys.append(key)
            print(f"   📁 {key:<20} -> {new_server} (✅ unchanged)")

    # Impact analysis
    total_keys = len(data_keys)
    affected_count = len(affected_keys)
    unaffected_count = len(unaffected_keys)

    print(f"\n📊 FAILURE IMPACT ANALYSIS:")
    print(f"   📁 Total data objects: {total_keys}")
    print(f"   🔄 Affected objects: {affected_count} ({(affected_count/total_keys)*100:.1f}%)")
    print(f"   ✅ Unaffected objects: {unaffected_count} ({(unaffected_count/total_keys)*100:.1f}%)")
    print(f"   🎯 Theoretical impact: ~{100/len(database_servers):.1f}% (1/{len(database_servers)} servers)")

    if affected_count <= total_keys // (len(database_servers) - 1):
        print("   🟢 Impact within expected bounds - Excellent fault tolerance!")
    else:
        print("   🟡 Impact slightly higher than expected - Still manageable")

    ch.print_ring_status()

    # SIMULATE RECOVERY
    print(f"\n🔄 RECOVERY: Adding replacement server 'DB_NewNode'")
    ch.add_node("DB_NewNode")

    # Analyze recovery redistribution
    print(f"\n📊 POST-RECOVERY LOAD REBALANCING:")
    recovery_changes = 0
    for key in data_keys:
        recovery_server = ch.get_node(key)
        if new_distribution[key] != recovery_server:
            recovery_changes += 1
            print(f"   📁 {key:<20} -> {recovery_server} (🔄 rebalanced)")
        else:
            print(f"   📁 {key:<20} -> {recovery_server} (✅ stable)")

    print(f"\n✅ RECOVERY COMPLETE:")
    print(f"   🔄 Keys rebalanced during recovery: {recovery_changes}")
    print(f"   🛡️  Cluster restored to full redundancy")

    ch.print_ring_status()


def demonstrate_virtual_nodes():
    """
    VIRTUAL NODES DEMONSTRATION:
    ===========================
    
    PURPOSE: Show why virtual nodes (replicas) are crucial for even distribution
    
    COMPARISON PROCESS:
    
    1. TEST DIFFERENT REPLICA COUNTS:
    - 1 replica per server (poor distribution)
    - 3 replicas per server (good distribution) 
    - 10 replicas per server (excellent distribution)
    
    2. MEASURE DISTRIBUTION QUALITY:
    - Use 300 test keys for statistical significance
    - Calculate standard deviation of load
    - Compare actual vs ideal distribution
    
    3. EXPLAIN THE MATH:
    - More virtual nodes = more mixing on ring
    - Better statistical distribution
    - Reduces hot spots
    
    WHY VIRTUAL NODES MATTER:
    - Real servers might hash to nearby positions
    - Creates uneven responsibility ranges
    - Virtual nodes spread server presence across ring
    """
    print("\n" + "="*70)
    print("🎭 VIRTUAL NODES (REPLICAS) DEMONSTRATION")
    print("="*70)

    servers = ["Alpha_Server", "Beta_Server", "Gamma_Server"]
    test_keys = [f"data_object_{i}" for i in range(300)]

    print(f"🧪 Testing load distribution with different replica counts")
    print(f"📊 Using {len(test_keys)} test keys across {len(servers)} servers")
    print(f"🎯 Ideal load per server: {len(test_keys)/len(servers):.1f} keys (33.3%)")

    replica_counts = [1, 3, 10]

    for replicas in replica_counts:
        print(f"\n{'='*20} TESTING {replicas} REPLICA(S) PER SERVER {'='*20}")

        ch = ConsistentHashing(servers, replicas=replicas)

        # Distribute test keys
        server_counts = {server: 0 for server in servers}
        for key in test_keys:
            server = ch.get_node(key)
            server_counts[server] += 1

        # Display distribution
        print(f"📈 Load distribution results:")
        load_values = []
        for server in sorted(servers):
            count = server_counts[server]
            percentage = (count / len(test_keys)) * 100
            load_values.append(count)

            # Visual representation
            bar_length = int(percentage // 3)  # Each █ represents ~3%
            bar = "█" * bar_length
            deviation = count - (len(test_keys) / len(servers))

            print(f"   {server:<15}: {count:3d} keys ({percentage:5.1f}%) {bar} (deviation: {deviation:+.1f})")

        # Statistical analysis
        ideal_load = len(test_keys) / len(servers)
        variance = sum((load - ideal_load) ** 2 for load in load_values) / len(load_values)
        std_deviation = variance ** 0.5
        coefficient_of_variation = (std_deviation / ideal_load) * 100

        print(f"\n📊 Statistical Metrics:")
        print(f"   Standard deviation: {std_deviation:.2f} keys")
        print(f"   Coefficient of variation: {coefficient_of_variation:.1f}%")

        # Quality assessment
        if coefficient_of_variation < 5:
            quality = "🟢 EXCELLENT"
            description = "Very even distribution"
        elif coefficient_of_variation < 15:
            quality = "🟡 GOOD"
            description = "Acceptable distribution"
        else:
            quality = "🔴 POOR"
            description = "Uneven distribution"

        print(f"   Distribution quality: {quality} - {description}")
        print(f"   Virtual nodes created: {len(servers)} × {replicas} = {len(servers) * replicas}")

    print(f"\n🎯 KEY INSIGHTS:")
    print(f"   • More replicas = better load distribution")
    print(f"   • Replicas spread server presence across the ring")
    print(f"   • Reduces impact of unlucky hash positioning")
    print(f"   • Trade-off: more replicas = more memory usage")
    print(f"   • Sweet spot: usually 3-10 replicas per server")


def practical_example_cache_system():
    """
    PRACTICAL CACHE SYSTEM EXAMPLE:
    ===============================
    
    PURPOSE: Real-world application showing distributed caching with consistent hashing
    
    CACHE SYSTEM FEATURES:
    
    1. DISTRIBUTED STORAGE:
    - Multiple cache servers
    - Data distributed via consistent hashing
    - Each server maintains local storage
    
    2. OPERATIONS:
    - PUT: Store key-value pair on appropriate server
    - GET: Retrieve value from appropriate server
    - Automatic server selection via hashing
    
    3. SCALING:
    - Add new cache servers dynamically
    - Minimal data redistribution
    - Continued operation during scaling
    
    4. MONITORING:
    - Track cache utilization per server
    - Monitor distribution efficiency
    
    REAL-WORLD APPLICABILITY:
    - Redis clusters
    - Memcached deployments
    - CDN edge caching
    - Application-level caching
    """
    print("\n" + "="*70)
    print("💾 PRACTICAL EXAMPLE: DISTRIBUTED CACHE SYSTEM")
    print("="*70)

    class DistributedCache:
        """
        DISTRIBUTED CACHE IMPLEMENTATION:
        ================================
        
        This class simulates a real distributed cache system using consistent hashing.
        
        ARCHITECTURE:
        - ConsistentHashing for server selection
        - Local dictionaries simulate cache servers
        - Automatic routing based on key hashing
        
        OPERATIONS:
        - put(): Store data on correct server
        - get(): Retrieve data from correct server  
        - add_cache_server(): Scale the cache cluster
        - get_cache_stats(): Monitor system health
        """

        def __init__(self, cache_servers: List[str]):
            print(f"🚀 Initializing distributed cache with {len(cache_servers)} servers")
            self.ch = ConsistentHashing(cache_servers, replicas=5)
            # Each server has its own local cache (simulated with dict)
            self.local_caches = {server: {} for server in cache_servers}
            self.total_operations = 0

        def put(self, key: str, value: str) -> bool:
            """Store key-value pair in appropriate cache server"""
            server = self.ch.get_node(key)
            if server:
                self.local_caches[server][key] = value
                self.total_operations += 1
                print(f"   💾 STORED '{key}' = '{value}' on {server}")
                return True
            return False

        def get(self, key: str) -> Optional[str]:
            """Retrieve value from appropriate cache server"""
            server = self.ch.get_node(key)
            if server:
                value = self.local_caches[server].get(key)
                result = "HIT" if value else "MISS"
                print(f"   🔍 GET '{key}' from {server}: {result}")
                return value
            return None

        def add_cache_server(self, server: str):
            """Add new cache server to the cluster"""
            print(f"\n🔄 SCALING UP: Adding cache server '{server}'")
            self.local_caches[server] = {}
            self.ch.add_node(server)
            print(f"✅ Cluster now has {len(self.local_caches)} cache servers")

        def get_cache_stats(self):
            """Display cache utilization statistics"""
            print(f"\n📊 CACHE CLUSTER STATISTICS:")
            print(f"   Total servers: {len(self.local_caches)}")
            print(f"   Total operations: {self.total_operations}")

            total_items = 0
            for server, cache in self.local_caches.items():
                item_count = len(cache)
                total_items += item_count
                print(f"   {server}: {item_count} cached items")

            print(f"   Total cached items: {total_items}")

            if total_items > 0:
                avg_items = total_items / len(self.local_caches)
                print(f"   Average per server: {avg_items:.1f} items")

    # CREATE DISTRIBUTED CACHE SYSTEM
    initial_servers = ["Cache_US_East", "Cache_US_West", "Cache_EU_Central"]
    cache_system = DistributedCache(initial_servers)

    print(f"🌐 Distributed cache system operational")
    print(f"📍 Initial cache servers: {', '.join(initial_servers)}")

    # SIMULATE CACHE OPERATIONS
    print(f"\n💾 STORING DATA IN DISTRIBUTED CACHE:")

    cache_data = {
        # User data
        "user:1001:profile": "{'name': 'John Doe', 'age': 30}",
        "user:1002:profile": "{'name': 'Jane Smith', 'age': 25}",
        "user:1003:profile": "{'name': 'Bob Johnson', 'age': 35}",

        # Session data
        "session:abc123": "{'user_id': 1001, 'login_time': '2025-10-03T02:53:00Z'}",
        "session:def456": "{'user_id': 1002, 'login_time': '2025-10-03T02:54:00Z'}",

        # Product data
        "product:laptop:5001": "{'name': 'Gaming Laptop', 'price': 1299.99}",
        "product:phone:5002": "{'name': 'Smartphone', 'price': 699.99}",

        # Search results
        "search:laptops:results": "['laptop1', 'laptop2', 'laptop3']",
        "search:phones:results": "['phone1', 'phone2', 'phone3']",

        # API responses
        "api:weather:newyork": "{'temp': 22, 'condition': 'sunny'}",
        "api:weather:london": "{'temp': 15, 'condition': 'cloudy'}"
    }

    # Store all data
    for key, value in cache_data.items():
        cache_system.put(key, value)

    cache_system.get_cache_stats()

    # DEMONSTRATE CACHE RETRIEVAL
    print(f"\n🔍 RETRIEVING DATA FROM CACHE:")

    test_keys = [
        "user:1001:profile",
        "session:abc123",
        "product:laptop:5001",
        "api:weather:newyork",
        "nonexistent:key"  # This will be a cache miss
    ]

    for key in test_keys:
        cache_system.get(key)

    # DEMONSTRATE SCALING
    cache_system.add_cache_server("Cache_ASIA_Pacific")

    # Show how new server affects distribution
    print(f"\n📊 LOAD DISTRIBUTION AFTER SCALING:")
    cache_system.get_cache_stats()

    # Store additional data to show new server usage
    print(f"\n💾 STORING ADDITIONAL DATA (will use new server):")
    additional_data = {
        "user:2001:profile": "{'name': 'Alice Wong', 'region': 'APAC'}",
        "user:2002:profile": "{'name': 'Chen Li', 'region': 'APAC'}",
        "session:ghi789": "{'user_id': 2001, 'region': 'APAC'}"
    }

    for key, value in additional_data.items():
        cache_system.put(key, value)
        cache_system.get(key)  # Immediate retrieval test

    # Final statistics
    cache_system.get_cache_stats()

    print(f"\n🎯 CACHE SYSTEM BENEFITS DEMONSTRATED:")
    print(f"   ✅ Automatic data distribution across servers")
    print(f"   ✅ Consistent key-to-server mapping")
    print(f"   ✅ Seamless scaling with minimal disruption")
    print(f"   ✅ Load balancing across all cache servers")
    print(f"   ✅ High availability (no single point of failure)")


if __name__ == "__main__":
    print("🔧 CONSISTENT HASHING - STEP-BY-STEP DEMONSTRATION")
    print("=" * 70)
    print("This script demonstrates consistent hashing with detailed explanations")
    print("of every step in the algorithm and implementation.")
    print("=" * 70)

    # Run all demonstrations with detailed explanations
    explain_consistent_hashing()
    demonstrate_traditional_vs_consistent()
    demonstrate_load_balancing()
    demonstrate_fault_tolerance()
    demonstrate_virtual_nodes()
    practical_example_cache_system()

    print("\n" + "=" * 70)
    print("🎓 CONSISTENT HASHING MASTERY COMPLETE!")
    print("=" * 70)

    print("""
    🎯 ALGORITHM SUMMARY:
    
    1. Hash Ring Creation:
       • Use cryptographic hash (SHA-256) for uniform distribution  
       • Create circular space (0 to 2^256-1)
       • Place virtual nodes on ring for each physical server
    
    2. Key Assignment Process:
       • Hash the key to get position on ring
       • Use binary search to find next server clockwise
       • Assign key to that server (O(log n) lookup time)
    
    3. Scaling Operations:
       • Add server: Create virtual nodes, minimal key redistribution
       • Remove server: Delete virtual nodes, keys move to next server
       • Only ~1/N of keys affected (vs ~100% in traditional hashing)
    
    4. Load Balancing:
       • Virtual nodes provide even distribution
       • More replicas = better balance (typically 3-10 per server)
       • Automatic load spreading across all servers
    
    💡 REAL-WORLD APPLICATIONS:
    • Database sharding (Cassandra, MongoDB)
    • Distributed caching (Redis, Memcached)
    • Load balancing (HAProxy, NGINX)
    • CDN content distribution
    • Microservices routing
    
    🚀 USE CONSISTENT HASHING WHEN YOU NEED:
    • Horizontal scaling with minimal disruption
    • Even load distribution across servers  
    • Fault tolerance in distributed systems
    • Efficient cache partitioning
    • Predictable performance during scaling
    """)
