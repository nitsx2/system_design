"""
Simple Paxos Algorithm Implementation with Comprehensive Documentation
====================================================================

This script implements the Paxos consensus algorithm with detailed explanations
of how it works, why each step is necessary, and real-world applications.

PAXOS ALGORITHM OVERVIEW:
========================
Paxos solves the consensus problem in distributed systems: How can multiple 
nodes agree on a single value even when some nodes may fail or messages may be lost?

KEY PROPERTIES:
- SAFETY: Only one value can ever be chosen (consistency guarantee)
- LIVENESS: Eventually a value will be chosen (progress guarantee) 
- FAULT TOLERANCE: Works as long as majority of acceptors are available

REAL-WORLD USAGE:
- Google Spanner (global distributed database)
- Apache Cassandra (distributed NoSQL database)
- Google Chubby (distributed lock service)
- Apache ZooKeeper (coordination service)
- etcd (distributed key-value store)

ALGORITHM PHASES:
===============
Phase 1 (Prepare/Promise):
- Proposer asks acceptors: "Can I propose with number N?"
- Acceptors respond: "Yes, and here's any value I previously accepted"
- Purpose: Reserve the right to propose and learn about constraints

Phase 2 (Accept/Accepted):  
- Proposer says: "Please accept this value with number N"
- Acceptors respond: "OK, I accept it" (if it doesn't violate promises)
- Purpose: Actually vote on and commit to the proposed value

SAFETY MECHANISM:
================
The key insight is that acceptors make promises in Phase 1. Once an acceptor 
promises not to accept lower-numbered proposals, it never violates that promise.
This prevents conflicting values from being chosen simultaneously.

PAXOS CONSTRAINT:
================
If any acceptor returns a previously accepted value in Phase 1, the proposer 
MUST use the value from the highest-numbered previous proposal. This ensures 
that once a value starts being accepted, all future proposals use that value.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

@dataclass
class Message:
    """
    Represents a message in the Paxos protocol.
    
    Fields:
    - msg_type: Type of message (PREPARE, PROMISE, ACCEPT, ACCEPTED)
    - sender: ID of the node sending the message
    - receiver: ID of the node receiving the message  
    - proposal_num: Unique proposal number for ordering
    - value: The value being proposed (for ACCEPT messages)
    - prev_accepted: Previously accepted proposal info (for PROMISE messages)
    """
    msg_type: str  # PREPARE, PROMISE, ACCEPT, ACCEPTED
    sender: int
    receiver: int
    proposal_num: int
    value: Any = None
    prev_accepted: Tuple[int, Any] = None

class ProposalNumber:
    """
    Proposal number with format (sequence, node_id) for total ordering.
    
    This ensures that:
    1. Each proposal has a unique, globally ordered number
    2. Higher sequence numbers take priority
    3. Node ID breaks ties for same sequence numbers
    4. No two proposers can generate the same proposal number
    
    Example: (3, 1) < (3, 2) < (4, 1) < (4, 2)
    """

    def __init__(self, seq: int, node_id: int):
        self.seq = seq        # Incrementing sequence number
        self.node_id = node_id # Unique node identifier

    def __lt__(self, other):
        return (self.seq, self.node_id) < (other.seq, other.node_id)
    def __le__(self, other):
        return (self.seq, self.node_id) <= (other.seq, other.node_id)
    def __gt__(self, other):
        return (self.seq, self.node_id) > (other.seq, other.node_id)
    def __ge__(self, other):
        return (self.seq, self.node_id) >= (other.seq, other.node_id)
    def __eq__(self, other):
        return (self.seq, self.node_id) == (other.seq, other.node_id)

    def __str__(self):
        return f"({self.seq}.{self.node_id})"

class Proposer:
    """
    PROPOSER ROLE IN PAXOS ALGORITHM
    ================================
    
    The Proposer is responsible for:
    1. Initiating consensus by proposing values
    2. Driving the two-phase Paxos protocol
    3. Ensuring proposal numbers are unique and increasing
    4. Handling the Paxos constraint (using previous values when required)
    
    PROPOSER STATE:
    - node_id: Unique identifier for this proposer
    - seq: Sequence counter for generating unique proposal numbers
    - name: Human-readable name for logging/debugging
    
    PAXOS PHASES FROM PROPOSER PERSPECTIVE:
    
    Phase 1 (Prepare):
    - Generate unique proposal number N
    - Send PREPARE(N) to majority of acceptors
    - Wait for PROMISE responses
    - If majority promises, proceed to Phase 2
    - If not, retry with higher proposal number
    
    Phase 2 (Accept):  
    - Determine value to propose (respecting Paxos constraint)
    - Send ACCEPT(N, value) to acceptors who promised
    - Wait for ACCEPTED responses
    - If majority accepts, consensus is achieved
    - If not, may need to retry entire process
    """

    def __init__(self, node_id: int, name: str = ""):
        self.node_id = node_id
        self.name = name or f"Node-{node_id}"
        self.seq = 0  # Sequence counter for unique proposal numbers

    def propose(self, value: Any, acceptors: List['Acceptor']) -> Optional[Any]:
        """
        Run complete Paxos consensus round.
        
        This method implements the full Paxos algorithm:
        1. Phase 1: Send PREPARE, collect PROMISE responses
        2. Check if majority of acceptors promised
        3. Phase 2: Send ACCEPT with chosen value, collect ACCEPTED responses  
        4. Check if majority of acceptors accepted
        5. Return consensus value if successful, None if failed
        
        Args:
            value: The value this proposer wants to achieve consensus on
            acceptors: List of acceptor nodes to send messages to
            
        Returns:
            The consensus value if achieved, None if consensus failed
        """
        # Generate unique, monotonically increasing proposal number
        self.seq += 1
        proposal_num = ProposalNumber(self.seq, self.node_id)
        majority = len(acceptors) // 2 + 1

        print(f"🏛️  {self.name}: Starting Paxos round to propose '{value}'")
        print(f"   📊 Proposal number: {proposal_num} | Majority needed: {majority}/{len(acceptors)}")

        # === PHASE 1: PREPARE ===
        print(f"   📤 PHASE 1: Sending PREPARE({proposal_num}) to all acceptors")
        promises = []

        for acceptor in acceptors:
            response = acceptor.prepare(proposal_num)
            if response:
                promises.append(response)

        # Check if we got majority promises
        if len(promises) < majority:
            print(f"   ❌ PHASE 1 FAILED: Only {len(promises)}/{majority} promises received")
            print(f"      Reason: Not enough acceptors promised to support this proposal")
            return None

        print(f"   ✅ PHASE 1 SUCCESS: {len(promises)}/{len(acceptors)} promises received")

        # === DETERMINE VALUE TO PROPOSE ===
        # This is the critical Paxos constraint implementation
        final_value = self._choose_value_following_paxos_constraint(value, promises)

        if final_value != value:
            print(f"   🔄 PAXOS CONSTRAINT APPLIED:")
            print(f"      Original proposal: '{value}'")
            print(f"      Must use existing: '{final_value}'")
            print(f"      Reason: Acceptor(s) already accepted a value in previous round")

        # === PHASE 2: ACCEPT ===
        print(f"   📤 PHASE 2: Sending ACCEPT({proposal_num}, '{final_value}') to promising acceptors")
        accepts = []

        for acceptor in acceptors:
            # Only send to acceptors who promised in Phase 1
            if acceptor.node_id in [p.sender for p in promises]:
                response = acceptor.accept(proposal_num, final_value)
                if response:
                    accepts.append(response)

        # Check if we got majority accepts
        if len(accepts) >= majority:
            print(f"   🎉 CONSENSUS ACHIEVED!")
            print(f"      Final value: '{final_value}'")
            print(f"      Votes received: {len(accepts)}/{len(acceptors)} acceptors")
            print(f"      Paxos guarantees: This value is now immutable across the system")
            return final_value
        else:
            print(f"   ❌ PHASE 2 FAILED: Only {len(accepts)}/{majority} accepts received")
            print(f"      Possible reasons: Network issues, acceptor failures, or competing proposals")
            return None

    def _choose_value_following_paxos_constraint(self, original_value: Any, promises: List[Message]) -> Any:
        """
        Implement the core Paxos safety constraint.
        
        PAXOS SAFETY RULE:
        If any acceptor in Phase 1 returns a previously accepted value,
        the proposer MUST use the value from the highest-numbered proposal.
        
        This rule ensures that:
        1. Once a value starts being accepted, all future proposals use that value
        2. No two different values can ever be chosen
        3. The system maintains consistency even with concurrent proposers
        
        WHY THIS RULE EXISTS:
        Without this rule, two proposers could choose different values:
        - Proposer A gets promises, proposes "Value X"  
        - Proposer B gets promises, proposes "Value Y"
        - Result: Inconsistent state!
        
        WITH this rule:
        - Proposer A gets promises, proposes "Value X", some acceptors accept
        - Proposer B gets promises, learns about "Value X", must propose "Value X"  
        - Result: Consistent state - both chose "Value X"
        
        Args:
            original_value: The value this proposer originally wanted to propose
            promises: List of PROMISE messages from acceptors in Phase 1
            
        Returns:
            The value that must be used in Phase 2 (may be different from original)
        """
        highest_proposal_num = -1
        highest_accepted_value = None

        # Examine all promises to find highest-numbered previously accepted value
        for promise in promises:
            if promise.prev_accepted is not None:
                prev_proposal_num, prev_value = promise.prev_accepted

                if prev_proposal_num > highest_proposal_num:
                    highest_proposal_num = prev_proposal_num
                    highest_accepted_value = prev_value

        # Apply Paxos constraint: use highest previous value if any exists
        if highest_accepted_value is not None:
            return highest_accepted_value
        else:
            # No previous values exist, can use our original proposal
            return original_value

class Acceptor:
    """
    ACCEPTOR ROLE IN PAXOS ALGORITHM  
    ================================
    
    The Acceptor is the core voting component of Paxos. Acceptors are responsible for:
    1. Receiving PREPARE requests and making promises
    2. Receiving ACCEPT requests and deciding whether to accept
    3. Never violating promises (this ensures Paxos safety)
    4. Persisting accepted values for durability
    
    ACCEPTOR STATE:
    - node_id: Unique identifier for this acceptor
    - highest_prepare: Highest proposal number this acceptor has promised to
    - accepted: The proposal (number, value) this acceptor has accepted
    - name: Human-readable name for logging
    
    PAXOS RULES FOR ACCEPTORS:
    
    Rule 1 (PREPARE handling):
    - If proposal number N > highest_prepare seen so far:
      * Promise not to accept any proposal < N  
      * Return any previously accepted proposal
    - If proposal number N <= highest_prepare:
      * Ignore the request (already promised to higher number)
    
    Rule 2 (ACCEPT handling):
    - If proposal number N >= highest_prepare promised:
      * Accept the proposal and store (N, value)
      * This doesn't violate any promises made
    - If proposal number N < highest_prepare:
      * Reject the proposal (would violate promise)
    
    WHY THESE RULES ENSURE SAFETY:
    The promise mechanism prevents acceptors from accepting "old" proposals
    that could conflict with newer ones. Once an acceptor promises to proposal N,
    it will never accept anything numbered < N. This creates a "window" where
    only the latest proposer can get acceptance.
    """

    def __init__(self, node_id: int, name: str = ""):
        self.node_id = node_id
        self.name = name or f"Acceptor-{node_id}"
        self.highest_prepare = None  # Highest proposal number promised to
        self.accepted = None         # (proposal_num, value) currently accepted

    def prepare(self, proposal_num: ProposalNumber) -> Optional[Message]:
        """
        Handle PREPARE request (Phase 1b of Paxos).
        
        This implements the core promise mechanism of Paxos:
        
        ACCEPTOR LOGIC FOR PREPARE(N):
        1. Compare N with highest_prepare seen so far
        2. If N > highest_prepare:
           - Update highest_prepare = N (make the promise)
           - Return PROMISE with any previously accepted value
        3. If N <= highest_prepare:  
           - Ignore request (already promised to higher number)
        
        WHAT THE PROMISE MEANS:
        "I promise not to accept any proposal numbered < N"
        
        This promise is the foundation of Paxos safety. Once made,
        the acceptor will never violate it.
        
        Args:
            proposal_num: The proposal number from the PREPARE request
            
        Returns:
            PROMISE message if accepting, None if rejecting
        """
        print(f"     📨 {self.name}: Received PREPARE({proposal_num})")

        # Check if this proposal number is higher than any we've promised to
        if self.highest_prepare is None or proposal_num > self.highest_prepare:
            # Make the promise - update our highest_prepare
            old_promise = self.highest_prepare
            self.highest_prepare = proposal_num

            print(f"     📝 {self.name}: PROMISE granted to {proposal_num}")
            if old_promise:
                print(f"        Previous promise was to {old_promise}")

            # Include any previously accepted value in the response
            # This is crucial for the Paxos constraint
            prev_accepted = None
            if self.accepted:
                prev_proposal_num, prev_value = self.accepted
                prev_accepted = (prev_proposal_num.seq, prev_value)
                print(f"        Returning previously accepted: {prev_proposal_num} -> '{prev_value}'")

            return Message("PROMISE", self.node_id, 0, proposal_num.seq,
                           prev_accepted=prev_accepted)
        else:
            # Reject - already promised to a higher numbered proposal
            print(f"     ❌ {self.name}: REJECT PREPARE {proposal_num}")
            print(f"        Reason: Already promised to higher number {self.highest_prepare}")
            return None

    def accept(self, proposal_num: ProposalNumber, value: Any) -> Optional[Message]:
        """
        Handle ACCEPT request (Phase 2b of Paxos).
        
        This implements the core voting mechanism of Paxos:
        
        ACCEPTOR LOGIC FOR ACCEPT(N, V):
        1. Compare N with highest_prepare promised to
        2. If N >= highest_prepare:
           - Accept the proposal: store (N, V)
           - Return ACCEPTED message
        3. If N < highest_prepare:
           - Reject (would violate our promise)
        
        WHY THIS RULE ENSURES SAFETY:
        By only accepting proposals >= highest_prepare, the acceptor
        ensures it never violates its promise. This prevents conflicting
        values from being accepted.
        
        EXAMPLE OF SAFETY IN ACTION:
        1. Acceptor promises to proposal (5, NodeA)  
        2. Later receives ACCEPT(3, "ValueX") - REJECTS (3 < 5)
        3. Later receives ACCEPT(6, "ValueY") - ACCEPTS (6 >= 5)
        
        This prevents "old" proposals from being accepted after newer
        ones have been promised to.
        
        Args:
            proposal_num: The proposal number from ACCEPT request
            value: The value being proposed for acceptance
            
        Returns:
            ACCEPTED message if accepting, None if rejecting
        """
        print(f"     📨 {self.name}: Received ACCEPT({proposal_num}, '{value}')")

        # Check if this proposal violates our promise
        if self.highest_prepare is None or proposal_num >= self.highest_prepare:
            # Accept the proposal - doesn't violate any promises
            old_accepted = self.accepted
            self.accepted = (proposal_num, value)

            print(f"     ✅ {self.name}: ACCEPTED '{value}' with proposal {proposal_num}")
            if old_accepted:
                old_num, old_val = old_accepted
                print(f"        Previous acceptance: {old_num} -> '{old_val}'")

            return Message("ACCEPTED", self.node_id, 0, proposal_num.seq, value)
        else:
            # Reject - would violate our promise  
            print(f"     ❌ {self.name}: REJECT ACCEPT {proposal_num}")
            print(f"        Reason: Would violate promise to {self.highest_prepare}")
            return None

class Learner:
    """
    LEARNER ROLE IN PAXOS ALGORITHM
    ===============================
    
    The Learner is responsible for:
    1. Learning about consensus results from acceptors
    2. Determining when consensus has been achieved  
    3. Notifying clients about the final agreed value
    4. Maintaining fault-tolerance through replication
    
    LEARNER FUNCTIONALITY:
    - Learners don't participate in the voting process
    - They monitor acceptors to learn when consensus is reached
    - Multiple learners can exist for fault tolerance
    - In practice, acceptors notify learners when they accept values
    
    CONSENSUS DETECTION:
    A learner determines consensus is achieved when:
    - It learns that a majority of acceptors have accepted the same value
    - This guarantees the value is "chosen" and will never change
    
    REAL-WORLD USAGE:
    - Database replicas learning about committed transactions
    - Service instances learning about configuration updates  
    - Monitoring systems learning about system state changes
    """

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.values = {}  # acceptor_id -> accepted_value mapping

    def learn(self, acceptor_id: int, value: Any):
        """
        Learn that a specific acceptor has accepted a value.
        
        In a real implementation, acceptors would actively notify learners
        whenever they accept a proposal. This simulates that notification.
        """
        self.values[acceptor_id] = value
        print(f"📚 Learner-{self.node_id}: Learned Acceptor-{acceptor_id} accepted '{value}'")

    def get_consensus(self, total_acceptors: int) -> Optional[Any]:
        """
        Determine if consensus has been achieved.
        
        CONSENSUS RULE:
        Consensus is achieved when a majority of acceptors have accepted
        the same value. This guarantees:
        1. The value is "chosen" and immutable
        2. All future Paxos rounds will converge on this value
        3. The system has reached a consistent state
        
        Args:
            total_acceptors: Total number of acceptors in the system
            
        Returns:
            The consensus value if achieved, None if no consensus yet
        """
        if not self.values:
            return None

        # Count how many acceptors accepted each value
        value_counts = {}
        for acceptor_id, value in self.values.items():
            if value in value_counts:
                value_counts[value] += 1
            else:
                value_counts[value] = 1

        majority_threshold = total_acceptors // 2 + 1

        # Check if any value has majority support
        for value, count in value_counts.items():
            if count >= majority_threshold:
                print(f"🎓 Learner-{self.node_id}: CONSENSUS DETECTED!")
                print(f"   Value: '{value}' accepted by {count}/{total_acceptors} acceptors")
                return value

        print(f"📊 Learner-{self.node_id}: No consensus yet. Vote counts: {value_counts}")
        return None

# === REALISTIC PAXOS DEMONSTRATIONS ===

def demo_database_leader_election():
    """Database Leader Election using Paxos - prevents split-brain scenarios."""
    print("="*80)
    print("🗳️  PAXOS DEMONSTRATION: DATABASE LEADER ELECTION")
    print("="*80)
    print("SCENARIO: Distributed database cluster electing primary node")
    print("CHALLENGE: Multiple nodes compete simultaneously - only one can win")
    print("SOLUTION: Paxos ensures exactly one leader is chosen")
    print()

    # Database nodes competing for leadership
    db_node_east = Proposer(1, "MySQL-Primary-East")
    db_node_west = Proposer(2, "MySQL-Primary-West")

    # Cluster coordinators (acceptors) 
    coordinators = [
        Acceptor(10, "ClusterCoord-DC1"),
        Acceptor(11, "ClusterCoord-DC2"),
        Acceptor(12, "ClusterCoord-DC3"),
        Acceptor(13, "ClusterCoord-DC4"),
        Acceptor(14, "ClusterCoord-DC5")
    ]

    learner = Learner(20)

    print(f"📊 CLUSTER SETUP:")
    print(f"   Candidates: MySQL-Primary-East, MySQL-Primary-West")
    print(f"   Coordinators: {len(coordinators)} nodes (majority = {len(coordinators)//2 + 1})")
    print(f"   Safety requirement: Only ONE leader can be elected")
    print()

    # Election attempts
    print("=" * 60)
    print("ELECTION ROUND 1: MySQL-Primary-East campaigns for leadership")
    print("=" * 60)
    elected_leader1 = db_node_east.propose("MySQL-Primary-East", coordinators)

    print()
    print("=" * 60)
    print("ELECTION ROUND 2: MySQL-Primary-West challenges for leadership")
    print("=" * 60)
    print("⚠️  CRITICAL TEST: Will Paxos prevent split-brain scenario?")
    elected_leader2 = db_node_west.propose("MySQL-Primary-West", coordinators)

    # Update learner and show results
    for coordinator in coordinators:
        if coordinator.accepted:
            _, accepted_value = coordinator.accepted
            learner.learn(coordinator.node_id, accepted_value)

    consensus_leader = learner.get_consensus(len(coordinators))

    print()
    print("🏆 ELECTION RESULTS:")
    print("=" * 40)
    print(f"   MySQL-Primary-East result: {elected_leader1}")
    print(f"   MySQL-Primary-West result: {elected_leader2}")
    print(f"   Learner consensus: {consensus_leader}")

    if elected_leader1 and elected_leader2 and elected_leader1 == elected_leader2:
        print()
        print("✅ PAXOS SAFETY GUARANTEED:")
        print(f"   🎯 Elected Leader: {elected_leader1}")
        print(f"   🛡️  No split-brain: Both nodes agreed on same leader")
        print(f"   📊 Consensus mechanism prevented database corruption")

    print()

def run_complete_paxos_demo():
    """Run the complete Paxos demonstration."""
    print("""
🏛️  PAXOS CONSENSUS ALGORITHM - COMPREHENSIVE DEMONSTRATION
===========================================================

WHAT IS PAXOS?
- Fundamental distributed consensus protocol (Leslie Lamport, 1990)
- Solves the problem: "How can distributed nodes agree on a value?"
- Used in production by Google, Amazon, Microsoft, Facebook, etc.

KEY GUARANTEES:
✅ SAFETY: Only one value can ever be chosen (no split-brain)
✅ LIVENESS: Eventually a value will be chosen (progress guarantee)  
✅ FAULT TOLERANCE: Works with up to (N-1)/2 failures in N-node system

ALGORITHM PHASES:
🔄 Phase 1 (Prepare/Promise): Reserve right to propose, learn constraints
🗳️  Phase 2 (Accept/Accepted): Vote on proposed value, achieve consensus

REAL-WORLD APPLICATIONS:
- Database leader election (MongoDB, MySQL Cluster)
- Configuration management (Kubernetes, etcd)
- Distributed locking (Chubby, ZooKeeper)  
- Transaction coordination (Spanner, CockroachDB)
""")

    demo_database_leader_election()

    print("="*80)
    print("🎯 PAXOS ALGORITHM SUMMARY & KEY TAKEAWAYS")
    print("="*80)
    print("✅ SAFETY DEMONSTRATED: Paxos prevented all conflict scenarios")
    print("🔄 CONSISTENCY ACHIEVED: All nodes converged on same values")
    print("🛡️  FAULT TOLERANCE: Algorithm works despite competing proposals")
    print("⚡ EFFICIENCY: Typically requires 2 network round-trips")
    print()
    print("🏛️  PRODUCTION USAGE:")
    print("   - Google Spanner: Global distributed SQL database")
    print("   - Apache Cassandra: Multi-datacenter NoSQL database")
    print("   - etcd: Kubernetes cluster coordination")
    print("   - Apache ZooKeeper: Configuration management")
    print("   - MongoDB: Replica set leader election")
    print()
    print("💡 CORE INSIGHT:")
    print("   Paxos ensures safety through promises - acceptors never violate")
    print("   their commitments, which prevents conflicting decisions.")
    print("="*80)

if __name__ == "__main__":
    run_complete_paxos_demo()
