import xmlrpc.client
import xmlrpc.server
import threading
import random
import time

M = 5  # Number of bits for the Chord ring (2^M nodes)
NODES = 2**M  # Maximum number of nodes in the network

# Utility Functions
def mod(n, base=NODES):
    """Modular arithmetic helper."""
    return n % base

def in_interval(x, a, b, inclusive=False):
    """Returns true if x is in interval (a, b) or [a, b]."""
    if a < b:
        if inclusive:
            return a <= x <= b
        return a < x < b
    else:
        if inclusive:
            return x >= a or x <= b
        return x > a or x < b

class ChordNode:
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.successor = self
        self.predecessor = None
        self.finger_table = [self] * M  # Initialize finger table
        self.data_store = {}  # Store key-value pairs

        # Create the server
        self.server = xmlrpc.server.SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_instance(self)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

        # Print node details after initialization
        print(f"Node {self.node_id} initialized at port {self.port}")

    def print_finger_table(self):
        """Print the finger table."""
        print(f"Node {self.node_id} Finger Table:")
        for i, finger in enumerate(self.finger_table):
            print(f"  Finger {i}: Node {finger.node_id}")

    def print_data_store(self):
        """Print the data store."""
        print(f"Node {self.node_id} Data Store:")
        if self.data_store:
            for key, value in self.data_store.items():
                print(f"  {key} => {value}")
        else:
            print("  (Empty)")

    # Chord core functions

    def get_successor(self):
        return self.successor

    def find_successor(self, id):
        if in_interval(id, self.node_id, self.successor.node_id, inclusive=True):
            return self.successor
        else:
            n_prime = self.closest_preceding_node(id)
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{n_prime.port}")
            return ChordNode(id, proxy.find_successor(id))  # Ensure this returns a ChordNode

    def closest_preceding_node(self, id):
        for i in range(M - 1, -1, -1):
            if in_interval(self.finger_table[i].node_id, self.node_id, id):
                return self.finger_table[i]
        return self

    def find_predecessor(self, id):
        n = self
        while not in_interval(id, n.node_id, n.successor.node_id, inclusive=True):
            n = n.closest_preceding_node(id)
        return n

    def join(self, n_prime_port=None):
        if n_prime_port:
            n_prime = xmlrpc.client.ServerProxy(f"http://localhost:{n_prime_port}")
            self.init_finger_table(n_prime_port)
            self.update_others()
        else:
            for i in range(M):
                self.finger_table[i] = self
            self.predecessor = None

        # Print finger table after joining
        self.print_finger_table()

    def init_finger_table(self, n_prime_port):
        n_prime = xmlrpc.client.ServerProxy(f"http://localhost:{n_prime_port}")
        self.finger_table[0] = ChordNode(self.node_id, n_prime.find_successor(self.start(1)))
        self.predecessor = ChordNode(self.node_id, self.successor.find_predecessor(self.node_id))
        self.successor.predecessor = self

        for i in range(M - 1):
            if in_interval(self.start(i + 1), self.node_id, self.finger_table[i].node_id, inclusive=True):
                self.finger_table[i + 1] = self.finger_table[i]
            else:
                self.finger_table[i + 1] = ChordNode(self.node_id, n_prime.find_successor(self.start(i + 1)))

    def update_others(self):
        for i in range(M):
            p = self.find_predecessor(self.node_id - 2**i)
            p.update_finger_table(self, i)

    def update_finger_table(self, s, i):
        if in_interval(s.node_id, self.node_id, self.finger_table[i].node_id, inclusive=False):
            self.finger_table[i] = s
            p = self.predecessor
            if p:
                p.update_finger_table(s, i)

    def stabilize(self):
        x = self.successor.predecessor
        if x and in_interval(x.node_id, self.node_id, self.successor.node_id):
            self.successor = x
        self.successor.notify(self)

    def notify(self, n_prime):
        if not self.predecessor or in_interval(n_prime.node_id, self.predecessor.node_id, self.node_id):
            self.predecessor = n_prime

    def fix_fingers(self):
        i = random.randint(1, M - 1)
        self.finger_table[i] = self.find_successor(self.start(i))
        # Print the updated finger table after fixing fingers
        self.print_finger_table()

    def check_predecessor(self):
        if self.predecessor and not self.ping(self.predecessor.port):
            self.predecessor = None

    def start(self, i):
        """Returns the start of the ith finger interval."""
        return mod(self.node_id + 2**i)

    def ping(self, port):
        """Pings a node to see if it's alive."""
        try:
            proxy = xmlrpc.client.ServerProxy(f"http://localhost:{port}")
            proxy.get_successor()
            return True
        except:
            return False

    # Data storage
    def store(self, key, value):
        """Store a key-value pair in the node."""
        self.data_store[key] = value
        print(f"Node {self.node_id} stored key {key} with value {value}")
        self.print_data_store()

    def retrieve(self, key):
        """Retrieve a key-value pair from the node."""
        value = self.data_store.get(key, None)
        print(f"Node {self.node_id} retrieved key {key} with value {value}")
        return value

# Main function to start nodes
def start_node(node_id, port, bootstrap_port=None):
    node = ChordNode(node_id, port)
    if bootstrap_port:
        node.join(bootstrap_port)
    else:
        node.join(None)
    return node

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python chord_node.py <node_id> <port> [bootstrap_port]")
        sys.exit(1)

    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    bootstrap_port = int(sys.argv[3]) if len(sys.argv) > 3 else None

    node = start_node(node_id, port, bootstrap_port)

    while True:
        node.stabilize()
        node.fix_fingers()
        time.sleep(1)
