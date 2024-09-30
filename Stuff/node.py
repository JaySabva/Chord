import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import sys
import hashlib
import threading

M = 6
NODES = 2**M

def hashFunction(key):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % NODES

def connect(ip, port):
    return xmlrpc.client.ServerProxy("http://" + ip + ":" + str(port))

class Chord_Node:
    def __init__ (self, ip, port):
        self.ip = ip
        self.port = port
        self.node_id = hashFunction(str(ip) + ":" + str(port))
        self.successor = {'node_id': self.node_id, 'ip': ip, 'port': port}
        self.predecessor = None
        self.finger_table = [{'node_id': self.node_id, 'ip': ip, 'port': port} for i in range(M)]
        self.data = {}

    def find_successor(self, id):
        print("Finding Successor of ", id, " in Node ", self.node_id)
        if id > self.node_id and id <= self.successor['node_id']:
            return self.successor
        if self.node_id == self.successor['node_id']:
            return self.successor
        else:
            n_prime = self.closest_preceding_node(id)
            if n_prime['node_id'] == self.node_id:
                return {'node_id': self.node_id, 'ip': self.ip, 'port': self.port}
            try:
                return connect(n_prime['ip'], n_prime['port']).find_successor(id)
            except:
                print("Node ", n_prime['node_id'], " is not responding")
                return None

    def closest_preceding_node(self, id):
        for i in range(M-1, -1, -1):
            if self.finger_table[i]['node_id'] > self.node_id and self.finger_table[i]['node_id'] < id:
                return self.finger_table[i]
        
        return {'node_id': self.node_id, 'ip': self.ip, 'port': self.port}
    
    def get_predecessor(self):
        return self.predecessor
    
    def join(self, n_prime):
        n_prime = connect(n_prime['ip'], n_prime['port'])
        x = n_prime.find_successor(self.node_id)
        self.successor = x
        print("Joining Node ", self.node_id, " to Node ", x['node_id'])
    
    def stabilize(self):
        try:
            successor = connect(self.successor['ip'], self.successor['port'])
            x = successor.get_predecessor()
            if x is not None and x['node_id'] > self.node_id and x['node_id'] < self.successor['node_id']:
                self.successor = x

            successor.notify({'node_id': self.node_id, 'ip': self.ip, 'port': self.port})
        except:
            print(f"Successor {self.successor['node_id']} not responding")

    def notify(self, n_prime):
        if self.predecessor is None or (n_prime['node_id'] < self.node_id and n_prime['node_id'] > self.predecessor['node_id']):
            self.predecessor = n_prime
        
        if self.successor['node_id'] == self.node_id:
            self.successor = n_prime
    
    def fix_fingers(self):
        for i in range(M):
            self.finger_table[i] = self.find_successor((self.node_id + 2**i) % NODES)
    
    def printFingerTable(self):
        print("Finger Table of Node ", self.node_id)
        for i in range(M):
            print("Finger ", i, ": ", self.finger_table[i]['node_id'])

    def printSucandPred(self):
        print("Node ", self.node_id)
        print("Successor: ", self.successor)
        print("Predecessor: ", self.predecessor)


def start_server(node, port):
    server = SimpleXMLRPCServer(("127.0.0.1", port), logRequests=True, allow_none=True)
    server.register_instance(node)
    print(f"Node {node.node_id} running on port {port}...")
    server.serve_forever()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 node.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    node = Chord_Node("127.0.0.1", port)
    
    # Start the server in a separate thread
    server_thread = threading.Thread(target=start_server, args=(node, port))
    server_thread.daemon = True
    server_thread.start()

    if port != 8001:
        existing_node = int(input("Enter the port of an existing node: "))
        node.join({'ip': "127.0.0.1", 'port': existing_node})
        print("Node ", node.node_id, " joined")
    
    print("Successor: ", node.successor)
    print("Predecessor: ", node.predecessor)

    while True:
        print("1. Print Finger Table")
        print("2. Print Successor and Predecessor")
        print("3. Exit")
        print("4. Update Data")
        choice = int(input("Enter your choice: "))
        if choice == 1:
            node.printFingerTable()
        elif choice == 2:
            node.printSucandPred()
        elif choice == 3:
            break
        elif choice == 4:
            node.stabilize()
            node.fix_fingers()
            node.printFingerTable()
        else:
            print("Invalid Choice")
