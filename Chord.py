from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import hashlib
import threading
import time

def hashFunction(key):
    """Generates a hash for the given key."""
    print(f"Hashing key: {key}")
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % 256

m = 6
nodes = 2 ** m

# User-defined variables
ip = "localhost"
port = input("Enter port number: ")
node_id = hashFunction(ip + port)

successor = {'node_id': node_id, 'ip': ip, 'port': port}
predecessor = None

finger_table = [{'node_id': node_id, 'ip': ip, 'port': port} for _ in range(m)]

def find_successor(key):
    """Finds the successor of a given key."""
    print(f"Finding successor for key: {key} in Node {node_id}")
    if successor['node_id'] == node_id:
        print(f"Node {node_id} is the only node in the ring. Returning itself as the successor.")
        return successor

    # Check if key falls between node_id and its successor
    if node_id < successor['node_id']:
        if node_id < key <= successor['node_id']:
            print(f"Key {key} lies between Node {node_id} and its successor Node {successor['node_id']}")
            return successor
    else:  # This handles the case where the ring wraps around
        if key > node_id or key <= successor['node_id']:
            print(f"Key {key} wraps around the ring. Returning Node {successor['node_id']} as successor.")
            return successor

    # Forward the request to the successor
    n_prime = successor
    print(f"Forwarding successor request to Node {n_prime['node_id']}")
    try:
        return xmlrpc.client.ServerProxy(f"http://{n_prime['ip']}:{n_prime['port']}").find_successor(key)
    except Exception as e:
        print(f"Node {n_prime['node_id']} is not responding: {e}")
        return None

def get_predecessor():
    """Returns the predecessor of the node."""
    # print(f"Returning predecessor of Node {node_id}: {predecessor}")
    return predecessor

def join(n_prime):
    """Joins the node to the Chord network through the given prime node."""
    global successor
    print(f"Node {node_id} trying to join via Node {n_prime['node_id']}")
    try:
        n_prime = xmlrpc.client.ServerProxy(f"http://{n_prime['ip']}:{n_prime['port']}")
        x = n_prime.find_successor(node_id)
        successor = x
        print(f"Node {node_id} joined the network. Successor is now Node {x['node_id']}")
    except Exception as e:
        print(f"Failed to join: {e}")

def stabilize():
    """Stabilizes the node."""
    global successor
    # print(f"Stabilizing Node {node_id}")
    try:
        x = xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").get_predecessor()
        if x is not None:
            # print(f"Found predecessor of successor Node {successor['node_id']}: Node {x['node_id']}")
            if node_id < x['node_id'] < successor['node_id']:
                print(f"Updating successor to Node {x['node_id']}")
                successor = x
            elif node_id > x['node_id'] < successor['node_id']:
                print(f"Updating successor (with ring wrap) to Node {x['node_id']}")
                successor = x

        if x is not None and successor['node_id'] == node_id:
            print(f"Updating successor to Node {x['node_id']} due to stabilization check")
            successor = x

        print(f"Notifying Node {successor['node_id']} of new predecessor: Node {node_id}")
        xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").notify({'node_id': node_id, 'ip': ip, 'port': port})
    except Exception as e:
        print(f"Failed to stabilize: {e}")

def notify(n_prime):
    """Notifies the node of a new predecessor."""
    global predecessor
    print(f"Node {node_id} received notify from Node {n_prime['node_id']}")
    if n_prime['node_id'] == node_id:
        print("Ignoring self notification.")
        return
    if predecessor is None:
        print(f"Setting predecessor to Node {n_prime['node_id']} (first predecessor)")
        predecessor = n_prime
    elif node_id < n_prime['node_id'] or n_prime['node_id'] < successor['node_id']:
            predecessor = n_prime
    else:
        if predecessor['node_id'] < n_prime['node_id'] < node_id:
            print(f"Updating predecessor to Node {n_prime['node_id']}")
            predecessor = n_prime

def start_server():
    """Starts the XML-RPC server."""
    print(f"Starting server for Node {node_id} on port {port}")
    server = SimpleXMLRPCServer((ip, int(port)), logRequests=False, allow_none=True)
    server.register_function(find_successor, "find_successor")
    server.register_function(join, "join")
    server.register_function(get_predecessor, "get_predecessor")
    server.register_function(stabilize, "stabilize")
    server.register_function(notify, "notify")

    print(f"Node {node_id} listening on port {port}")
    server.serve_forever()

def user_input_loop():
    """Handles user input to join other nodes."""
    join_choice = input("Do you want to join another node? (yes/no): ").strip().lower()
    if join_choice == 'y':
        n_prime_ip = 'localhost'
        n_prime_port = input("Enter the port of the node you want to join: ")
        print(f"Node {node_id} trying to join Node {hashFunction(n_prime_ip + n_prime_port)}")
        join({'node_id': hashFunction(n_prime_ip + n_prime_port), 'ip': n_prime_ip, 'port': n_prime_port})
    elif join_choice == 'n':
        print(f"Node {node_id} will not join any other node.")
    else:
        print("Invalid input. Please enter 'yes' or 'no'.")

def stabilize_loop():
    """Periodically stabilizes the node."""
    while True:
        # print(f"Stabilizing Node {node_id}...")
        stabilize()
        print(f"Predecessor of Node {node_id}: {predecessor}")
        print(f"Successor of Node {node_id}: {successor}")
        print("--------------------")
        time.sleep(5)

if __name__ == '__main__':
    # Start the XML-RPC server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True  # Daemonize thread to allow exit
    server_thread.start()

    # Start the user input loop
    user_input_loop()
    stabilize_loop()
