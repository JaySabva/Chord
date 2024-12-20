from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
import hashlib
import threading
import time

def hashFunction(key):
    """Generates a hash for the given key."""
    print(f"Hashing key: {key}")
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % nodes

def is_between(x, a, b, ring_size):
    """Check if x is between a and b on a modular ring of size ring_size."""
    # x = x % ring_size
    # a = a % ring_size
    # b = b % ring_size
    # print(x, a, b)
    if a < b:
        return a < x <= b
    else:  # Handle the case when there's a wrap-around in the ring
        return x > a or x <= b

m = 6
nodes = 2 ** m

# User-defined variables
ip = "localhost"
port = input("Enter port number: ")
node_id = hashFunction(ip + port)

successor = {'node_id': node_id, 'ip': ip, 'port': port}
predecessor = None

finger_table = [{'node_id': node_id, 'ip': ip, 'port': port} for _ in range(m)]

data = {}

def find_successor(key):
    """Finds the successor of a given key."""
    print(f"Finding successor for key: {key} in Node {node_id}")
    if successor['node_id'] == node_id:
        print(f"Node {node_id} is the only node in the ring. Returning itself as the successor.")
        return successor

    if is_between(key, node_id, successor['node_id'], nodes):
            print(f"Key {key} lies between Node {node_id} and its successor Node {successor['node_id']}")
            return successor

    # Forward the request to the successor
    n_prime = closest_preceding_node(key)
    if n_prime['node_id'] == node_id:
        print(f"Forwarding successor request to Node {successor['node_id']}")
        return successor
    print(f"Forwarding successor request to Node {n_prime['node_id']}")
    try:
        return xmlrpc.client.ServerProxy(f"http://{n_prime['ip']}:{n_prime['port']}").find_successor(key)
    except Exception as e:
        print(f"Node {n_prime['node_id']} is not responding: {e}")
        return None

def closest_preceding_node(key):
    """Finds the closest preceding node to the given key."""
    print(f"Finding closest preceding node to key {key} in Node {node_id}")
    for i in range(m - 1, -1, -1):
        if finger_table[i]['node_id'] == node_id:
            continue
        if is_between(finger_table[i]['node_id'], node_id, key, nodes):
            print(f"Closest preceding node to key {key} is Node {finger_table[i]['node_id']}")
            return finger_table[i]
        
    return {'node_id': node_id, 'ip': ip, 'port': port}

def get_predecessor():
    """Returns the predecessor of the node."""
    # print(f"Returning predecessor of Node {node_id}: {predecessor}")
    return predecessor

def join(n_prime):
    """Joins the node to the Chord network through the given prime node."""
    global successor
    global data
    print(f"Node {node_id} trying to join via Node {n_prime['node_id']}")
    try:
        n_prime = xmlrpc.client.ServerProxy(f"http://{n_prime['ip']}:{n_prime['port']}")
        x = n_prime.find_successor(node_id)
        successor = x
        print(f"Node {node_id} joined the network. Successor is now Node {x['node_id']}")
        # transfer keys from successor
        keys = xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").get_keys(node_id)
        # convert dictionary key to string
        # Now you can safely merge or use the 'keys' dictionary
        data = {**data, **keys}

        # convert keys to integer
        data = {int(k): v for k, v in data.items()}
        print(f"Transferred keys from Node {successor['node_id']}")

    except Exception as e:
        print(f"Failed to join: {e}")

def stabilize():
    """Stabilizes the node."""
    global successor
    # print(f"Stabilizing Node {node_id}")
    try:
        x = xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").get_predecessor()
        if x is not None:
            if is_between(x['node_id'], node_id, successor['node_id'], nodes):
                print(f"Updating successor to Node {x['node_id']}")
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
    elif is_between(n_prime['node_id'], predecessor['node_id'], node_id, nodes):
        print(f"Updating predecessor to Node {n_prime['node_id']}")
        predecessor = n_prime

next = 0
def fix_fingers():
    """Fixes the finger table."""
    global next
    for i in range(m):
        finger_table[i] = find_successor((node_id + 2 ** i) % nodes)

def start_server():
    """Starts the XML-RPC server."""
    print(f"Starting server for Node {node_id} on port {port}")
    server = SimpleXMLRPCServer((ip, int(port)), logRequests=False, allow_none=True)
    server.register_function(find_successor, "find_successor")
    server.register_function(join, "join")
    server.register_function(get_predecessor, "get_predecessor")
    server.register_function(stabilize, "stabilize")
    server.register_function(notify, "notify")
    server.register_function(hashFunction, "hashFunction")
    server.register_function(put, "put")
    server.register_function(get, "get")
    server.register_function(suc_update, "suc_update")
    server.register_function(pred_update, "pred_update")
    server.register_function(get_keys, "get_keys")

    print(f"Node {node_id} listening on port {port}")
    server.serve_forever()

def suc_update(node):
    global successor
    successor = node
    return True

def pred_update(node):
    global predecessor
    predecessor = node
    return True

def get_keys(key):
    d2 = {}
    for k, v in data.items():
        print(k, v)  # Debugging: check the type and value of each key
        print(predecessor['node_id'], key)  # Debugging: check the node_id and key comparison

        # Ensure the key is converted to string before adding to d2
        if is_between(k, predecessor['node_id'], key, nodes):
            d2[str(k)] = v  # Convert key to string
    
    print(f"Returning keys for Node {key}: {d2}")
    # delete keys from data
    for k in d2.keys():
        del data[int(k)]
    return d2
    
def put(key, value):
    print(f"Storing key '{key}' with value '{value}' in Node {node_id}")
    data[key] = value

def get(key):
    print(f"Retrieving value for key '{key}' from Node {node_id}")
    return data[key]

def print_data():
    print("--------------------")
    print(data)
    print("--------------------")

def user_input_loop():
    """Handles user input to join other nodes."""
    
    if port != 3000:
        print("Joining Node 3000...")
        join({'node_id': 3000, 'ip': 'localhost', 'port': '3000'})

def stabilize_loop():
    """Periodically stabilizes the node."""
    while True:
        # print(f"Stabilizing Node {node_id}...")
        stabilize()
        fix_fingers()
        for i in range(m):
            print(f"{i} {finger_table[i]['node_id']}")
        print_data()
        print(f"Predecessor of Node {node_id}: {predecessor}")
        print(f"Successor of Node {node_id}: {successor}")
        print("--------------------")
        time.sleep(5)

if __name__ == '__main__':
    try:
        # Start the XML-RPC server in a separate thread
        server_thread = threading.Thread(target=start_server)
        server_thread.daemon = True  # Daemonize thread to allow exit
        server_thread.start()

        # Start the user input loop
        user_input_loop()
        stabilize_loop()
    except KeyboardInterrupt:
        if predecessor is not None:
            xmlrpc.client.ServerProxy(f"http://{predecessor['ip']}:{predecessor['port']}").suc_update(successor)
        if successor is not None:
            xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").pred_update(predecessor)
        # transfer keys to successor
        for k, v in data.items():
            xmlrpc.client.ServerProxy(f"http://{successor['ip']}:{successor['port']}").put(k, v)
        print("Exiting...")
