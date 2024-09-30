import xmlrpc.client
import hashlib

M = 5  # Number of bits for the Chord ring (2^M nodes)
NODES = 2**M  # Maximum number of nodes in the network

# Utility Functions
def mod(n, base=NODES):
    """Modular arithmetic helper."""
    return n % base

def hashFunction(key):
    """Hash a key to generate a node ID."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % NODES

def store_value(node_ip, node_port, key, value):
    """Store a key-value pair in the specified node."""
    try:
        with xmlrpc.client.ServerProxy(f"http://{node_ip}:{node_port}") as node:
            result = node.store(key, value)
            if result:
                print(f"Stored key: '{key}' with value: '{value}' at node {node_ip}:{node_port}")
            else:
                print(f"Failed to store key: '{key}'")
    except Exception as e:
        print(f"Error storing key-value pair: {e}")

def lookup_value(node_ip, node_port, key):
    """Lookup a value for a given key from the specified node."""
    try:
        with xmlrpc.client.ServerProxy(f"http://{node_ip}:{node_port}") as node:
            result = node.lookup(key)
            if result is not None:
                print(f"Key: '{key}' found with value: '{result}' at node {node_ip}:{node_port}")
            else:
                print(f"Key: '{key}' not found in node {node_ip}:{node_port}")
    except Exception as e:
        print(f"Error looking up key: {e}")

def print_finger_table(node_ip, node_port):
    """Print the finger table of the specified node."""
    try:
        with xmlrpc.client.ServerProxy(f"http://{node_ip}:{node_port}") as node:
            node.print_finger_table()
    except Exception as e:
        print(f"Error printing finger table: {e}")

def find_successor(node_ip, node_port, key):
    """Find the successor of a given key by querying the specified node."""
    try:
        with xmlrpc.client.ServerProxy(f"http://{node_ip}:{node_port}") as node:
            successor = node.find_successor(hashFunction(key))
            print(f"Successor for key '{key}' (ID: {hashFunction(key)}) is Node {successor['node_id']} at {successor['ip']}:{successor['port']}")
    except Exception as e:
        print(f"Error finding successor: {e}")

def update(node_ip, node_port):
    try:
        with xmlrpc.client.ServerProxy(f"http://{node_ip}:{node_port}") as node:
            node.update()
            print(f"Node {node_ip}:{node_port} updated.")
    except Exception as e:
        print(f"Error updating node: {e}")

def main():
    # Replace with actual node IP and port to interact with
    node_ip = "127.0.0.1"  # Change this to the IP of your node
    node_port = 8001       # Change this to the port of your node

    while True:
        print("\nOptions:")
        print("1. Store a key-value pair")
        print("2. Lookup a value by key")
        print("3. Print finger table")
        print("4. Find successor of a key")
        print("5. Update")
        print("6. Exit")
        option = input("Choose an option: ")

        node_port = input("Enter port: ")

        if option == "1":
            key = input("Enter key to store: ")
            value = input("Enter value to store: ")
            store_value(node_ip, node_port, key, value)
        elif option == "2":
            key = input("Enter key to lookup: ")
            lookup_value(node_ip, node_port, key)
        elif option == "3":
            print_finger_table(node_ip, node_port)
        elif option == "4":
            key = input("Enter key to find successor: ")
            find_successor(node_ip, node_port, key)
        elif option == "5":
            update(node_ip, node_port)
        elif option == "6":
            print("Exiting client.")
            break
        else:
            print("Invalid option. Please try again.")

if __name__ == "__main__":
    main()
