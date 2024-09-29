import xmlrpc.client
import hashlib

def hashFunction(key):
    """Generates a hash for the given key."""
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % 256

# User-defined variables for connecting to the initial Chord node
ip = 'localhost'
port = input("Enter the port of the node you want to connect to: ")
server_url = f"http://{ip}:{port}"

try:
    # Connect to the Chord node
    server = xmlrpc.client.ServerProxy(server_url)
    print(f"Connected to Node on {server_url}")
except Exception as e:
    print(f"Failed to connect to the node: {e}")
    exit(1)

def find_successor_of_key(key):
    """Finds the successor node for a given key."""
    try:
        key_hash = hashFunction(key)
        print(f"Finding successor for key '{key}' (Hash: {key_hash})")
        successor = server.find_successor(key_hash)
        print(f"Successor found: Node {successor['node_id']} at {successor['ip']}:{successor['port']}")
        return successor
    except Exception as e:
        print(f"Error finding successor: {e}")
        return None

def put_data(key, value):
    """Stores a key-value pair in the Chord network."""
    successor = find_successor_of_key(key)
    if successor:
        try:
            # Connect to the successor node and store the data
            successor_server_url = f"http://{successor['ip']}:{successor['port']}"
            successor_server = xmlrpc.client.ServerProxy(successor_server_url)
            key_hash = hashFunction(key)
            print(f"Storing key '{key}' (Hash: {key_hash}) with value '{value}' in Node {successor['node_id']}")
            successor_server.put(key_hash, value)
            print("Data stored successfully.")
        except Exception as e:
            print(f"Error storing data on successor node: {e}")

def get_data(key):
    """Retrieves a value for a given key from the Chord network."""
    successor = find_successor_of_key(key)
    if successor:
        try:
            # Connect to the successor node and retrieve the data
            successor_server_url = f"http://{successor['ip']}:{successor['port']}"
            successor_server = xmlrpc.client.ServerProxy(successor_server_url)
            key_hash = hashFunction(key)
            print(f"Retrieving value for key '{key}' (Hash: {key_hash}) from Node {successor['node_id']}")
            value = successor_server.get(key_hash)
            if value is not None:
                print(f"Value retrieved: {value}")
            else:
                print(f"No value found for key: {key}")
        except Exception as e:
            print(f"Error retrieving data from successor node: {e}")

# Main client loop
if __name__ == '__main__':
    while True:
        print("\nOptions:")
        print("1. Store data (put)")
        print("2. Retrieve data (get)")
        print("3. Exit")
        choice = input("Choose an option: ").strip()

        if choice == '1':
            key = input("Enter the key: ")
            value = input("Enter the value: ")
            put_data(key, value)
        
        elif choice == '2':
            key = input("Enter the key to retrieve the value: ")
            get_data(key)
        
        elif choice == '3':
            print("Exiting client.")
            break
        
        else:
            print("Invalid choice. Please select a valid option.")
