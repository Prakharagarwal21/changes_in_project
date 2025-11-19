import socket
import threading
import time
import json
import os
from pathlib import Path
from models import Config, FileChange, Peer, NetworkMessage, MessageType, ChangeType

class NetworkHandler:
    def __init__(self, config: Config, sync_engine):
        self.config = config
        self.sync_engine = sync_engine
        self.is_running = False
        self.connected_peers = {}  # peer_id -> (socket, address)
        self.server_socket = None
        self.server_thread = None
        self.connection_thread = None
        
    def start(self):
        """Start network handler with real networking"""
        self.is_running = True
        
        # Start TCP server to listen for incoming connections
        self.server_thread = threading.Thread(target=self._start_server, daemon=True)
        self.server_thread.start()
        
        # Start thread to connect to known peers
        self.connection_thread = threading.Thread(target=self._connect_to_peers, daemon=True)
        self.connection_thread.start()
        
        print(f"Network handler started on port {self.config.port}")
    
    def stop(self):
        """Stop network handler"""
        self.is_running = False
        
        # Close all peer connections
        for peer_id, (sock, addr) in self.connected_peers.items():
            try:
                sock.close()
            except:
                pass
        self.connected_peers.clear()
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
        
        print("Network handler stopped")
    
    def _start_server(self):
        """Start TCP server to accept incoming connections"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.config.host, self.config.port))
            self.server_socket.listen(5)
            print(f"Server listening on {self.config.host}:{self.config.port}")
            
            while self.is_running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f"Incoming connection from {address}")
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client_connection,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.error as e:
                    if self.is_running:
                        print(f"Server accept error: {e}")
                    
        except Exception as e:
            print(f"Server error: {e}")
    
    def _connect_to_peers(self):
        """Connect to all known peers with better logic"""
        time.sleep(2)  # Wait a bit before starting connections
        
        while self.is_running:
            known_peers = self.config.get_known_peers()
            
            for peer in known_peers:
                if peer.id != self.config.peer_id:  # Don't connect to ourselves
                    self._connect_to_peer(peer)
            
            time.sleep(10)  # Retry every 10 seconds
    
    def _connect_to_peer(self, peer: Peer):
        """Connect to a specific peer"""
        # Don't connect if we already have this peer connected
        if peer.id in self.connected_peers:
            return
            
        try:
            print(f"Attempting to connect to {peer.host}:{peer.port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((peer.host, peer.port))
            
            # Send hello message immediately
            hello_msg = NetworkMessage(
                message_type=MessageType.HELLO,
                sender_id=self.config.peer_id,
                data={"port": self.config.port}
            )
            self._send_message(sock, hello_msg)
            
            # Store the connection
            self.connected_peers[peer.id] = (sock, (peer.host, peer.port))
            
            # Start receiving messages from this peer
            receive_thread = threading.Thread(
                target=self._receive_messages,
                args=(sock, peer),
                daemon=True
            )
            receive_thread.start()
            
            print(f"✅ Successfully connected to peer {peer.id} at {peer.host}:{peer.port}")
            
        except socket.timeout:
            print(f"⏰ Connection timeout to {peer.host}:{peer.port}")
        except ConnectionRefusedError:
            print(f"❌ Connection refused by {peer.host}:{peer.port} (peer may not be running)")
        except Exception as e:
            print(f"❌ Failed to connect to {peer.host}:{peer.port}: {e}")
    
    def _handle_client_connection(self, client_socket, address):
            """Handle incoming client connection"""
            peer_id = None
            try:
                client_socket.settimeout(10.0)
                
                # Receive hello message to identify peer
                data = self._receive_data(client_socket)
                if not data:
                    return
                    
                message = NetworkMessage.from_bytes(data)
                
                if message.message_type == MessageType.HELLO:
                    peer_id = message.sender_id
                    # Create peer object for this connection
                    peer = Peer(
                        id=peer_id,
                        host=address[0],
                        port=message.data.get('port', address[1])
                    )
                    
                    # Store the connection
                    self.connected_peers[peer.id] = (client_socket, address)
                    print(f"Successfully connected with peer {peer.id}")
                    
                    # Start receiving messages
                    self._receive_messages(client_socket, peer)
                else:
                    print(f"Expected HELLO message, got {message.message_type}")
                    
            except Exception as e:
                print(f"Error handling client connection from {address}: {e}")
                if peer_id and peer_id in self.connected_peers:
                    del self.connected_peers[peer_id]
            finally:
                # Don't close the socket here - it's managed in _receive_messages
                pass
    
    def _receive_messages(self, sock, peer: Peer):
        """Continuously receive messages from a peer"""
        while self.is_running:
            try:
                data = self._receive_data(sock)
                if not data:
                    break
                
                message = NetworkMessage.from_bytes(data)
                self._handle_message(message, peer)
                
            except Exception as e:
                print(f"Error receiving from {peer.id}: {e}")
                break
        
        # Remove peer from connected list
        if peer.id in self.connected_peers:
            del self.connected_peers[peer.id]
        print(f"Disconnected from peer {peer.id}")
    
    def _handle_message(self, message: NetworkMessage, peer: Peer):
            """Handle incoming network message"""
            try:
                if message.message_type == MessageType.FILE_CHANGE:
                    print(f"Received file change from {peer.id}: {message.data['change_type']} - {message.data['file_path']}")
                    # Convert back to FileChange object
                    change_data = message.data
                    
                    # Convert string back to ChangeType enum
                    change_type = ChangeType(change_data['change_type'])
                    
                    change = FileChange(
                        change_type=change_type,  # Use the enum here
                        file_path=change_data['file_path'],
                        old_path=change_data.get('old_path'),
                        is_directory=change_data.get('is_directory', False)
                    )
                    # Apply the change locally
                    self.sync_engine.apply_remote_change(change)
                    
                    # If it's a file creation/modification, request the file content
                    if change.change_type in [ChangeType.CREATED, ChangeType.MODIFIED] and not change.is_directory:
                        self._request_file_content(change.file_path, peer)
                
                elif message.message_type == MessageType.FILE_REQUEST:
                    self._send_file_content(message.data['file_path'], peer)
                
                elif message.message_type == MessageType.FILE_DATA:
                    self._save_file_content(message.data)
                
                elif message.message_type == MessageType.HELLO:
                    print(f"Peer {peer.id} says hello")
                    
            except Exception as e:
                print(f"Error handling message from {peer.id}: {e}")
    
    def broadcast_change(self, change: FileChange):
        """Broadcast file change to all connected peers"""
        if not self.connected_peers:
            print("No connected peers to broadcast to")
            return
        
        change_data = {
            'change_type': change.change_type.value,
            'file_path': change.file_path,
            'old_path': change.old_path,
            'is_directory': change.is_directory
        }
        
        message = NetworkMessage(
            message_type=MessageType.FILE_CHANGE,
            sender_id=self.config.peer_id,
            data=change_data
        )
        
        disconnected_peers = []
        for peer_id, (sock, addr) in self.connected_peers.items():
            try:
                self._send_message(sock, message)
                print(f"Broadcasted {change.change_type.value}: {change.file_path} to {peer_id}")
            except Exception as e:
                print(f"Failed to send to {peer_id}: {e}")
                disconnected_peers.append(peer_id)
        
        # Clean up disconnected peers
        for peer_id in disconnected_peers:
            if peer_id in self.connected_peers:
                del self.connected_peers[peer_id]
    
    def _request_file_content(self, file_path: str, peer: Peer):
        """Request file content from peer"""
        message = NetworkMessage(
            message_type=MessageType.FILE_REQUEST,
            sender_id=self.config.peer_id,
            data={'file_path': file_path}
        )
        
        if peer.id in self.connected_peers:
            sock, addr = self.connected_peers[peer.id]
            self._send_message(sock, message)
            print(f"Requested file content: {file_path} from {peer.id}")
    
    def _send_file_content(self, file_path: str, peer: Peer):
        """Send file content to peer"""
        try:
            absolute_path = os.path.join(self.config.shared_folder, file_path)
            
            if not os.path.exists(absolute_path) or os.path.isdir(absolute_path):
                return
            
            with open(absolute_path, 'rb') as f:
                file_content = f.read()
            
            file_data = {
                'file_path': file_path,
                'content': file_content
            }
            
            message = NetworkMessage(
                message_type=MessageType.FILE_DATA,
                sender_id=self.config.peer_id,
                data=file_data
            )
            
            if peer.id in self.connected_peers:
                sock, addr = self.connected_peers[peer.id]
                self._send_message(sock, message)
                print(f"Sent file content: {file_path} to {peer.id}")
                
        except Exception as e:
            print(f"Error sending file {file_path} to {peer.id}: {e}")
    
    def _save_file_content(self, file_data: dict):
        """Save received file content"""
        try:
            file_path = file_data['file_path']
            content = file_data['content']
            
            absolute_path = os.path.join(self.config.shared_folder, file_path)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(absolute_path), exist_ok=True)
            
            with open(absolute_path, 'wb') as f:
                f.write(content)
            
            print(f"Saved file content: {file_path}")
            
        except Exception as e:
            print(f"Error saving file {file_path}: {e}")
    
    def _send_message(self, sock, message: NetworkMessage):
        """Send message over socket"""
        try:
            data = message.to_bytes()
            # Send message length first
            sock.sendall(len(data).to_bytes(4, byteorder='big'))
            # Send message data
            sock.sendall(data)
        except Exception as e:
            raise Exception(f"Failed to send message: {e}")
    
    def _receive_data(self, sock):
            """Receive data from socket with better error handling"""
            try:
                sock.settimeout(5.0)  # Add timeout to prevent hanging
                
                # First receive the message length
                length_data = b''
                while len(length_data) < 4:
                    chunk = sock.recv(4 - len(length_data))
                    if not chunk:
                        return None
                    length_data += chunk
                
                message_length = int.from_bytes(length_data, byteorder='big')
                
                if message_length > 100 * 1024 * 1024:  # 100MB max
                    raise Exception("Message too large")
                
                # Receive the actual message data
                chunks = []
                bytes_received = 0
                while bytes_received < message_length:
                    chunk = sock.recv(min(message_length - bytes_received, 4096))
                    if not chunk:
                        return None
                    chunks.append(chunk)
                    bytes_received += len(chunk)
                
                return b''.join(chunks)
                
            except socket.timeout:
                raise Exception("Receive timeout")
            except Exception as e:
                raise Exception(f"Failed to receive data: {e}")
