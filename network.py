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
        time.sleep(3)  # Wait a bit before starting connections
        
        connection_attempts = {}
        
        while self.is_running:
            known_peers = self.config.get_known_peers()
            
            for peer in known_peers:
                if peer.id != self.config.peer_id:  # Don't connect to ourselves
                    # Only attempt connection if we're not already connected
                    if peer.id not in self.connected_peers:
                        # Limit connection attempts to once every 30 seconds
                        last_attempt = connection_attempts.get(peer.id, 0)
                        if time.time() - last_attempt > 30:
                            connection_attempts[peer.id] = time.time()
                            self._connect_to_peer(peer)
            
            time.sleep(5)  # Check every 5 seconds
    
    def _connect_to_peer(self, peer: Peer):
        """Connect to a specific peer"""
        # Don't connect if we already have this peer connected
        if peer.id in self.connected_peers:
            print(f"⚠️  Already connected to {peer.id}, skipping")
            return
            
        try:
            print(f"🔗 Attempting to connect to {peer.host}:{peer.port}...")
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
            print(f"❌ Connection refused by {peer.host}:{peer.port}")
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
                print(f"❌ No data received from {address}")
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
                print(f"✅ Successfully connected with peer {peer.id}")
                
                # Start receiving messages
                self._receive_messages(client_socket, peer)
            else:
                print(f"❌ Expected HELLO message, got {message.message_type}")
                
        except Exception as e:
            print(f"❌ Error handling client connection from {address}: {e}")
            if peer_id and peer_id in self.connected_peers:
                del self.connected_peers[peer_id]
        finally:
            # Don't close the socket here - it's managed in _receive_messages
            pass
    
    def _receive_messages(self, sock, peer: Peer):
        """Continuously receive messages from a peer"""
        consecutive_timeouts = 0
        max_timeouts = 3
        
        while self.is_running:
            try:
                data = self._receive_data(sock)
                if not data:
                    # No data received (could be timeout or closed connection)
                    consecutive_timeouts += 1
                    if consecutive_timeouts >= max_timeouts:
                        print(f"Too many timeouts from {peer.id}, disconnecting")
                        break
                    continue
                
                # Reset timeout counter on successful receive
                consecutive_timeouts = 0
                
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
            print(f"📨 Processing {message.message_type.value} from {peer.id}")
            
            if message.message_type == MessageType.FILE_CHANGE:
                print(f"Received file change from {peer.id}: {message.data['change_type']} - {message.data['file_path']}")
                # Convert back to FileChange object
                change_data = message.data
                
                # Convert string back to ChangeType enum
                change_type = ChangeType(change_data['change_type'])
                
                change = FileChange(
                    change_type=change_type,
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
                print(f"Received file request for: {message.data['file_path']}")
                self._send_file_content(message.data['file_path'], peer)
            
            elif message.message_type == MessageType.FILE_DATA:
                print(f"Received file data for: {message.data['file_path']}")
                self._save_file_content(message.data)
            
            elif message.message_type == MessageType.HELLO:
                print(f"Peer {peer.id} says hello")
            
            elif message.message_type == MessageType.PING:
                # Send pong response
                pong_msg = NetworkMessage(
                    message_type=MessageType.PONG,
                    sender_id=self.config.peer_id,
                    data={}
                )
                if peer.id in self.connected_peers:
                    sock, addr = self.connected_peers[peer.id]
                    self._send_message(sock, pong_msg)
                    
        except Exception as e:
            print(f"❌ Error handling message from {peer.id}: {e}")
            import traceback
            traceback.print_exc()
    
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
    
    def _handle_message(self, message: NetworkMessage, peer: Peer):
        """Handle incoming network message"""
        try:
            print(f"📨 Processing {message.message_type.value} from {peer.id}")
            
            if message.message_type == MessageType.FILE_CHANGE:
                print(f"📁 File change: {message.data['change_type']} - {message.data['file_path']}")
                # Convert back to FileChange object
                change_data = message.data
                
                # Convert string back to ChangeType enum
                change_type = ChangeType(change_data['change_type'])
                
                change = FileChange(
                    change_type=change_type,
                    file_path=change_data['file_path'],
                    old_path=change_data.get('old_path'),
                    is_directory=change_data.get('is_directory', False)
                )
                # Apply the change locally
                self.sync_engine.apply_remote_change(change)
                
                # If it's a file creation/modification, request the file content
                if change.change_type in [ChangeType.CREATED, ChangeType.MODIFIED] and not change.is_directory:
                    print(f"🔍 Requesting file content: {change.file_path}")
                    self._request_file_content(change.file_path, peer)
            
            elif message.message_type == MessageType.FILE_REQUEST:
                print(f"📤 Sending file: {message.data['file_path']}")
                self._send_file_content(message.data['file_path'], peer)
            
            elif message.message_type == MessageType.FILE_DATA:
                print(f"💾 Saving file: {message.data['file_path']}")
                self._save_file_content(message.data)
            
            elif message.message_type == MessageType.HELLO:
                print(f"👋 Peer {peer.id} says hello")
            
            print(f"✅ Successfully processed {message.message_type.value} from {peer.id}")
                    
        except Exception as e:
            print(f"❌ Error handling message from {peer.id}: {e}")
            import traceback
            traceback.print_exc()
    
    def _send_message(self, sock, message: NetworkMessage):
        """Send message over socket with consistent protocol"""
        try:
            data = message.to_bytes()
            # Send message length first (4 bytes), then the data
            length_prefix = len(data).to_bytes(4, byteorder='big')
            sock.sendall(length_prefix + data)
            print(f"📤 Sent {message.message_type.value} message ({len(data)} bytes) to peer")
        except Exception as e:
            raise Exception(f"Failed to send message: {e}")

    def _receive_data(self, sock):
        """Receive data from socket with consistent protocol"""
        try:
            sock.settimeout(5.0)
            
            # First receive the message length (4 bytes)
            length_data = b''
            while len(length_data) < 4:
                chunk = sock.recv(4 - len(length_data))
                if not chunk:
                    return None
                length_data += chunk
            
            message_length = int.from_bytes(length_data, byteorder='big')
            print(f"📏 Expecting message of length: {message_length} bytes")
            
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
            
            data = b''.join(chunks)
            print(f"📥 Received complete message: {len(data)} bytes")
            return data
                
        except socket.timeout:
            # print("⏰ Receive timeout (normal)")
            return None
        except Exception as e:
            print(f"❌ Receive error: {e}")
            return None
    
    
