#!/usr/bin/env python3
"""
MQTT Subscriber - Receives data from MQTT broker and logs to file
"""

import argparse
import paho.mqtt.client as mqtt
import json
import logging
import configparser
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error
import requests
import sys


import hashlib
from typing import Dict, List, Optional, Tuple
from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType
from meshcoredecoder.crypto import MeshCoreKeyStore
from meshcoredecoder.types.crypto import DecryptionOptions

LOGGER_LEVEL = logging.INFO
SEEN_TTL = timedelta(minutes=5)  # Time-to-live for seen message hashes

import requests


def create_discord_thread(channel_id, bot_token, thread_name):
    headers = {"Authorization": f"Bot {bot_token}"}
    
    # First, get channel info to retrieve guild_id
    channel_info_url = f"https://discord.com/api/v10/channels/{channel_id}"
    channel_response = requests.get(channel_info_url, headers=headers)
    
    if channel_response.status_code != 200:
        raise Exception(f"Failed to get channel info: {channel_response.status_code} - {channel_response.text}")
    
    channel_data = channel_response.json()
    guild_id = channel_data.get('guild_id')
    
    if not guild_id:
        raise Exception(f"Channel {channel_id} is not in a guild (DMs are not supported)")
    
    # Helper function to search threads in a list by name, filtering by parent channel
    def find_thread_by_name(threads_list, name, parent_channel_id):
        for thread in threads_list:
            # Check if thread belongs to this channel and name matches
            thread_parent_id = thread.get('parent_id')
            if str(thread_parent_id) == str(parent_channel_id) and thread.get('name') == name:
                return thread.get('id')
        return None
    
    # 1. Check for existing active threads using guild endpoint
    active_threads_url = f"https://discord.com/api/v10/guilds/{guild_id}/threads/active"
    get_response = requests.get(active_threads_url, headers=headers)
    
    if get_response.status_code == 200:
        response_data = get_response.json()
        threads = response_data.get('threads', [])
#        print(f"Checking {len(threads)} active threads in guild for '{thread_name}' in channel {channel_id}")
        for thread in threads:
#            print(f"  Active thread: '{thread.get('name')}' (parent: {thread.get('parent_id')}, id: {thread.get('id')})")
            thread_id = find_thread_by_name(threads, thread_name, channel_id)
            if thread_id:
                print(f"Found existing active thread: {thread_name} ({thread_id})")
                return thread_id
    else:
        print(f"Warning: Active threads endpoint returned {get_response.status_code}: {get_response.text}")
    
    # 2. Check for public archived threads using guild endpoint
    archived_public_url = f"https://discord.com/api/v10/guilds/{guild_id}/threads/archived/public"
    archived_public_response = requests.get(archived_public_url, headers=headers)
    
    if archived_public_response.status_code == 200:
        response_data = archived_public_response.json()
        archived_threads = response_data.get('threads', [])
#        print(f"Checking {len(archived_threads)} archived public threads in guild for '{thread_name}' in channel {channel_id}")
        for thread in archived_threads:
#            print(f"  Archived public thread: '{thread.get('name')}' (parent: {thread.get('parent_id')}, id: {thread.get('id')})")
            thread_id = find_thread_by_name(archived_threads, thread_name, channel_id)
            if thread_id:
                print(f"Found existing archived public thread: {thread_name} ({thread_id})")
                return thread_id
    
    # 3. Check for private archived threads (if bot has access)
    archived_private_url = f"https://discord.com/api/v10/guilds/{guild_id}/threads/archived/private"
    archived_private_response = requests.get(archived_private_url, headers=headers)
    
    if archived_private_response.status_code == 200:
        response_data = archived_private_response.json()
        archived_private_threads = response_data.get('threads', [])
#        print(f"Checking {len(archived_private_threads)} archived private threads in guild for '{thread_name}' in channel {channel_id}")
        thread_id = find_thread_by_name(archived_private_threads, thread_name, channel_id)
        if thread_id:
            print(f"Found existing archived private thread: {thread_name} ({thread_id})")
            return thread_id
    
    # 4. Create a new thread
    # For forum channels (type 15), threads are created the same way but might need an initial message
    # For text channels, we can create threads without a message
    create_url = f"https://discord.com/api/v10/channels/{channel_id}/threads"
    payload = {
        "name": thread_name,
        "type": 11,  # 11 = public_thread
        "auto_archive_duration": 1440*3  # 72 hours (1440 minutes * 3)
    }
    
    # For forum channels, we need to provide an initial message
    # Try without message first, if that fails with 400, try with message
    post_response = requests.post(create_url, headers=headers, json=payload)
    
    if post_response.status_code == 201:
        created_thread_id = post_response.json()['id']
        print(f"Created new thread: {thread_name} ({created_thread_id})")
        return created_thread_id
    elif post_response.status_code == 400:
        error_text = post_response.text
        # Check if it's a forum channel that requires an initial message
        if 'message' in error_text.lower() or 'content' in error_text.lower() or 'forum' in error_text.lower():
            # Try again with an initial message (forum threads require a message)
            payload_with_message = {
                **payload,
                "message": {
                    "content": f"Thread: {thread_name}"  # Initial message for forum threads
                }
            }
            retry_response = requests.post(create_url, headers=headers, json=payload_with_message)
            if retry_response.status_code == 201:
                created_thread_id = retry_response.json()['id']
                print(f"Created new forum thread: {thread_name} ({created_thread_id})")
                return created_thread_id
            else:
                raise Exception(f"Failed to create forum thread: {retry_response.status_code} - {retry_response.text}")
        else:
            raise Exception(f"Failed to create thread: {error_text}")
    else:
        raise Exception(f"Failed to create thread: {post_response.status_code} - {post_response.text}")

# 2. Your modified webhook function
#def send_discord_message(webhook_url, content, thread_id=None):
#    params = {'thread_id': thread_id} if thread_id else {}
#    data = {"content": content}
    
#    response = requests.post(webhook_url, json=data, params=params)
#    return response.status_code

# --- Example Usage ---
# BOT_TOKEN = "your_bot_token_here"
# CHANNEL_ID = "123456789"
# WEBHOOK_URL = "https://discord.com/api/webhooks/..."

# new_id = create_discord_thread(CHANNEL_ID, BOT_TOKEN, "New Discussion")
# send_discord_message(WEBHOOK_URL, "Hello inside the new thread!", thread_id=new_id)

def send_discord_message(
    webhook_url: str,
    thread_id: int,
    content: str = None,
    username: str = None,
    avatar_url: str = None,
    embeds: list = None
):
    """
    Send a message to a Discord channel via webhook.
    
    Args:
        webhook_url (str): Discord webhook URL.
        content (str, optional): Plain text message content.
        username (str, optional): Override default username.
        avatar_url (str, optional): Override default avatar image.
        embeds (list, optional): A list of embed dicts for rich messages.
    """
    data = {}
    if content:
        data["content"] = content
    if username:
        data["username"] = username
    if avatar_url:
        data["avatar_url"] = avatar_url
    if embeds:
        data["embeds"] = embeds
    params = {'thread_id': thread_id}
    response = requests.post(webhook_url, json=data, params=params)
    if response.status_code not in (200, 204):
        raise Exception(f"Failed to send message: {response.status_code} - {response.text}")

def get_color_for_channel(channel_name: str) -> int:
    """
    Generate a consistent color for a channel name using hash.
    Returns a color value between 0x333333 and 0xFFFFFF (avoiding very dark colors).
    """
    # Use hash to get a consistent value for the channel name
    hash_value = hash(channel_name)
    # Convert to positive and map to color range (0x333333 to 0xFFFFFF for good visibility)
    color_hash = abs(hash_value) % (0xFFFFFF - 0x333333 + 1)
    color = 0x333333 + color_hash
    return color

def send_discord_grouptext(webhook_url: str, thread_id: int, chnl_name: str, sender: str, message: str, path: str):
    str = f"**{chnl_name}** {sender}: {message}"
    if webhook_url != None:
        # Generate unique color for this channel
        channel_color = get_color_for_channel(chnl_name)
        embed = {
    #        "title": "CC9 HV4",
    #        "description": "Your automated build finished successfully.",
            "color": channel_color,
            "fields": [
                {"name": '', "value": str, "inline": False},
#                {"name": '', "value": path, "inline": True},
            ],
        "footer": {"text": path},
    #        "timestamp": datetime.now().isoformat() + 'Z'
        }
        send_discord_message(webhook_url, thread_id, embeds=[embed])
    print(str)

def format_path(path, origin):
    # Get the first two characters of origin, force uppercase
    origin_prefix = origin[:2]

    if path is None:
        # Return '[XX]' where XX is the origin prefix
        return f"[{origin_prefix}]"
    elif isinstance(path, list):
        # Format the list into a string '[item1,item2,XX]'
        prefix = f"[{','.join(path)}"
        return f"{prefix},{origin_prefix}]"
    else:
        # Handle unexpected types if necessary
        return str(path)


class MessageBot:
    def _post_discord_webhook(url: str, content: str) -> None:
        payload = {"content": content}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            # Some environments see Cloudflare 403 without an explicit UA
            "User-Agent": f"meshbot/1.0 (+https://example) Python/{sys.version_info[0]}.{sys.version_info[1]}"
        }
        req = urllib.request.Request(url, data=data, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            # Read to complete the request; response body is ignored
            _ = resp.read()
            
            
    async def send_to_discord(webhook_url: str, content: str) -> None:
        
        try:
            await asyncio.to_thread(_post_discord_webhook, webhook_url, content)
        except urllib.error.HTTPError as he:
            print(f"Discord webhook HTTP {he.code}: {he.reason}")
        except Exception as e:
            # Non-fatal: log and continue
            print(f"Discord webhook error: {e}")


    
    def __init__(self, config_file="config.ini"):
        """Initialize MQTT subscriber with configuration"""
        self.config = configparser.ConfigParser()
        # self.config.optionxform = str  # 👈 prevents lowercasing of option names
        self.config.read(config_file)

        # Get MQTT settings from config
        self.broker_url = self.config.get("mqtt", "mqtt_url")
        self.broker_port = self.config.getint("mqtt", "mqtt_port")
        self.username = self.config.get("mqtt", "mqtt_username")
        self.password = self.config.get("mqtt", "mqtt_password")

        # Topics to subscribe to
        topics_string = self.config.get("mqtt", "mqtt_topics")
        self.topics = [topic.strip() for topic in topics_string.split(',')]

        # Set up logging directory
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)

        # Set up file logging
        # log_file = self.log_dir / f"mqtt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.setup_logging()


        # Set up MQTT client
        if self.config.get("mqtt", "use_websockets",fallback='n') == 'y':
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, transport="websockets")
        else:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(self.username, self.password)

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        if self.config.get("mqtt", "tls_insecure",fallback='n') == 'y':
            self.client.tls_set(cert_reqs=mqtt.ssl.CERT_NONE)
            self.client.tls_insecure_set(True)
        else:
            self.client.tls_set()


        self.logger.info(f"Initialized MQTT subscriber for broker: {self.broker_url}:{self.broker_port}")
        self.logger.info(f"Subscribed topics: {self.topics}")

        self.webhook_url = self.config.get("discord", "webhook_url", fallback=None)
        self.logger.info(f"Discord webhook URL: {self.webhook_url}")
        self.msgbot_token = self.config.get("discord", "msgbot_token", fallback=None)
        self.logger.info(f"MSGBot Token: {self.msgbot_token}")
        self.discord_channel_id = self.config.get("discord", "channel_id", fallback=None)
        self.logger.info(f"Discord Channel ID: {self.discord_channel_id}")
        print(f"Discord Channel ID: {self.discord_channel_id}")

        # Message hash tracking to avoid duplicates (dict: hash -> timestamp)
        self.seen_message_hashes: Dict[str, datetime] = {}
        
        # Aggregate messages by channel for output
        self.channel_messages: Dict[str, List[Dict]] = {}

        # Store thread_id for each channel name (channel_name -> thread_id)
        self.channel_thread_ids: Dict[str, int] = {}

        self.channel_keys_by_hash, self.channel_keys_by_name = self._load_channel_keys(self.config)
        if self.channel_keys_by_hash:
            # Initialize key store (library will compute hashes)
            key_store = MeshCoreKeyStore({
                'channel_secrets': list(self.channel_keys_by_hash.values())
            })
            self.decryption_options = DecryptionOptions(key_store=key_store)

        

    def setup_logging(self):
        """Set up logging to both console and file"""
        # Create logger
        self.logger = logging.getLogger("mqtt_subscriber")
        self.logger.setLevel(LOGGER_LEVEL)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
#        console_handler.setLevel(logging.ERROR)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        # File handler for general logs
        # file_handler = logging.FileHandler(log_file)
        # file_handler.setLevel(logging.INFO)
        # file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        # file_handler.setFormatter(file_formatter)

        self.logger.addHandler(console_handler)
        # self.logger.addHandler(file_handler)

        # Data file for structured logging (symlink to current week's file)
       # self.data_log_symlink = self.log_dir / "data_log.jsonl"

    def _compute_channel_hash(self, secret_key: str) -> str:
        """First byte of SHA256 of the channel secret, as uppercase hex"""
        try:
            if len(secret_key) == 32:
                key_bytes = bytes.fromhex(secret_key)
            else:
                key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key
        except ValueError:
            key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key
        h = hashlib.sha256(key_bytes).digest()[0]
        return f"{h:02X}"

    def _load_channel_keys(self, config: configparser.ConfigParser) -> Tuple[Dict[str, str], Dict[str, Tuple[str, str]]]:
        """Return (by_hash, by_name) channel key maps from config"""
        by_hash: Dict[str, str] = {}
        by_name: Dict[str, Tuple[str, str]] = {}
        print("channels:")
        for name in config.options('channels'):
            # Strip leading and trailing quotes (both single and double) from channel name
            channel_name = name.strip()
            if (channel_name.startswith("'") and channel_name.endswith("'")) or \
               (channel_name.startswith('"') and channel_name.endswith('"')):
                channel_name = channel_name[1:-1]
            thread_id = create_discord_thread(self.discord_channel_id, self.msgbot_token, channel_name)
            # Store thread_id using the modified channel name
            self.channel_thread_ids[channel_name] = thread_id
            print(f"{channel_name} {thread_id}")
            secret = config.get('channels', name).strip()
            chash = self._compute_channel_hash(secret)
            self.logger.info(f" {channel_name}:{thread_id}:{secret}:{chash}")
            print(f" {channel_name}:{thread_id}:{secret}:{chash}")
            by_hash[chash] = secret
            by_name[channel_name] = (chash, secret)
        return by_hash, by_name

    def process_packet(self, entry):
        """Process a single packet entry"""
        try:
            data = entry.get('data', {})

            # Skip if not a PACKET type
            if data.get('type') != 'PACKET':
                return

            raw_hex = data.get('raw', '')
            if not raw_hex:
                return
            # Decode and check if it's a GroupText packet
            self.decode_and_store(raw_hex, entry, data)
        except Exception as e:
            self.logger.error(f"Error processing packet: {e}")

    def decode_and_store(self, hex_string: str, entry: Dict, packet_data: Dict):
        """Decode a packet and store into per-channel buckets"""
        try:
            # Decode the packet (with decryption if keys available)
            packet = MeshCoreDecoder.decode(hex_string, self.decryption_options)
            # Only process valid GroupText packets
            if not packet.is_valid or packet.payload_type != PayloadType.GroupText:
                return
            payload = packet.payload
            if not payload or not payload.get('decoded'):
                return
            group_text = payload['decoded']

            # filter: Skip if we've already seen this message hash
            message_hash = packet.message_hash
            now = datetime.now()
                
            # Clean old entries and check for duplicates
            if message_hash in self.seen_message_hashes:
                # Check if entry is still valid (not expired)
                if now - self.seen_message_hashes[message_hash] < SEEN_TTL:
                    return
                else:
                    # Entry expired, remove it
                    del self.seen_message_hashes[message_hash]
                    
            # Clean up any other expired entries (lazy cleanup)
            expired_hashes = [
                h for h, ts in self.seen_message_hashes.items()
                if now - ts >= SEEN_TTL
            ]
            for h in expired_hashes:
                del self.seen_message_hashes[h]
                
            # Add current message hash with timestamp
            self.seen_message_hashes[message_hash] = now

            # Build message entry (with metadata)
            channel_hash = group_text.channel_hash if hasattr(group_text, 'channel_hash') else None
            message_entry = {
                'message_hash': message_hash,
                'timestamp': entry.get('timestamp'),
                'received_time': datetime.now().isoformat() + 'Z',
                'origin': packet_data.get('origin', '').rstrip(),
                'origin_id': packet_data.get('origin_id', ''),
                'route_type': packet.route_type.name if packet.route_type else None,
                'channel_hash': channel_hash,
                'path': packet.path,
                'SNR': packet_data.get('SNR'),
                'RSSI': packet_data.get('RSSI'),
                'score': packet_data.get('score'),
                'decrypted': False
            }

            path_str = format_path(packet.path, message_entry['origin_id'])
#            print(f"path {path_str}")

            # Determine channel bucket name
            bucket_name = None

            # If decrypted via provided keys
            if hasattr(group_text, 'decrypted') and group_text.decrypted:
                decrypted = group_text.decrypted
                message_entry['decrypted'] = True
                message_entry['sender'] = decrypted.get('sender', '')
                message_entry['message'] = decrypted.get('message', '')
                if decrypted.get('timestamp'):
                    message_entry['message_timestamp'] = datetime.fromtimestamp(
                        decrypted['timestamp']
                    ).isoformat() + 'Z'

                # Map channel hash to configured channel name if possible
                channel_name = None
                for name, (h, _) in self.channel_keys_by_name.items():
                    if channel_hash and h.upper() == channel_hash.upper():
                        channel_name = name
                        break
                bucket_name = channel_name or (f"encrypted_{channel_hash}" if channel_hash else "encrypted_unknown")
                print(f"{bucket_name}:{message_entry['sender']}:{message_entry['message']}")
                sender = message_entry['sender']
                msg = message_entry['message']
                # Retrieve thread_id for this channel
                thread_id = self.channel_thread_ids.get(channel_name) if channel_name else None
                if thread_id and self.webhook_url:
                    send_discord_grouptext(self.webhook_url, thread_id, bucket_name, sender, msg, path_str)
                else:
                    # Channel not in config or no thread_id available - still print but don't send to Discord
                    if not thread_id:
                        self.logger.warning(f"No thread_id found for channel: {channel_name or bucket_name}, skipping Discord")
                    msg_str = f"**{bucket_name}** {sender}: {msg}"
                    print(msg_str)
                #print(message_entry.get('path','Direct'))
                #print(json.dumps(message_entry, indent=2))
                #print(json.dumps(message_entry))
            else:
                # Not decrypted
                if hasattr(group_text, 'ciphertext'):
                    message_entry['ciphertext'] = group_text.ciphertext[:64] + '...' if len(group_text.ciphertext) > 64 else group_text.ciphertext
                bucket_name = f"encrypted_{channel_hash}" if channel_hash else "encrypted_unknown"

            # Append to bucket
            if bucket_name not in self.channel_messages:
                self.channel_messages[bucket_name] = []
            self.channel_messages[bucket_name].append(message_entry)

        except Exception as e:
            self.logger.error(f"Error decoding GroupText packet: {e}")


    def log_message_data(self, topic, payload):
        """Log message data in structured JSON Lines format"""
        try:
            # Check if we need to rotate to a new week's log file
            #self._check_log_rotation()

            # Parse JSON if possible, otherwise keep as string
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                data = {"raw_data": payload}

            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "topic": topic,
                "data": data
            }
            #self.logger.info(json.dumps(log_entry))
            self.process_packet(log_entry)

            #with open(self.data_log_file, 'a') as f:
            #    f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            self.logger.error(f"Error logging message data: {e}")

    def on_connect(self, client, userdata, connect_flags, reason_code, properties):
        """Callback for when client connects to broker"""
        if reason_code == 0:
            self.logger.info("Successfully connected to MQTT broker")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"Failed to connect to broker, reason code {reason_code}")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8')

        # Log to console and file
        self.logger.debug(f"Received message from topic: {topic}")
        self.logger.debug(f"Payload length: {len(payload)} bytes")

        # Log structured data
        self.log_message_data(topic, payload)

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Callback for when client disconnects from broker"""
        if reason_code != 0:
            self.logger.warning(f"Unexpected disconnection from broker (reason_code={reason_code})")
        else:
            self.logger.info("Disconnected from broker")

    def start(self):
        """Start the MQTT subscriber"""
        try:
            self.logger.info(f"Connecting to MQTT broker at {self.broker_url}:{self.broker_port}")
            self.client.connect(self.broker_url, self.broker_port, 60)

            # Start the loop to process callbacks
            self.logger.info("Starting MQTT subscriber loop...")
            self.logger.info("Press Ctrl+C to stop")
            self.client.loop_forever()

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
            self.client.loop_stop()
            self.client.disconnect()
            self.logger.info("MQTT subscriber stopped")
        except Exception as e:
            self.logger.error(f"Error in MQTT subscriber: {e}")
            raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="MQTT subscriber message bot")
    parser.add_argument(
        "-c",
        "--config",
        dest="config_file",
        help="Path to config.ini file"
    )
    args = parser.parse_args()

    if not args.config_file:
        parser.print_help()
        return

    subscriber = MessageBot(config_file=args.config_file)
    subscriber.start()


if __name__ == "__main__":
    main()
