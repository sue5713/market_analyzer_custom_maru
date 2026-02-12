import requests
import os
import sys
import json
import time

def send_line_push(message, access_token, user_id):
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
        "to": user_id,
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }
    
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
        print("Message sent successfully.")
        return True
    except Exception as e:
        print(f"Failed to send message: {e}")
        if 'r' in locals():
            print(f"Response: {r.text}")
        return False

def main():
    access_token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
    user_id = os.environ.get("LINE_USER_ID")
    
    if not access_token:
        print("Error: LINE_CHANNEL_ACCESS_TOKEN not found.")
        return
    if not user_id:
        print("Error: LINE_USER_ID not found.")
        return

    output_file = "analysis_output.txt"
    if not os.path.exists(output_file):
        print(f"Error: {output_file} not found.")
        return

    with open(output_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Split report by dashed lines (sectors)
    # The first part (Indices + Macro) comes before the first dashed line
    delimiter = "--------------------"
    raw_chunks = content.split(delimiter)
    
    # Process chunks to ensure they are not empty and add delimiters back for readability if needed
    # Actually, for LINE, we can just send each raw chunk as a separate message bubble
    # Grouping small chunks is better to avoid too many messages, but user asked for splitting by dashed lines.
    # Indices + Macro part is usually large enough.
    # Each sector is relatively small, maybe we can group 2-3 sectors per message?
    # User said: "dot line de kugitte aru naiyou goto ni wakete" -> Split by dotted line content.
    
    # Group chunks to save quota (Max 5000 chars/msg, target ~4000)
    messages_to_send = []
    current_buffer = ""
    MAX_LENGTH = 4000
    
    for chunk in raw_chunks:
        clean_chunk = chunk.strip()
        if not clean_chunk: continue
        
        # Re-add the delimiter for visual separation within the message
        # (Except for the very last one, but simpler to just add it)
        formatted_chunk = clean_chunk + "\n\n" + "-"*20 + "\n\n"
        
        if len(current_buffer) + len(formatted_chunk) > MAX_LENGTH:
            # Current buffer is full, save it
            messages_to_send.append(current_buffer.strip())
            current_buffer = formatted_chunk
        else:
            current_buffer += formatted_chunk
            
    if current_buffer:
        messages_to_send.append(current_buffer.strip())

    print(f"Total messages to send: {len(messages_to_send)} (Optimized from {len(raw_chunks)})")
    
    for i, msg in enumerate(messages_to_send):
        print(f"Sending message {i+1}/{len(messages_to_send)}...")
        
        # Add page counter
        header = f"({i+1}/{len(messages_to_send)})\n"
        final_msg = header + msg
        
        # Safety check for 5000 limit
        if len(final_msg) > 5000:
             print("Warning: Message > 5000 chars! Truncating to 5000.")
             final_msg = final_msg[:5000]
        
        success = send_line_push(final_msg, access_token, user_id)
        if not success:
            print("Stopping due to error.")
            break
        # Wait to ensure order
        time.sleep(1)

if __name__ == "__main__":
    main()
