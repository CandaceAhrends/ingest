import WebSocket from 'ws';
import fs from 'fs';
import path from 'path';

// Create WebSocket connection
const ws = new WebSocket('ws://127.0.0.1:9001');

// Handle WebSocket connection
ws.on('open', () => {
    console.log('Connected to WebSocket server');
});

ws.on('message', (data) => {
    try {
        const message = JSON.parse(data);
        
        // Add timestamp to the message
        message.timestamp = new Date().toISOString();
        
        // Create filename with timestamp
        const timestamp = new Date().toISOString();
        const filename = `stock-event-${timestamp.replace(/[:.]/g, '-')}.json`;
        const filepath = path.join('data', filename);
        
        // Ensure data directory exists
        fs.mkdirSync(path.dirname(filepath), { recursive: true });
        
        // Write single event to file
        fs.writeFileSync(filepath, JSON.stringify(message, null, 2));
        console.log(`Wrote event to ${filename}`);
    } catch (error) {
        console.error('Error processing WebSocket message:', error);
    }
});

ws.on('close', () => {
    console.log('WebSocket connection closed');
});

ws.on('error', (error) => {
    console.error('WebSocket error:', error);
});

console.log('Stock event collector started - one file per event');