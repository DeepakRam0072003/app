def inject_websocket_code():
    """Returns JavaScript code to inject WebSocket functionality"""
    return """
    <script src="/static/js/websocket.js"></script>
    <style>
        .ws-notification {
            position: fixed;
            bottom: 20px;
            right: 20px;
            padding: 12px 16px;
            color: white;
            border-radius: 4px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
            animation: fadeIn 0.3s;
            display: flex;
            align-items: center;
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeOut {
            from { opacity: 1; }
            to { opacity: 0; }
        }
    </style>
    """