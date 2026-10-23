
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from app.producer import publish_order

app = Flask(__name__)


@app.get("/")
def home():
    return jsonify({
        "service": "Real-Time Order Event API",
        "status": "running",
        "endpoints": [
            "GET /health",
            "POST /orders"
        ]
    })


@app.get("/health")
def health():
    return jsonify({"status": "healthy"}), 200


@app.post("/orders")
def create_order():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({"error": "A JSON object is required"}), 400

    if not data.get("product"):
        return jsonify({"error": "product is required"}), 400

    try:
        quantity = int(data.get("quantity", 1))
    except (ValueError, TypeError):
        return jsonify({"error": "quantity must be an integer"}), 400

    if quantity < 1:
        return jsonify({"error": "quantity must be positive"}), 400

    order = {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.created",
        "order_id": str(uuid.uuid4()),
        "product": data["product"],
        "quantity": quantity,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        publish_order(order)
    except Exception:
        app.logger.exception("Unable to publish order event")
        return jsonify({
            "error": "Order event could not be delivered"
        }), 503

    return jsonify({
        "message": "Order event accepted by Kafka",
        "order": order
    }), 202
