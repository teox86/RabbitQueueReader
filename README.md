# RabbitMQ Stream Viewer

A zero-dependency, read-only web viewer for RabbitMQ queues (including stream queues).

## Requirements

- Python 3.8+ (stdlib only — no pip installs needed)
- RabbitMQ with the **Management Plugin** enabled (`rabbitmq-plugins enable rabbitmq_management`)

## Start

| OS | How |
|----|-----|
| Windows | Double-click `start.bat` |
| macOS / Linux | Double-click `start.sh` (or `bash start.sh` in a terminal) |

The app opens at **http://127.0.0.1:8765** automatically in your default browser.

## Usage

1. Fill in **Host**, **Management Port** (default 15672), **VHost**, **Username**, **Password**.
2. Type a **Queue Name** (or click **Browse Queues** to pick one).
3. Click **Load Messages**.
4. Use the filter box to search payloads; click **Export JSON** to save.
5. Tick **Auto-refresh** to poll continuously.

## Read-only guarantee

Messages are fetched via `POST /api/queues/{vhost}/{queue}/get` with `"requeue": true`.  
This is the RabbitMQ Management API's peek operation — messages are returned to the queue immediately and are never deleted, acknowledged, or modified.
