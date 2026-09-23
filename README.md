# Amplify Portable Interface - Python Library

This library is a Python implementation of the specification found in `../interface-spec/`.
It provides a standardized way to interact with AMPLIFY-compatible devices and
services using a Redis-backed communication layer.

## Purpose

The main goal of this library is to facilitate the connection, registration and
data exchange between different components of the AMPLIFY ecosystem. It
abstracts the underlying Redis operations into a simple API for managing data
streams.

## Features

- **Connection Management**: Connect to Redis hosts and manage device
  registration.
- **Stream Registration**: Register and unregister data streams with metadata
  (type and data type).
- **Pub/Sub Support**: Subscribe to streams to receive real-time updates and
  publish data to streams.
- **Stream Types**:
    - `discrete`: For data that occurs at specific intervals or events.
    - `continuous`: For streaming data.
- **Data Types**: Support for `number`, `string`, and `boolean` values.
- **Heartbeat Mechanism**: Automatic device registry heartbeat to maintain
  connection status.

## Requirements

- Python >= 3.8
- Redis server

## Usage Example

```python
import amplify_portable_interface as api

# Connect to the Redis host
connection = api.connect(host="localhost", type="producer")

# Register a new stream
stream = connection.register_stream(
    name="sensor_data",
    type="continuous",
    data_type="number"
)

# Publish data
stream.publish(23.5)

# Subscribe to a stream
def on_data(data):
    print(f"Received data: {data}")

other_stream = connection.get_stream("other_sensor")
if other_stream:
    other_stream.subscribe(on_data)
```
