

import pyarrow as pa
import pyarrow.ipc as ipc
import requests
import pyarrow.flight as flight
import pyarrow.parquet as pq

from pyarrow import feather


def fetch_arrow_data(server_url):
    client = flight.FlightClient("grpc://localhost:8815")

    # Define the data descriptor
    descriptor = flight.FlightDescriptor.for_path("dataset_name")

    # Make a request to get data
    info = client.get_flight_info(descriptor)

    # Fetch the data stream
    import time

    before_current_millis = int(time.time() * 1000)
    print(before_current_millis)
    reader = client.do_get(info.endpoints[0].ticket)
    after_current_millis = int(time.time() * 1000)
    print(after_current_millis)

    print(f"Total time taken {after_current_millis - before_current_millis}")

    with pq.ParquetWriter("output_data.parquet", reader.schema) as writer:
        # Stream and write batches directly from the reader
        for chunk in reader:
            batch = chunk.data  # Extract RecordBatch from FlightStreamChunk
            writer.write_batch(batch)

    print("Data successfully written to output_data.feather")


if __name__ == '__main__':
    fetch_arrow_data('http://localhost:5000/dataset_ticket')






# import pyarrow.flight as flight
# from flask import Flask, Response, jsonify
# import time
#
# app = Flask(__name__)
#
# # Fetch and stream data
# def fetch_arrow_data():
#     client = flight.FlightClient("grpc://localhost:8815")
#     descriptor = flight.FlightDescriptor.for_path("dataset_name")
#     info = client.get_flight_info(descriptor)
#     reader = client.do_get(info.endpoints[0].ticket)
#     return reader
#
# # Create a Flask endpoint to stream data
# @app.route('/stream-data', methods=['GET'])
# def stream_data():
#     try:
#         reader = fetch_arrow_data()
#
#         def generate():
#             for chunk in reader:
#                 batch = chunk.data
#                 yield batch.to_pandas().to_json(orient='records') + "\n"  # Stream as JSON with newlines
#
#         return Response(generate(), mimetype='application/json')
#
#     except Exception as e:
#         print(f"Error fetching data: {e}")
#         return jsonify({"error": str(e)}), 500
#
# if __name__ == '__main__':
#     # Start the Flask app
#     print("Starting Flask API...")
#     app.run(port=5000)
