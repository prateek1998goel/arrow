# Code to read data directly as pyarrow from CSV
# Taking 4-8sec for 850MB
# import pyarrow as pa
# import pyarrow.csv as csv
# import pyarrow.ipc as ipc
# from flask import Flask, Response
# import pyarrow.flight as flight
#
# app = Flask(__name__)
#
#
# class ArrowFlightServer(flight.FlightServerBase):
#     def __init__(self, location="grpc://0.0.0.0:8815"):
#         super().__init__(location)
#         self.location = location
#
#     def get_flight_info(self, context, descriptor):
#         # Define the schema of the data
#         schema = pa.schema([
#             ('Year', pa.int64()),
#             ('Age', pa.int64()),
#             ('Ethnic', pa.int64()),
#             ('Area', pa.string()),
#             ('count', pa.string()),
#             # Add more columns based on your CSV file schema
#         ])
#
#         # Create FlightInfo to describe the dataset
#         ticket = flight.Ticket(b"dataset_ticket")
#
#         # Create FlightInfo to describe the dataset
#         return flight.FlightInfo(
#             schema,  # Data schema
#             descriptor,  # Data descriptor
#             endpoints=[flight.FlightEndpoint(ticket, [self.location])],
#             total_records=1000,  # Placeholder for total number of records
#             total_bytes=100000  # Placeholder for size in bytes
#         )
#
#     def do_get(self, context, ticket):
#         if ticket.ticket == b"dataset_ticket":
#             import time
#             before1_current_millis = int(time.time() * 1000)
#             print(before1_current_millis)
#             table = csv.read_csv("data1.csv")
#             batches = table.to_batches()
#             after1_current_millis = int(time.time() * 1000)
#             print(after1_current_millis)
#
#             print(f"Total time taken to read the file {after1_current_millis - before1_current_millis}")
#
#             # Create a record batch reader to stream the data
#             return flight.RecordBatchStream(pa.Table.from_batches(batches))
#         else:
#             raise flight.FlightServerError("Invalid ticket")
#
#
# if __name__ == '__main__':
#     server = ArrowFlightServer()
#     print(f"Starting server on {server.location}")
#     server.serve()
#
#
#


# # Code to convert the data frame to arrow format
# # Taking 55sec for 850MB
# import pyarrow as pa
# import pyarrow.csv as csv
# import pyarrow.ipc as ipc
# from flask import Flask, Response
# import pyarrow.flight as flight
# from pyspark.sql import SparkSession
#
# app = Flask(__name__)
#
# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Arrow Flight Server") \
#     .config("spark.executor.memory", "8g") \
#     .config("spark.driver.memory", "8g") \
#     .config("spark.default.parallelism", "1000") \
#     .config("spark.sql.shuffle.partitions", "1000") \
#     .config("spark.executor.instances", "10") \
#     .config("spark.executor.cores", "4") \
#     .config("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024) \
#     .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     .config("spark.driver.maxResultSize", "3g") \
#     .getOrCreate()
#
#
# class ArrowFlightServer(flight.FlightServerBase):
#     def __init__(self, location="grpc://0.0.0.0:8815"):
#         super().__init__(location)
#         self.location = location
#         self.df = spark.read.csv("data1.csv", header=True, inferSchema=True)
#
#
#     def get_flight_info(self, context, descriptor):
#         # Define the schema of the data
#         schema = pa.schema([
#             ('Year', pa.int64()),
#             ('Age', pa.int64()),
#             ('Ethnic', pa.int64()),
#             ('Area', pa.string()),
#             ('count', pa.string()),
#             # Add more columns based on your CSV file schema
#         ])
#
#         # Create FlightInfo to describe the dataset
#         ticket = flight.Ticket(b"dataset_ticket")
#
#         # Create FlightInfo to describe the dataset
#         return flight.FlightInfo(
#             schema,  # Data schema
#             descriptor,  # Data descriptor
#             endpoints=[flight.FlightEndpoint(ticket, [self.location])],
#             total_records=1000,  # Placeholder for total number of records
#             total_bytes=100000  # Placeholder for size in bytes
#         )
#
#     def do_get(self, context, ticket):
#         if ticket.ticket == b"dataset_ticket":
#             import time
#             before1_current_millis = int(time.time() * 1000)
#             print(before1_current_millis)
#
#             # # Read CSV using PySpark
#             # df = spark.read.csv("data1.csv", header=True, inferSchema=True)
#
#             # Use the Spark Arrow conversion
#             # Create a PyArrow Table directly from the Spark DataFrame
#             arrow_table = pa.Table.from_pandas(self.df.toPandas(), preserve_index=False)
#             batches = arrow_table.to_batches()
#
#             after1_current_millis = int(time.time() * 1000)
#             print(after1_current_millis)
#             print(f"Total time taken to read the file {after1_current_millis - before1_current_millis}")
#
#             # Create a record batch reader to stream the data
#             return flight.RecordBatchStream(pa.Table.from_batches(batches))
#             # return flight.RecordBatchStream(arrow_table)
#         else:
#             raise flight.FlightServerError("Invalid ticket")
#
#
# if __name__ == '__main__':
#     server = ArrowFlightServer()
#     print(f"Starting server on {server.location}")
#     server.serve()


import pyarrow as pa
import pyarrow.flight as flight
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Arrow Flight Server") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.default.parallelism", "1000") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .getOrCreate()

# Define the Arrow Flight server
class ArrowFlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self.location = location
        self.df = spark.read.csv("data1.csv", header=True, inferSchema=True).cache()

    def get_flight_info(self, context, descriptor):
        schema = pa.schema([
            ('Year', pa.int64()),
            ('Age', pa.int64()),
            ('Ethnic', pa.int64()),
            ('Area', pa.string()),
            ('count', pa.string())
        ])
        # total_records = self.df.count()
        ticket = flight.Ticket(b"dataset_ticket")
        return flight.FlightInfo(
            schema,  # Data schema
            descriptor,  # Data descriptor
            endpoints=[flight.FlightEndpoint(ticket, [self.location])],
            total_records=1000,  # Placeholder for total number of records
            total_bytes=100000  # Placeholder for size in bytes
        )
        # return flight.FlightInfo(
        #     schema,
        #     descriptor,
        #     endpoints=[flight.FlightEndpoint(ticket, [self.location])],
        #     total_records=total_records,
        #     total_bytes=total_records * schema.num_fields * 8  # Estimate size in bytes
        # )

    def do_get(self, context, ticket):
        if ticket.ticket == b"dataset_ticket":
            arrow_table = self.df.to_arrow()
            return flight.RecordBatchStream(arrow_table)
        else:
            raise flight.FlightServerError("Invalid ticket")

def start_arrow_flight_server():
    server = ArrowFlightServer()
    print("Starting Arrow Flight Server...")
    server.serve()

if __name__ == '__main__':
    start_arrow_flight_server()


