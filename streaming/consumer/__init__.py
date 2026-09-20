"""The streaming consumer: Kafka events applied to NorthwindRT.

The brief calls this layer the staging of the streaming path, and that is
accurate: with no staging database, everything staging did for the batch
path — type coercion, null normalisation, lookup resolution, SCD rules —
happens here, per event instead of per table.
"""