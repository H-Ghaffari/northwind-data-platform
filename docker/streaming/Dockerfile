# ---------------------------------------------------------------------------
# Image shared by the producer and the consumer.
#
# One image rather than two: they import the same modules under
# streaming/common, and a shared base makes it impossible for the two
# services to end up running against different versions of that code.
# ---------------------------------------------------------------------------
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# confluent-kafka and pymssql both ship manylinux wheels that bundle their
# C dependencies, so nothing needs compiling here. build-essential is left
# out deliberately; if a future dependency needs it, that should be a
# visible change rather than a permanently fattened image.
WORKDIR /app

COPY streaming/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY streaming /app/streaming

# Not root. Nothing here needs privileges, and the container has a live
# connection to the source database.
RUN useradd --create-home --uid 1000 stream
USER stream

CMD ["python", "-m", "streaming.producer.main"]