# ---------------------------------------------------------------------------
# Airflow image extended with a local Spark runtime.
#
# The project brief specifies "Airflow (Python Operator) => PySpark", so Spark
# runs in-process inside the scheduler rather than on a separate cluster.
# That requires a JDK and the JDBC drivers to be present in this image.
# ---------------------------------------------------------------------------
FROM apache/airflow:2.9.3-python3.10

USER root

# --- Java 17: required by Spark 3.5 ---------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- JDBC drivers, baked in so nothing is downloaded at run time ----------
ENV SPARK_JARS_DIR=/opt/spark-jars
RUN mkdir -p ${SPARK_JARS_DIR} \
 && curl -fsSL -o ${SPARK_JARS_DIR}/mssql-jdbc.jar \
      https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/postgresql.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
 && chmod -R 755 ${SPARK_JARS_DIR}

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
