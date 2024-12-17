FROM python:3.10-slim

# Cài đặt Java và Spark
RUN apt-get update && apt-get install -y default-jdk wget && \
  wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
  tar -xzf spark-3.4.4-bin-hadoop3.tgz -C /opt && \
  ln -s /opt/spark-3.4.4-bin-hadoop3 /opt/spark && \
  rm spark-3.4.4-bin-hadoop3.tgz && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Cài đặt biến môi trường
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Cài đặt Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy mã nguồn
WORKDIR /app
COPY app /app

# Chạy ứng dụng
CMD ["python", "main.py"]
