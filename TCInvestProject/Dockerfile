FROM apache/airflow:2.7.1
#RUN mkdir /code
ENV TZ=Asia/Ho_Chi_Minh
COPY requirements.txt requirements.txt

ENV LD_LIBRARY_PATH="/path/to/odbc_driver_folder"
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt
USER root

RUN apt-get update && apt-get install -y unixodbc unixodbc-dev
USER airflow