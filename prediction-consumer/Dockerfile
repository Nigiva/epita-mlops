FROM python:3.8.16
RUN pip install --upgrade pip
RUN mkdir /app
WORKDIR /app
COPY requirements.txt /app
RUN pip install -r requirements.txt
COPY . /app
CMD ["python", "app.py"]
