FROM python:3.6-alpine
MAINTAINER Jessey White-Cinis "jcinis@gmail.com"
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /usr/src/app
CMD ["python", "app.py"]
