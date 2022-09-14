FROM  python:3.9-alpine
MAINTAINER Andrey Gribunin 'andrey@gribunin.ru'
WORKDIR /app
ADD . /app
RUN pip install -r requirements.txt
CMD ["python", "app.py"]