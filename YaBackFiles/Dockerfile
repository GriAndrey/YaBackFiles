FROM  python:3.9-alpine
MAINTAINER Andrey Gribunin 'andrey@gribunin.ru'
WORKDIR /main
ADD . /main
RUN pip install -r requirements.txt
CMD ["python", "main.py"]
