FROM python:3.9-alpine

COPY . /kaspaservices/
WORKDIR /kaspaservices/
RUN pip install -r requirements.txt

EXPOSE 5000
ENTRYPOINT [ "python" ]
CMD [ "kaspaservices.py" ]
