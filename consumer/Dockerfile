FROM python:3.11.6-bookworm
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python","-u", "./consumer.py" ]