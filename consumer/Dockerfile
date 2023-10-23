FROM python:3.11.6-bookworm
COPY . requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . consumer.py ./
CMD [ "python","-u", "./consumer.py" ]