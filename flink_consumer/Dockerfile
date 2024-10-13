FROM flink:1.19.1-scala_2.12-java8
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip3 install apache-flink==1.19.1

# Install required file
#ADD src/flood_write_to_delta.py /opt/flink/usrlib/artifacts/
ADD ./lib /opt/flink/lib/
#DD ./requirements.txt /opt/flink/usrlib/artifacts/
#RUN pip3 install -r  /opt/flink/usrlib/artifacts/requirements.txt