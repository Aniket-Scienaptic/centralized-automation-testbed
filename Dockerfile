# python3.9 lambda base image
FROM public.ecr.aws/lambda/python:3.9
WORKDIR /srv
ADD ./requirements.txt /srv/requirements.txt
RUN pip3 install -r requirements.txt
ADD . /srv
ENTRYPOINT python3 /srv/main.py