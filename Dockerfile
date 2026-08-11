FROM public.ecr.aws/d3j8x8q7/olympus-base-python:latest

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements/default.txt && \
    pip install --no-cache-dir pytest==8.3.4 pytest-subtests==0.14.0 && \
    pip install --no-cache-dir --no-deps -e .

CMD ["bash"]
