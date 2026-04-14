FROM --platform=$BUILDPLATFORM python:3.12-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apk add --no-cache gcc musl-dev postgresql-dev

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM python:3.12-alpine

RUN apk add --no-cache libpq

WORKDIR /app
COPY --from=builder /install /usr/local
COPY worker/ ./worker/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-m", "worker.main"]
