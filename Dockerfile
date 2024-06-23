ARG MILENA_VERSION
FROM --platform=linux/amd64 debian:bookworm-slim
WORKDIR /tmp
ADD https://github.com/VanceLongwill/milena/releases/download/$MILENA_VERSION/milena-x86_64-unknown-linux-gnu.tar.gz milena
RUN tar xvfz milena -C /usr/local/bin/
RUN rm -rf /tmp/milena
ENTRYPOINT ["milena"]
