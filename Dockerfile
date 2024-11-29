FROM ubuntu:24.04 as base
ARG TARGETPLATFORM
COPY . /tmp
WORKDIR /tmp

RUN echo $TARGETPLATFORM
RUN ls -R /tmp/
# move the binary to root based on platform
RUN case $TARGETPLATFORM in \
        "linux/amd64")  BUILD=x86_64-unknown-linux-gnu  ;; \
        "linux/arm64")  BUILD=aarch64-unknown-linux-gnu  ;; \
        *) exit 1 ;; \
    esac; \
    mv /tmp/$BUILD/atm0s-sdn-standalone-$BUILD /atm0s-sdn-standalone; \
    chmod +x /atm0s-sdn-standalone

FROM ubuntu:24.04

COPY --from=base /atm0s-sdn-standalone /atm0s-sdn-standalone

ENTRYPOINT ["/atm0s-sdn-standalone"]