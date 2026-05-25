FROM mcr.microsoft.com/oss/go/microsoft/golang:1.26-bookworm@sha256:0d073f5cd83f14fc344cd92be21994b90c91236c635b636eb9cc9bd601f6030a

# [Optional] Uncomment this section to install additional OS packages.
# graphviz for pprof
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends graphviz
