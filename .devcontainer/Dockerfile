FROM openjdk:8-jdk

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ARG EMBULK_VERSION=0.9.23
ARG EMBULK_PATH=/usr/local/bin/embulk
ARG EMBULK_DOWNLOAD_URL="https://dl.bintray.com/embulk/maven/embulk-${EMBULK_VERSION}.jar"

ARG AMMONITE_VERSION=2.1.4
ARG AMMONITE_SCALA_VERSION=2.13
ARG AMMONITE_PATH=/usr/local/bin/amm
ARG AMMONITE_DOWNLOAD_URL="https://github.com/lihaoyi/Ammonite/releases/download/${AMMONITE_VERSION}/${AMMONITE_SCALA_VERSION}-${AMMONITE_VERSION}"

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
    && groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && apt-get -y install git openssh-client less iproute2 procps curl lsb-release \
    && curl --create-dirs -o ${EMBULK_PATH} -L "${EMBULK_DOWNLOAD_URL}" \
    && chmod +x ${EMBULK_PATH} \
    && curl --create-dirs -o ${AMMONITE_PATH} -L "${AMMONITE_DOWNLOAD_URL}" \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

ENV DEBIAN_FRONTEND=dialog
