FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt-get update && apt-get install sudo vim git -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

USER vscode

RUN rustup update && cargo install cargo-watch

RUN echo "alias ll='ls -ltrh'" >> /home/vscode/.bashrc
RUN echo "alias g=git" >> /home/vscode/.bashrc
RUN echo "alias c=cargo" >> /home/vscode/.bashrc
RUN echo "alias m=make" >> /home/vscode/.bashrc
