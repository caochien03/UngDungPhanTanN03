#!/bin/bash

# Tăng giới hạn bộ nhớ cho Node.js
export NODE_OPTIONS="--max-old-space-size=512"

# Chạy server với node ID được truyền vào
node server.js $1 