#!/bin/bash

# Stop all running containers
sudo docker stop $(sudo docker ps -aq)

# Remove all containers
sudo docker rm $(sudo docker ps -aq)

# Remove all images
sudo docker rmi $(sudo docker images -q)

# Remove all volumes
sudo docker volume rm $(sudo docker volume ls -q)

# Clean up system (networks, build cache, etc.)
sudo docker system prune -a --volumes -f
