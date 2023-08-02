#!/bin/bash

sudo timedatectl set-ntp no
sudo timedatectl set-time "1990-01-01"
sudo make adit
sudo timedatectl set-ntp on