#!/bin/bash
# EC2 user data for a new benchmark host (Amazon Linux 2023): build tools, a high open-file limit and deep socket
# backlogs. It is what the rig's hosts were launched with, plus rsync and tmux, which they got after boot. It leaves
# /var/tmp/userdata.done when finished. pintune.sh is separate: it runs after boot, from ~/offbench-src/scripts,
# and again after every reboot.
set -x
dnf -y install gcc gcc-c++ make git openssl-devel jemalloc-devel tcl python3 iperf3 sysstat perf numactl rsync tmux \
    >/var/log/userdata.log 2>&1
echo '* soft nofile 1048576' >> /etc/security/limits.conf
echo '* hard nofile 1048576' >> /etc/security/limits.conf
sysctl -w net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 net.ipv4.ip_local_port_range="1024 65535" \
    >>/var/log/userdata.log
touch /var/tmp/userdata.done
