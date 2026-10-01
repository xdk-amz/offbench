#!/usr/bin/env bash
# Runs ON the metal host (sudo). Partitions the 96 CPUs so nothing shares a core with the server:
#   cpu0            housekeeping: sshd, valkey-cli control plane, the cell script, leftover kernel threads
#   cpu1            valkey main thread
#   cpu2-16         io_thd_1..io_thd_15 (io_thd_i -> cpu i+1; io-threads 16 = main + 15)
#   cpu17-32        IRQ/softirq set: every movable hardware IRQ, RPS for loopback rx, unbound workqueues
#   cpu33-95        cachecannon (one worker thread per core)
# Loopback has no hardware IRQ: the "interrupt handler" cost is NET_RX softirq, which the kernel
# runs inline on the SENDING core (netif_rx -> do_softirq). Receive Packet Steering on lo
# (rps_cpus) instead queues every skb to a core from the RPS set (IPI), so tcp_v4_rcv /
# tcp_rcv_established / socket wakeups for both directions run on the IRQ set, off the main,
# IO-thread and generator cores. RFS is deliberately NOT enabled: it would steer rx back to the
# consuming thread's core.
# env: MAINCPU=1 SRVCPUS=1-16 IRQCPUS=17-32 GENCPUS=33-95 BACKLOG=50000
set -u
IRQCPUS=${IRQCPUS:-17-32}; BACKLOG=${BACKLOG:-50000}
NCPU=$(nproc)
mask() { # cpu list -> comma-separated 32-bit hex groups as /sys and /proc/irq expect
  python3 - "$1" "$NCPU" <<'EOF'
import sys
lst, n = sys.argv[1], int(sys.argv[2])
bits = 0
for part in lst.split(','):
    a, _, b = part.partition('-'); a = int(a); b = int(b) if b else a
    for c in range(a, b + 1): bits |= 1 << c
words = (n + 31) // 32
print(','.join('%08x' % ((bits >> (32 * (words - 1 - i))) & 0xffffffff) for i in range(words)))
EOF
}
IRQMASK=$(mask $IRQCPUS)
HKMASK=$(mask "0,$IRQCPUS")
echo "=== pintune $(date -u +%FT%TZ) irq=$IRQCPUS mask=$IRQMASK"
sudo systemctl stop irqbalance 2>/dev/null; sudo systemctl disable irqbalance 2>/dev/null
echo "irqbalance: $(systemctl is-active irqbalance)"
# hardware IRQs (ENA queues, nvme, pcie hotplug, ...): per-cpu ones refuse the write, that is fine
moved=0; kept=0
for d in /proc/irq/[0-9]*; do
  if echo $IRQMASK | sudo tee $d/smp_affinity >/dev/null 2>&1; then moved=$((moved+1)); else kept=$((kept+1)); fi
done
echo "$IRQMASK" | sudo tee /proc/irq/default_smp_affinity >/dev/null
echo "irqs moved=$moved unmovable=$kept"
# loopback rx steering
for q in /sys/class/net/lo/queues/rx-*; do echo $IRQMASK | sudo tee $q/rps_cpus >/dev/null; done
echo 0 | sudo tee /proc/sys/net/core/rps_sock_flow_entries >/dev/null   # RFS off
sudo sysctl -q -w net.core.netdev_max_backlog=$BACKLOG
# ENA rx too, so ssh traffic also lands on the IRQ set (its IRQs are already there)
for q in /sys/class/net/en*/queues/rx-*; do echo $IRQMASK | sudo tee $q/rps_cpus >/dev/null 2>&1; done
# unbound kernel work off the server and generator cores
echo $HKMASK | sudo tee /sys/devices/virtual/workqueue/cpumask >/dev/null
echo $HKMASK | sudo tee /sys/bus/workqueue/devices/writeback/cpumask >/dev/null 2>&1
# move whatever userland is already running (sshd, agents, this shell's parent) onto cpu0 + irq set
for p in $(ps -eo pid= --no-headers); do sudo taskset -apc 0,$IRQCPUS $p >/dev/null 2>&1; done
echo "lo rps_cpus=$(cat /sys/class/net/lo/queues/rx-0/rps_cpus) backlog=$(sysctl -n net.core.netdev_max_backlog) wq=$(cat /sys/devices/virtual/workqueue/cpumask)"
echo "ena irq sample: $(grep -m1 -E 'en.*Tx-Rx-0' /proc/interrupts | awk '{print $1}' | tr -d : | xargs -I{} cat /proc/irq/{}/smp_affinity_list)"
echo "=== pintune done"
