from mock import mock_open, patch

from lumigo_tracer.extension.extension_utils import (
    get_current_bandwidth,
    get_current_cpu_time,
    get_current_memory,
)

MEMINFO = """MemTotal:         100 kB
MemFree:           90432 kB
MemAvailable:     50 kB
Buffers:            6260 kB
Cached:            52344 kB
SwapCached:            0 kB
Active:            36364 kB
Inactive:          43728 kB
Active(anon):      21488 kB
Inactive(anon):      212 kB
Active(file):      14876 kB
Inactive(file):    43516 kB
Unevictable:           0 kB
Mlocked:               0 kB
SwapTotal:             0 kB
SwapFree:              0 kB
Dirty:               136 kB
Writeback:             0 kB
AnonPages:         21516 kB
Mapped:            35044 kB
Shmem:               228 kB
Slab:              13176 kB
SReclaimable:       3780 kB
SUnreclaim:         9396 kB
KernelStack:        1144 kB
PageTables:         1496 kB
NFS_Unstable:          0 kB
Bounce:                0 kB
WritebackTmp:          0 kB
CommitLimit:       96052 kB
Committed_AS:     157908 kB
VmallocTotal:   34359738367 kB
VmallocUsed:           0 kB
VmallocChunk:          0 kB
AnonHugePages:         0 kB
ShmemHugePages:        0 kB
ShmemPmdMapped:        0 kB
HugePages_Total:       0
HugePages_Free:        0
HugePages_Rsvd:        0
HugePages_Surp:        0
Hugepagesize:       2048 kB
DirectMap4k:       18432 kB
DirectMap2M:      194560 kB"""

PROC_STAT = """cpu  2165 34 2290 22625563 6290 127 456
cpu0 1132 34 1441 11311718 3675 127 438
cpu1 1123 0 849 11313845 2614 0 18
intr 114930548 113199788 3 0 5 263 0 4 [... lots more numbers ...]
ctxt 1990473
btime 1062191376
processes 2915
procs_running 1
procs_blocked 0"""

NET_STAT = """IpExt: InNoRoutes InTruncatedPkts InMcastPkts OutMcastPkts InBcastPkts OutBcastPkts InOctets OutOctets InMcastOctets OutMcastOctets InBcastOctets OutBcastOctets InCsumErrors InNoECTPkts InECT0Pktsu InCEPkts
IpExt: 0 0 0 0 277959 0 1234 1234 0 0 58649349 0 0 0 0 0"""


def test_get_current_cpu_time():
    m = mock_open()
    m().readlines.return_value = PROC_STAT.split("\n")
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_cpu_time()
    assert result == 44550


def test_get_current_memory():
    m = mock_open()
    m().read.return_value = MEMINFO
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_memory()
    assert result == 50


def test_get_current_memory_fails_gracfully():
    m = mock_open()
    m().read.return_value = "MEMINFO"
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_memory()
    assert result == 0


def test_get_current_cpu_time_fails():
    m = mock_open()
    m().readlines.return_value = ["cpu  2165"]  # malformed line
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_cpu_time()
    assert result is None


def test_get_current_bandwidth():
    m = mock_open()
    m().read.return_value = NET_STAT
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_bandwidth()
    assert result == 2468


def test_get_current_bandwidth_fails():
    m = mock_open()
    m().read.return_value = "IpExt: 0 0"
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_bandwidth()
    assert result is None
