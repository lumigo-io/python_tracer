from mock import mock_open, patch

from lumigo_tracer.extension.extension_utils import get_current_cpu_time, get_current_bandwidth, get_current_memory

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
    m().readlines.return_value = PROC_STAT.split("\n")
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        result = get_current_memory()
    assert result == 1


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
