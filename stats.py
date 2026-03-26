"""
Monitoramento de requisições e memória.
Contadores globais + leitura de RSS do processo.
"""
import gc
import os
import time
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("faturamento_sync.stats")


@dataclass
class RequestStats:
    """Contadores de requisições por serviço."""
    clickup_requests: int = 0
    clickup_tasks_fetched: int = 0
    sheets_read_requests: int = 0
    sheets_write_requests: int = 0
    sheets_cells_written: int = 0
    powerrev_requests: int = 0
    powerrev_invoices_fetched: int = 0
    _start_time: float = field(default_factory=time.time)

    def reset(self) -> None:
        self.clickup_requests = 0
        self.clickup_tasks_fetched = 0
        self.sheets_read_requests = 0
        self.sheets_write_requests = 0
        self.sheets_cells_written = 0
        self.powerrev_requests = 0
        self.powerrev_invoices_fetched = 0
        self._start_time = time.time()

    @property
    def total_requests(self) -> int:
        return (
            self.clickup_requests
            + self.sheets_read_requests
            + self.sheets_write_requests
            + self.powerrev_requests
        )

    @property
    def uptime_s(self) -> float:
        return time.time() - self._start_time

    @staticmethod
    def get_memory_mb_safe() -> float:
        """Wrapper seguro para uso no heartbeat."""
        return get_memory_mb()


# Instância global
stats = RequestStats()

# Acumuladores lifetime
_lifetime_clickup = 0
_lifetime_sheets = 0
_lifetime_powerrev = 0


def get_memory_mb() -> float:
    """Lê RSS do processo. Suporta Linux e Windows."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except (FileNotFoundError, ValueError, IndexError):
        pass
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return usage.ru_maxrss / 1024
    except Exception:
        pass
    return -1.0


def log_memory(label: str = "") -> None:
    """Loga uso de memória atual."""
    mb = get_memory_mb()
    prefix = f"[{label}] " if label else ""
    logger.info("%sMemória RSS: %.1f MB", prefix, mb)


def force_free_memory() -> None:
    """gc.collect() + malloc_trim no Linux para devolver memória ao OS.

    CPython não libera arenas de memória de volta ao OS automaticamente.
    malloc_trim(0) força o glibc a devolver páginas livres, reduzindo RSS.
    No Windows ou sem glibc, apenas gc.collect() é executado.
    """
    gc.collect()
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
    except (OSError, AttributeError):
        pass


def log_sync_stats(sync_type: str) -> None:
    """Loga resumo de requisições do ciclo de sync atual."""
    global _lifetime_clickup, _lifetime_sheets, _lifetime_powerrev

    _lifetime_clickup += stats.clickup_requests
    _lifetime_sheets += stats.sheets_read_requests + stats.sheets_write_requests
    _lifetime_powerrev += stats.powerrev_requests

    logger.info(
        "─── STATS %s ───\n"
        "  ClickUp:  %d requests, %d tasks fetched\n"
        "  Sheets:   %d reads, %d writes, %d cells written\n"
        "  PowerRev: %d requests, %d invoices fetched\n"
        "  Total:    %d requests neste ciclo\n"
        "  Memória:  %.1f MB\n"
        "  Lifetime: %d ClickUp, %d Sheets, %d PowerRev (desde boot)",
        sync_type,
        stats.clickup_requests,
        stats.clickup_tasks_fetched,
        stats.sheets_read_requests,
        stats.sheets_write_requests,
        stats.sheets_cells_written,
        stats.powerrev_requests,
        stats.powerrev_invoices_fetched,
        stats.total_requests,
        get_memory_mb(),
        _lifetime_clickup,
        _lifetime_sheets,
        _lifetime_powerrev,
    )