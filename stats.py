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

# Estado de monitoramento de memória por ciclo (FULL/DELTA)
_mem_cycle_name: str | None = None
_mem_cycle_start_mb: float = -1.0
_mem_cycle_peak_mb: float = -1.0
_mem_cycle_min_mb: float = -1.0
_mem_cycle_samples: int = 0


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
    global _mem_cycle_peak_mb, _mem_cycle_min_mb, _mem_cycle_samples
    mb = get_memory_mb()

    if _mem_cycle_name is not None and mb >= 0:
        if _mem_cycle_samples <= 0:
            _mem_cycle_peak_mb = mb
            _mem_cycle_min_mb = mb
            _mem_cycle_samples = 1
        else:
            if mb > _mem_cycle_peak_mb:
                _mem_cycle_peak_mb = mb
            if mb < _mem_cycle_min_mb:
                _mem_cycle_min_mb = mb
            _mem_cycle_samples += 1

    prefix = f"[{label}] " if label else ""
    if _mem_cycle_name is not None and _mem_cycle_peak_mb >= 0:
        logger.info(
            "%sMemória RSS: %.1f MB (ciclo=%s, pico=%.1f MB, min=%.1f MB)",
            prefix,
            mb,
            _mem_cycle_name,
            _mem_cycle_peak_mb,
            _mem_cycle_min_mb,
        )
    else:
        logger.info("%sMemória RSS: %.1f MB", prefix, mb)


def begin_memory_cycle(cycle_name: str) -> None:
    """Inicia monitoramento de memória de um ciclo."""
    global _mem_cycle_name, _mem_cycle_start_mb, _mem_cycle_peak_mb, _mem_cycle_min_mb, _mem_cycle_samples
    mb = get_memory_mb()
    _mem_cycle_name = cycle_name
    _mem_cycle_start_mb = mb
    _mem_cycle_peak_mb = mb
    _mem_cycle_min_mb = mb
    _mem_cycle_samples = 1 if mb >= 0 else 0
    logger.info("[MEM-CYCLE %s] início RSS: %.1f MB", cycle_name, mb)


def end_memory_cycle(cycle_name: str) -> None:
    """Finaliza monitoramento de memória de um ciclo e loga resumo."""
    global _mem_cycle_name, _mem_cycle_start_mb, _mem_cycle_peak_mb, _mem_cycle_min_mb, _mem_cycle_samples
    if _mem_cycle_name is None and _mem_cycle_samples <= 0 and _mem_cycle_start_mb < 0:
        return

    end_mb = get_memory_mb()
    active_name = _mem_cycle_name or cycle_name

    if end_mb >= 0:
        if _mem_cycle_samples <= 0:
            _mem_cycle_peak_mb = end_mb
            _mem_cycle_min_mb = end_mb
            _mem_cycle_samples = 1
        else:
            if end_mb > _mem_cycle_peak_mb:
                _mem_cycle_peak_mb = end_mb
            if end_mb < _mem_cycle_min_mb:
                _mem_cycle_min_mb = end_mb

    delta_end = (end_mb - _mem_cycle_start_mb) if (_mem_cycle_start_mb >= 0 and end_mb >= 0) else 0.0
    amplitude = (_mem_cycle_peak_mb - _mem_cycle_min_mb) if (_mem_cycle_peak_mb >= 0 and _mem_cycle_min_mb >= 0) else 0.0

    logger.info(
        "[MEM-CYCLE %s] resumo: início=%.1f MB | pico=%.1f MB | mínimo=%.1f MB | fim=%.1f MB | delta_fim_inicio=%+.1f MB | amplitude=%.1f MB | amostras=%d",
        active_name,
        _mem_cycle_start_mb,
        _mem_cycle_peak_mb,
        _mem_cycle_min_mb,
        end_mb,
        delta_end,
        amplitude,
        _mem_cycle_samples,
    )

    _mem_cycle_name = None
    _mem_cycle_start_mb = -1.0
    _mem_cycle_peak_mb = -1.0
    _mem_cycle_min_mb = -1.0
    _mem_cycle_samples = 0


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
        "[STATS %s]\n"
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
