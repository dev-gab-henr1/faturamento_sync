"""
Monitor de recursos do Faturamento Sync (poll.py)

Uso:
    python monitor.py                  # Inicia poll.py e monitora
    python monitor.py --interval 10    # Amostragem a cada 10s (padrão: 5s)
    python monitor.py --attach         # Conecta a um poll.py já rodando

Mostra: RSS, VMS, CPU%, threads, pico de memória e contadores de requests.
"""
import argparse
import subprocess
import sys
import time
import os

import psutil


def format_mb(bytes_val: int) -> str:
    return f"{bytes_val / (1024 * 1024):.1f} MB"


def format_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def find_poll_process() -> psutil.Process | None:
    """Encontra um poll.py já rodando."""
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any("poll.py" in arg for arg in cmdline):
                # Não pegar o próprio monitor
                if not any("monitor.py" in arg for arg in cmdline):
                    return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None


def print_header():
    print("=" * 80)
    print("  FATURAMENTO SYNC — Monitor de Recursos")
    print("=" * 80)
    print(
        f"  {'Uptime':<10} "
        f"{'RSS':<12} "
        f"{'VMS':<12} "
        f"{'CPU%':<8} "
        f"{'Threads':<9} "
        f"{'Pico RSS':<12}"
    )
    print("-" * 80)


def print_sample(
    uptime: float,
    rss: int,
    vms: int,
    cpu: float,
    threads: int,
    peak_rss: int,
):
    print(
        f"  {format_uptime(uptime):<10} "
        f"{format_mb(rss):<12} "
        f"{format_mb(vms):<12} "
        f"{cpu:<8.1f} "
        f"{threads:<9} "
        f"{format_mb(peak_rss):<12}"
    )


def print_summary(peak_rss: int, total_time: float, samples: int):
    print("-" * 80)
    print(f"  Pico RSS:        {format_mb(peak_rss)}")
    print(f"  Tempo total:     {format_uptime(total_time)}")
    print(f"  Amostras:        {samples}")
    print()

    # Estimativa Railway
    peak_gb = peak_rss / (1024 ** 3)
    idle_gb = peak_gb * 0.3  # estimativa idle = ~30% do pico
    avg_gb = (peak_gb + idle_gb) / 2
    monthly_cost = avg_gb * 0.000231 * 60 * 24 * 30
    print(f"  ── Estimativa Railway ──")
    print(f"  Pico:            {peak_gb:.3f} GB")
    print(f"  Média estimada:  {avg_gb:.3f} GB")
    print(f"  Custo RAM/mês:   ~${monthly_cost:.2f}")
    print(f"  (dentro do plano Hobby $5/mês)")
    print("=" * 80)


def monitor_process(ps: psutil.Process, interval: float, aggregate: bool = False):
    """Loop de monitoramento. Se aggregate=True, soma pai + filhos."""
    peak_rss = 0
    samples = 0
    start_time = time.time()

    print_header()

    try:
        while ps.is_running() and ps.status() != psutil.STATUS_ZOMBIE:
            try:
                if aggregate:
                    mem = ps.memory_info()
                    rss = mem.rss
                    vms = mem.vms
                    cpu = ps.cpu_percent(interval=min(1.0, interval))
                    threads = ps.num_threads()
                    for child in ps.children(recursive=True):
                        try:
                            cmem = child.memory_info()
                            rss += cmem.rss
                            vms += cmem.vms
                            threads += child.num_threads()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                else:
                    mem = ps.memory_info()
                    rss = mem.rss
                    vms = mem.vms
                    cpu = ps.cpu_percent(interval=min(1.0, interval))
                    threads = ps.num_threads()

                if rss > peak_rss:
                    peak_rss = rss

                uptime = time.time() - start_time
                samples += 1

                print_sample(uptime, rss, vms, cpu, threads, peak_rss)

                remaining = interval - min(1.0, interval)
                if remaining > 0:
                    time.sleep(remaining)

            except psutil.NoSuchProcess:
                print("\n  [processo encerrou]")
                break
            except psutil.AccessDenied:
                print("\n  [acesso negado ao processo]")
                break

    except KeyboardInterrupt:
        print("\n\n  [Ctrl+C — encerrando monitor]")

    total_time = time.time() - start_time
    print()
    print_summary(peak_rss, total_time, samples)

    return peak_rss

def main():
    parser = argparse.ArgumentParser(description="Monitor de recursos do Faturamento Sync")
    parser.add_argument(
        "--interval", type=float, default=5.0,
        help="Intervalo entre amostras em segundos (padrão: 5)",
    )
    parser.add_argument(
        "--attach", action="store_true",
        help="Conecta a um poll.py já rodando ao invés de iniciar um novo",
    )
    args = parser.parse_args()

    if args.attach:
        print("Procurando poll.py rodando...")
        ps = find_poll_process()
        if ps is None:
            print("Nenhum poll.py encontrado rodando. Inicie primeiro ou rode sem --attach.")
            return 1
        print(f"Conectado ao PID {ps.pid}")
        monitor_process(ps, args.interval)
        return 0

    # Iniciar poll.py como subprocess
    cmd = [sys.executable, "poll.py"]
    print(f"Iniciando: {' '.join(cmd)}")
    print()

    proc = subprocess.Popen(cmd)
    time.sleep(3)

    try:
        parent = psutil.Process(proc.pid)
    except psutil.NoSuchProcess:
        print("poll.py encerrou imediatamente. Verifique os logs.")
        return 1

    # Descobrir o processo real (pode ser filho no Windows)
    children = parent.children(recursive=True)
    if children:
        # Pega o filho com maior RSS (o Python real)
        ps = max(children, key=lambda p: p.memory_info().rss)
        print(f"Monitorando filho PID {ps.pid} (RSS: {format_mb(ps.memory_info().rss)})")
    else:
        ps = parent
        print(f"Monitorando PID {ps.pid} (RSS: {format_mb(ps.memory_info().rss)})")

    # Se RSS < 10MB, provavelmente é o wrapper — monitorar tudo junto
    if ps.memory_info().rss < 10 * 1024 * 1024:
        print("RSS muito baixo, monitorando processo pai + filhos somados.")
        ps = None  # flag para modo agregado

    print()

    try:
        peak = monitor_process(ps or parent, args.interval, aggregate=ps is None)
    finally:
        if proc.poll() is None:
            print("Encerrando poll.py...")
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    print(f"Exit code: {proc.returncode}")
    return 0


if __name__ == "__main__":
    sys.exit(main())