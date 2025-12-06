#!/usr/bin/env python3
"""
system_info.py - System information utilities for scraper application
"""

import platform
import psutil
import shutil
import os
import time

import requests


def get_system_specs():
    """Get CPU, RAM, Disk information"""
    try:
        # CPU information
        cpu_name = platform.processor() or "Unknown Processor"
        cpu_cores = psutil.cpu_count(logical=False) or 1
        cpu_threads = psutil.cpu_count(logical=True) or 1

        # RAM information
        mem = psutil.virtual_memory()
        ram_total_gb = round(mem.total / (1024**3), 2)
        ram_available_gb = round(mem.available / (1024**3), 2)

        # Disk information (for system drive)
        disk = shutil.disk_usage(os.path.expanduser("~"))
        disk_free_gb = round(disk.free / (1024**3), 2)
        freq = psutil.cpu_freq()
        cpu_clock_ghz = round((freq.current if freq else 0) / 1000, 2) if freq else None

        return {
            'cpu_name': cpu_name,
            'cpu_cores': cpu_cores,
            'cpu_threads': cpu_threads,
            'cpu_clock_ghz': cpu_clock_ghz,
            'ram_total_gb': ram_total_gb,
            'ram_available_gb': ram_available_gb,
            'disk_free_gb': disk_free_gb
        }
    except Exception as e:
        print(f"Error getting system specs: {e}")
        return {
            'cpu_name': "Unknown",
            'cpu_cores': 1,
            'cpu_threads': 1,
            'cpu_clock_ghz': None,
            'ram_total_gb': 4.0,
            'ram_available_gb': 2.0,
            'disk_free_gb': 10.0
        }


def format_system_info(specs):
    """Format system specs for display"""
    lines = [
        f"🖥️  CPU: {specs['cpu_name']}",
        f"⚙️  Cores: {specs['cpu_cores']} cores / {specs['cpu_threads']} threads",
    ]
    if specs.get('cpu_clock_ghz'):
        lines.append(f"⏱️  Clock: {specs['cpu_clock_ghz']} GHz")
    lines.extend([
        f"💾 RAM: {specs['ram_available_gb']} GB tersedia dari {specs['ram_total_gb']} GB",
        f"💿 Disk: {specs['disk_free_gb']} GB tersedia"
    ])
    return "\n".join(lines)


def estimate_speed(cpu_cores, ram_gb, network_mbps: float | None = None, ping_ms: float | None = None):
    """
    Estimate scraping speed (Keluarga/second)

    Base speed is ~0.8 Keluarga/second per core
    RAM bonus: +10% for every 4GB above 8GB
    """
    # Conservative base speed per core
    base_speed = cpu_cores * 0.8

    # RAM bonus calculation
    ram_bonus = max(0, (ram_gb - 8) / 4 * 0.1)

    # Final estimated speed
    estimated = base_speed * (1 + ram_bonus)

    if network_mbps is not None:
        if network_mbps < 5:
            network_factor = 0.45
        elif network_mbps < 15:
            network_factor = 0.7
        elif network_mbps < 30:
            network_factor = 0.9
        elif network_mbps < 60:
            network_factor = 1.0
        else:
            network_factor = min(1.2, 1.0 + (network_mbps - 60) / 250)
        estimated *= network_factor

    if ping_ms is not None:
        if ping_ms > 800:
            ping_factor = 0.4
        elif ping_ms > 400:
            ping_factor = 0.6
        elif ping_ms > 200:
            ping_factor = 0.8
        elif ping_ms > 80:
            ping_factor = 0.95
        else:
            ping_factor = 1.05
        estimated *= ping_factor

    return round(max(0.3, estimated), 1)


def measure_network_metrics(test_url: str | None = None, max_bytes: int = 1_048_576) -> dict:
    """Measure approximate download speed (Mbps) and HTTP ping latency (ms)."""
    metrics = {"download_mbps": None, "ping_ms": None}

    # Measure latency via lightweight HTTP request
    try:
        ping_start = time.perf_counter()
        requests.get("https://www.google.com/generate_204", timeout=8)
        metrics["ping_ms"] = round((time.perf_counter() - ping_start) * 1000, 1)
    except Exception as exc:
        print(f"Error measuring latency: {exc}")

    url = test_url or "https://speed.cloudflare.com/__down?bytes=1048576"
    try:
        start = time.perf_counter()
        with requests.get(url, stream=True, timeout=20) as resp:
            resp.raise_for_status()
            downloaded = 0
            for chunk in resp.iter_content(65536):
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded >= max_bytes:
                    break
        elapsed = time.perf_counter() - start
        if elapsed > 0 and downloaded > 0:
            bits_per_second = (downloaded * 8) / elapsed
            metrics["download_mbps"] = round(bits_per_second / 1_000_000, 2)
    except Exception as exc:
        print(f"Error measuring network speed: {exc}")

    return metrics


def get_default_output_folder():
    """Get default output folder location (Desktop)"""
    try:
        # Get Desktop path
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")

        # Create output subfolder
        output_folder = os.path.join(desktop, "DTSEN_Output")

        return output_folder
    except Exception as e:
        print(f"Error getting desktop folder: {e}")
        # Fallback to current directory
        return os.path.join(os.getcwd(), "output")


if __name__ == "__main__":
    # Test the module
    specs = get_system_specs()
    print("=== System Information ===")
    print(format_system_info(specs))
    net_metrics = measure_network_metrics()
    print(f"Network: {net_metrics}")
    print(
        f"\nEstimated Speed: ~{estimate_speed(specs['cpu_cores'], specs['ram_total_gb'], net_metrics['download_mbps'], net_metrics['ping_ms'])} Keluarga/detik"
    )
    print(f"\nDefault Output Folder: {get_default_output_folder()}")
