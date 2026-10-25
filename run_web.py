#!/usr/bin/env python3
"""
Запуск веб-интерфейса одной командой:

  python3 run_web.py

При первом запуске сам поставит fastapi и uvicorn. Сервер: http://127.0.0.1:8000
"""
import subprocess
import sys


def ensure_deps():
    try:
        import uvicorn  # noqa: F401
        return True
    except ImportError:
        pass
    print("Устанавливаю зависимости веб-режима…")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "-q", "fastapi", "uvicorn[standard]"
    ])
    return True


if __name__ == "__main__":
    ensure_deps()
    import uvicorn
    uvicorn.run(
        "app.web.server:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
    )
