"""FastAPI Web UI Application - Giao diện 4 Tab chuẩn Netflix cho Hệ thống Gợi ý Phim Big Data.
Chạy ứng dụng:
    uvicorn serving.web:app --reload --port 8000
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Tái sử dụng FastAPI app và toàn bộ endpoints nghiệp vụ từ serving.api
from serving.api import app

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Đảm bảo mount thư mục static
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------- 4 TAB GIAO DIỆN (UI ROUTES) ----------

@app.get("/", response_class=HTMLResponse)
async def tab_dashboard(request: Request):
    """TAB 1: Dashboard - Gợi ý cá nhân hóa Batch ALS & Top 20 Trending Real-time (User 1 mặc định)."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "active_tab": "dashboard",
            "current_user_id": 1
        }
    )


@app.get("/user/{user_id}", response_class=HTMLResponse)
async def tab_user_dashboard(request: Request, user_id: int):
    """TAB 1 (Biến thể theo User): Hiển thị gợi ý cho một User ID cụ thể."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "active_tab": "dashboard",
            "current_user_id": user_id
        }
    )


@app.get("/coldstart", response_class=HTMLResponse)
async def tab_coldstart(request: Request):
    """TAB 2: ColdStart - Tìm phim tương đồng Tag Genome (Item) & Fallback Top Trending (User)."""
    return templates.TemplateResponse(
        request=request,
        name="coldstart.html",
        context={
            "active_tab": "coldstart"
        }
    )


@app.get("/admin", response_class=HTMLResponse)
async def tab_admin(request: Request):
    """TAB 3: Vận hành & Demo - Kích hoạt Batch ALS, bắn 100 sự kiện Kafka và click boost Redis."""
    return templates.TemplateResponse(
        request=request,
        name="admin.html",
        context={
            "active_tab": "admin"
        }
    )

