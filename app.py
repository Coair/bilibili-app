import os
import io
import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO
import yt_dlp

app = Flask(__name__, static_folder='frontend/dist', static_url_path='')
CORS(app)
# 关键修复：使用 threading 模式而非 eventlet，避免 monkey-patch 导致 yt-dlp 线程卡死
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ---------- 配置路径 ----------
BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / 'config.json'
HISTORY_FILE = BASE_DIR / 'history.json'

# ---------- 配置管理 ----------
def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "download_dir": str(BASE_DIR / 'downloads'),
        "cookie_file": ""
    }

def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def load_history():
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_history(history):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

# ---------- 下载任务管理 ----------
download_tasks = {}  # task_id -> {...}
task_lock = threading.Lock()

def emit_log(task_id, level, message):
    """推送日志到前端"""
    socketio.emit('download_log', {
        'task_id': task_id,
        'level': level,  # info, warning, error, debug
        'message': message,
        'time': datetime.now().strftime('%H:%M:%S'),
    })

def progress_hook(task_id):
    """yt-dlp 进度回调"""
    def hook(d):
        with task_lock:
            if task_id not in download_tasks:
                return
            task = download_tasks[task_id]
            if d['status'] == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                downloaded = d.get('downloaded_bytes', 0)
                speed = d.get('speed', 0)
                eta = d.get('eta', 0)
                percent = (downloaded / total * 100) if total > 0 else 0
                task['progress'] = round(percent, 1)
                task['speed'] = format_speed(speed)
                task['eta'] = format_eta(eta)
                task['status'] = 'downloading'

                # 下载中的片段信息
                frag_info = d.get('fragment_index')
                frag_total = d.get('fragment_count')
                if frag_info is not None and frag_total:
                    task['fragment'] = f'{frag_info}/{frag_total}'
            elif d['status'] == 'finished':
                task['progress'] = 100
                task['status'] = 'processing'
                task['speed'] = ''
                task['eta'] = ''
            socketio.emit('progress_update', task)
    return hook

def format_speed(speed):
    if not speed:
        return ''
    if speed < 1024:
        return f'{speed:.0f} B/s'
    elif speed < 1024 * 1024:
        return f'{speed/1024:.1f} KB/s'
    else:
        return f'{speed/1024/1024:.1f} MB/s'

def format_eta(eta):
    if not eta:
        return ''
    m, s = divmod(eta, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f'{h}:{m:02d}:{s:02d}'
    return f'{m:02d}:{s:02d}'

class TaskLogger:
    """将 yt-dlp 内部日志重定向到 WebSocket"""
    def __init__(self, task_id):
        self.task_id = task_id
        self.buffer = io.StringIO()
        self._log_handler = None

    def get_handler(self):
        task_id = self.task_id
        class _Handler(logging.Handler):
            def emit(self2, record):
                msg = self2.format(record)
                level = 'debug'
                if record.levelno >= logging.ERROR:
                    level = 'error'
                elif record.levelno >= logging.WARNING:
                    level = 'warning'
                elif record.levelno >= logging.INFO:
                    level = 'info'
                emit_log(task_id, level, msg)
        return _Handler()

def download_worker(task_id, url, download_dir, audio_only, cookie_file):
    """后台下载线程"""
    with task_lock:
        if task_id not in download_tasks:
            return
        task = download_tasks[task_id]
        task['status'] = 'starting'

    emit_log(task_id, 'info', f'开始解析: {url}')

    # 构建 yt-dlp 选项
    outtmpl = os.path.join(download_dir, '%(title)s.%(ext)s')

    ydl_opts = {
        'outtmpl': outtmpl,
        'progress_hooks': [progress_hook(task_id)],
        'merge_output_format': 'mp4',
        'windowsfilenames': True,
        'logger': logging.getLogger(f'yt-dlp-{task_id}'),
        'verbose': True,
    }

    if cookie_file and os.path.exists(cookie_file):
        ydl_opts['cookiefile'] = cookie_file
        emit_log(task_id, 'info', f'使用 Cookie 文件: {cookie_file}')

    if audio_only:
        ydl_opts['format'] = 'bestaudio/best'
        ydl_opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '320',
        }]
        emit_log(task_id, 'info', '下载模式: 仅音频 (MP3/320kbps)')
    else:
        # 更宽松的格式选择：最佳视频+最佳音频，自动合并为 mp4
        ydl_opts['format'] = 'bestvideo+bestaudio/best'
        emit_log(task_id, 'info', '下载模式: 视频+音频 (最佳质量)')

    # 设置 logger 捕获 yt-dlp 输出
    logger = logging.getLogger(f'yt-dlp-{task_id}')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    handler = TaskLogger(task_id).get_handler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # 阻止日志向上传播到 root logger
    logger.propagate = False

    try:
        with task_lock:
            if task_id in download_tasks:
                download_tasks[task_id]['status'] = 'downloading'

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            emit_log(task_id, 'info', '正在获取视频信息...')
            info = ydl.extract_info(url, download=True)
            title = info.get('title', '未知标题')
            ext = 'mp3' if audio_only else 'mp4'
            filename = f"{title}.{ext}"

            emit_log(task_id, 'info', f'视频标题: {title}')
            emit_log(task_id, 'info', '下载完成，正在处理文件...')

            # 找到实际下载的文件
            actual_file = os.path.join(download_dir, filename)
            if not os.path.exists(actual_file):
                for f in os.listdir(download_dir):
                    fpath = os.path.join(download_dir, f)
                    if os.path.isfile(fpath) and title in f:
                        actual_file = fpath
                        filename = f
                        break

            file_size = os.path.getsize(actual_file) if os.path.exists(actual_file) else 0

            with task_lock:
                if task_id in download_tasks:
                    task = download_tasks[task_id]
                    task['status'] = 'completed'
                    task['progress'] = 100
                    task['title'] = title
                    task['filename'] = filename
                    task['filepath'] = actual_file
                    task['size'] = file_size

            emit_log(task_id, 'info', f'文件已保存: {filename} ({format_size_str(file_size)})')

            # 保存到历史记录
            record = {
                'id': task_id,
                'url': url,
                'title': title,
                'filename': filename,
                'filepath': actual_file,
                'audio_only': audio_only,
                'size': file_size,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'completed'
            }
            history = load_history()
            history.insert(0, record)
            if len(history) > 200:
                history = history[:200]
            save_history(history)

            socketio.emit('progress_update', task)
            socketio.emit('download_complete', task)
            emit_log(task_id, 'info', '✅ 下载任务完成!')

    except Exception as e:
        error_msg = str(e)
        emit_log(task_id, 'error', f'下载失败: {error_msg}')

        # 打印完整堆栈到日志
        import traceback
        tb = traceback.format_exc()
        emit_log(task_id, 'error', tb)

        with task_lock:
            if task_id in download_tasks:
                task = download_tasks[task_id]
                task['status'] = 'failed'
                task['error'] = error_msg
        socketio.emit('progress_update', task)
    finally:
        # 清理 logger
        logger.handlers.clear()

def format_size_str(size):
    if not size:
        return '0 B'
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} TB'

# ---------- API 路由 ----------

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify(load_config())

@app.route('/api/config', methods=['POST'])
def update_config():
    data = request.json
    config = load_config()
    if 'download_dir' in data:
        config['download_dir'] = data['download_dir']
    if 'cookie_file' in data:
        config['cookie_file'] = data['cookie_file']
    save_config(config)
    return jsonify({"success": True, "config": config})

@app.route('/api/download', methods=['POST'])
def start_download():
    data = request.json
    urls = data.get('urls', [])
    audio_only = data.get('audio_only', False)

    if isinstance(urls, str):
        urls = [urls]

    if not urls:
        return jsonify({"error": "请提供下载链接"}), 400

    config = load_config()
    download_dir = config.get('download_dir', str(BASE_DIR / 'downloads'))
    cookie_file = config.get('cookie_file', '')

    os.makedirs(download_dir, exist_ok=True)

    task_ids = []
    for url in urls:
        url = url.strip()
        if not url:
            continue
        task_id = str(int(time.time() * 1000000))
        with task_lock:
            download_tasks[task_id] = {
                'id': task_id,
                'url': url,
                'title': '',
                'status': 'queued',
                'progress': 0,
                'speed': '',
                'eta': '',
                'audio_only': audio_only,
                'filename': '',
                'filepath': '',
                'error': '',
                'fragment': '',
                'logs': [],
            }
        thread = threading.Thread(
            target=download_worker,
            args=(task_id, url, download_dir, audio_only, cookie_file),
            daemon=True
        )
        thread.start()
        task_ids.append(task_id)

    return jsonify({"success": True, "task_ids": task_ids})

@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    with task_lock:
        tasks = list(download_tasks.values())
    return jsonify(tasks)

@app.route('/api/tasks/<task_id>', methods=['DELETE'])
def delete_task(task_id):
    with task_lock:
        if task_id in download_tasks:
            del download_tasks[task_id]
    return jsonify({"success": True})

@app.route('/api/history', methods=['GET'])
def get_history():
    history = load_history()
    return jsonify(history)

@app.route('/api/history', methods=['DELETE'])
def clear_history():
    save_history([])
    return jsonify({"success": True})

@app.route('/api/history/<record_id>', methods=['DELETE'])
def delete_history_record(record_id):
    history = load_history()
    history = [r for r in history if r['id'] != record_id]
    save_history(history)
    return jsonify({"success": True})

@app.route('/api/open-folder', methods=['POST'])
def open_folder():
    data = request.json
    folder = data.get('folder', '')
    if folder:
        # 规范化路径，统一分隔符（Windows 上转为 \）
        folder = os.path.normpath(folder)
        if os.path.exists(folder):
            import subprocess
            subprocess.Popen(['explorer', folder])
            return jsonify({"success": True})
        return jsonify({"error": f"文件夹不存在: {folder}"}), 400
    return jsonify({"error": "未提供文件夹路径"}), 400

# ---------- 启动 ----------
if __name__ == '__main__':
    print(f"\n{'='*50}")
    print(f"  B站下载工具")
    print(f"  访问地址: http://localhost:5001")
    print(f"{'='*50}\n")
    socketio.run(app, host='0.0.0.0', port=5001, debug=True, allow_unsafe_werkzeug=True)
