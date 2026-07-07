import os
import io
import json
import logging
import threading
import time
import uuid
import tempfile
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
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "download_dir": str(BASE_DIR / 'downloads'),
        "cookie_file": ""
    }

def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def load_history():
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return []

def save_history(history):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

# ---------- 下载任务管理 ----------
download_tasks = {}  # task_id -> {...}
task_lock = threading.Lock()
history_lock = threading.Lock()

def emit_log(task_id, level, message):
    """推送日志到前端（过滤无意义的进度行）"""
    # 过滤 yt-dlp 的 [download] 进度日志，界面已有进度条
    if isinstance(message, str) and message.strip().startswith('[download]'):
        return
    socketio.emit('download_log', {
        'task_id': task_id,
        'level': level,  # info, warning, error, debug
        'message': message,
        'time': datetime.now().strftime('%H:%M:%S'),
    })

def progress_hook(task_id, skip_on_playlist=False):
    """yt-dlp 进度回调。skip_on_playlist=True 时，如果是合集任务则跳过更新（由后台手动控制进度）"""
    def hook(d):
        with task_lock:
            if task_id not in download_tasks:
                return
            task = download_tasks[task_id]
            if skip_on_playlist and task.get('is_playlist'):
                return  # 合集模式：跳过 progress_hook，避免覆盖后台手动计算的进度
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

def _build_ydl_opts(task_id, download_dir, audio_only, cookie_file, skip_progress=False):
    """构建 yt-dlp 选项（共用）。skip_progress=True 时 progress_hook 对合集任务跳过"""
    outtmpl = os.path.join(download_dir, '%(title)s.%(ext)s')
    ydl_opts = {
        'outtmpl': outtmpl,
        'progress_hooks': [progress_hook(task_id, skip_on_playlist=skip_progress)],
        'merge_output_format': 'mp4',
        'windowsfilenames': True,
        'logger': logging.getLogger(f'yt-dlp-{task_id}'),
    }
    if cookie_file:
        if os.path.exists(cookie_file):
            ydl_opts['cookiefile'] = cookie_file
        else:
            try:
                tmp = tempfile.NamedTemporaryFile(
                    mode='w', suffix='.txt', prefix='bili_cookie_',
                    delete=False, encoding='utf-8'
                )
                tmp.write(cookie_file)
                tmp.close()
                ydl_opts['cookiefile'] = tmp.name
            except Exception as e:
                emit_log(task_id, 'warning', f'Cookie 处理失败: {e}')
    if audio_only:
        ydl_opts['format'] = 'bestaudio/best'
        ydl_opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '320',
        }]
    else:
        ydl_opts['format'] = 'bestvideo+bestaudio/best'
    return ydl_opts

def _setup_logger(task_id):
    """设置 yt-dlp logger"""
    logger = logging.getLogger(f'yt-dlp-{task_id}')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = TaskLogger(task_id).get_handler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.propagate = False
    return logger

def _finish_single_task(task_id, url, title, actual_file, audio_only):
    """单个视频下载完成后的收尾工作"""
    file_size = os.path.getsize(actual_file) if actual_file and os.path.exists(actual_file) else 0
    filename = os.path.basename(actual_file) if actual_file else ''
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
    with history_lock:
        history = load_history()
        history.insert(0, record)
        if len(history) > 200:
            history = history[:200]
        save_history(history)
    task_snapshot = dict(download_tasks.get(task_id, {})) if task_id in download_tasks else {}
    socketio.emit('progress_update', task_snapshot)
    socketio.emit('download_complete', task_snapshot)
    emit_log(task_id, 'info', '✅ 下载任务完成!')

def _find_downloaded_file(download_dir, title, ext, info):
    """根据 info 或文件系统找到实际下载的文件路径"""
    actual_file = None
    filename = f"{title}.{ext}"
    requested_downloads = info.get('requested_downloads', [])
    if requested_downloads:
        fp = requested_downloads[0].get('filepath', '')
        if fp and os.path.exists(fp):
            return fp
    # 回退：拼接文件名查找
    candidate = os.path.join(download_dir, filename)
    if os.path.exists(candidate):
        return candidate
    # 模糊匹配
    try:
        for f in os.listdir(download_dir):
            fpath = os.path.join(download_dir, f)
            if os.path.isfile(fpath) and f.startswith(title):
                return fpath
    except (FileNotFoundError, PermissionError):
        pass
    return candidate  # 返回候选路径，调用方自己判断是否存在

def download_worker(task_id, url, download_dir, audio_only, cookie_file):
    """后台下载线程（支持单视频、合集、收藏夹、播放列表）"""
    with task_lock:
        if task_id not in download_tasks:
            return
        task = download_tasks[task_id]
        task['status'] = 'starting'

    emit_log(task_id, 'info', f'开始解析: {url}')

    ydl_opts = _build_ydl_opts(task_id, download_dir, audio_only, cookie_file)
    logger = _setup_logger(task_id)

    try:
        with task_lock:
            if task_id in download_tasks:
                download_tasks[task_id]['status'] = 'downloading'

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            emit_log(task_id, 'info', '正在获取视频信息...')
            info = ydl.extract_info(url, download=False)

            # 判断是否为播放列表（合集/收藏夹/分P）
            is_playlist = info.get('_type') == 'playlist' or 'entries' in info
            entries = info.get('entries', []) if is_playlist else None

            if is_playlist and entries:
                # ========== 播放列表模式 ==========
                playlist_title = info.get('title', '未知列表')
                total = len(entries)
                emit_log(task_id, 'info', f'📂 检测到合集/收藏夹: {playlist_title}（共 {total} 个视频）')

                # 在父任务所在目录下创建子目录
                playlist_dir = os.path.join(download_dir, _safe_filename(playlist_title))
                os.makedirs(playlist_dir, exist_ok=True)

                # 为父任务设置播放列表信息
                with task_lock:
                    if task_id in download_tasks:
                        t = download_tasks[task_id]
                        t['title'] = playlist_title
                        t['is_playlist'] = True
                        t['playlist_total'] = total
                        t['playlist_completed'] = 0
                        t['playlist_progress'] = 0  # 合集整体进度
                        t['progress'] = 0            # 当前视频进度（由 progress_hook 更新）
                        t['url'] = url
                socketio.emit('progress_update', dict(download_tasks.get(task_id, {})))

                completed_count = 0
                for idx, entry in enumerate(entries):
                    if entry is None:
                        continue
                    entry_url = entry.get('webpage_url') or entry.get('url') or entry.get('id', '')
                    entry_title = entry.get('title', f'第{idx+1}个视频')

                    emit_log(task_id, 'info', f'[{idx+1}/{total}] {entry_title}')
                    # 更新当前视频信息
                    with task_lock:
                        if task_id in download_tasks:
                            t = download_tasks[task_id]
                            t['progress'] = 0
                            t['current_video'] = f'{idx+1}/{total}'
                            t['current_title'] = entry_title
                            t['status'] = 'downloading'
                    socketio.emit('progress_update', dict(download_tasks.get(task_id, {})))

                    # 当前视频使用带 skip_progress=True 的选项，让 progress_hook 正常更新 progress
                    sub_opts = _build_ydl_opts(task_id, playlist_dir, audio_only, cookie_file)
                    sub_opts['logger'] = logging.getLogger(f'yt-dlp-{task_id}')

                    try:
                        with yt_dlp.YoutubeDL(sub_opts) as sub_ydl:
                            sub_info = sub_ydl.extract_info(entry_url, download=True)
                            ext = 'mp3' if audio_only else 'mp4'
                            actual_file = _find_downloaded_file(playlist_dir, sub_info.get('title', entry_title), ext, sub_info)
                            if os.path.exists(actual_file):
                                file_size = os.path.getsize(actual_file)
                                emit_log(task_id, 'info', f'  ✅ 完成 ({format_size_str(file_size)})')
                    except Exception as e:
                        emit_log(task_id, 'error', f'  ❌ 下载失败: {e}')

                    completed_count += 1
                    with task_lock:
                        if task_id in download_tasks:
                            t = download_tasks[task_id]
                            t['playlist_completed'] = completed_count
                            t['playlist_progress'] = round((completed_count / total) * 100, 1)
                    socketio.emit('progress_update', dict(download_tasks.get(task_id, {})))

                # 全部完成
                with task_lock:
                    if task_id in download_tasks:
                        t = download_tasks[task_id]
                        t['status'] = 'completed'
                        t['progress'] = 100
                        t['playlist_progress'] = 100
                        t['playlist_completed'] = total
                emit_log(task_id, 'info', f'✅ 合集下载完成！共 {total} 个视频，保存至: {playlist_dir}')

                # 合集只存一条历史记录
                record = {
                    'id': task_id,
                    'url': url,
                    'title': playlist_title,
                    'filename': _safe_filename(playlist_title),
                    'filepath': playlist_dir,
                    'audio_only': audio_only,
                    'size': sum(
                        os.path.getsize(os.path.join(playlist_dir, f))
                        for f in (os.listdir(playlist_dir) if os.path.exists(playlist_dir) else [])
                        if os.path.isfile(os.path.join(playlist_dir, f))
                    ),
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'completed',
                    'is_playlist': True,
                    'playlist_total': total,
                }
                with history_lock:
                    history = load_history()
                    history.insert(0, record)
                    if len(history) > 200:
                        history = history[:200]
                    save_history(history)

                socketio.emit('progress_update', dict(download_tasks.get(task_id, {})))
                socketio.emit('download_complete', dict(download_tasks.get(task_id, {})))
            else:
                # ========== 单视频模式 ==========
                info = ydl.extract_info(url, download=True)
                title = info.get('title', '未知标题')
                ext = 'mp3' if audio_only else 'mp4'
                emit_log(task_id, 'info', f'视频标题: {title}')
                emit_log(task_id, 'info', '下载完成，正在处理文件...')

                actual_file = _find_downloaded_file(download_dir, title, ext, info)
                if not os.path.exists(actual_file):
                    emit_log(task_id, 'warning', '无法确定下载的文件路径')

                _finish_single_task(task_id, url, title, actual_file, audio_only)

    except Exception as e:
        error_msg = str(e)
        emit_log(task_id, 'error', f'下载失败: {error_msg}')
        import traceback
        emit_log(task_id, 'error', traceback.format_exc())
        with task_lock:
            if task_id in download_tasks:
                task = download_tasks[task_id]
                task['status'] = 'failed'
                task['error'] = error_msg
        socketio.emit('progress_update', dict(download_tasks.get(task_id, {})))
    finally:
        logger.handlers.clear()

def _safe_filename(name):
    """清理文件名中的非法字符"""
    import re
    return re.sub(r'[\\/:*?"<>|]', '_', name)

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
        url = url.strip().strip('/')  # 去除前后斜杠
        if not url:
            continue
        # 智能补全：纯 BV号 / av号 / ep号 自动转为完整 B站链接
        if not url.startswith('http'):
            if url.upper().startswith('BV'):
                url = f'https://www.bilibili.com/video/{url}'
            elif url.lower().startswith('av'):
                url = f'https://www.bilibili.com/video/{url}'
            elif url.lower().startswith('ep'):
                url = f'https://www.bilibili.com/bangumi/play/{url}'
            elif url.lower().startswith('ss'):
                url = f'https://www.bilibili.com/bangumi/play/{url}'
            else:
                url = f'https://www.bilibili.com/video/{url}'
        task_id = str(uuid.uuid4())[:12]
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
    with history_lock:
        history = load_history()
    return jsonify(history)

@app.route('/api/history', methods=['DELETE'])
def clear_history():
    with history_lock:
        save_history([])
    return jsonify({"success": True})

@app.route('/api/history/<record_id>', methods=['DELETE'])
def delete_history_record(record_id):
    with history_lock:
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
    debug_mode = os.environ.get('FLASK_DEBUG', '0') == '1'
    socketio.run(app, host='0.0.0.0', port=5001, debug=debug_mode, allow_unsafe_werkzeug=True)
