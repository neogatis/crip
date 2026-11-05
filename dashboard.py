"""Password-protected read-only spread dashboard sharing scanner's SQLite volume."""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import time
from pathlib import Path

from aiohttp import web

LOG = logging.getLogger('sarb.dashboard')
COOKIE = 'sarb_session'

def _password():
    return os.environ.get('DASHBOARD_PASSWORD', '')

def _sign(expires):
    key = _password().encode('utf-8')
    return hmac.new(key, str(expires).encode('ascii'), hashlib.sha256).hexdigest()

def _authenticated(req):
    password = _password()
    if not password:
        return False
    value = req.cookies.get(COOKIE,'')
    try:
        expires_str, signature = value.split(':', 1)
        expires = int(expires_str)
    except (ValueError, TypeError):
        return False
    return expires > int(time.time()) and hmac.compare_digest(_sign(expires), signature)

def _headers(response):
    response.headers['Cache-Control'] = 'no-store, max-age=0'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['Referrer-Policy'] = 'no-referrer'
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; img-src 'self' data:; connect-src 'self'; "
        "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
        "frame-ancestors 'none'; form-action 'self'; base-uri 'none'")
    return response

def _login_page(error=False):
    error_html = '<p class="err">Пароль неверный</p>' if error else ''
    return """<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>S-Arbitrage · Доступ</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#08111e;color:#eaf1ff;font:16px system-ui,-apple-system,Segoe UI,sans-serif;min-height:100vh;display:grid;place-items:center}
main{width:min(420px,calc(100% - 28px));padding:36px;background:#101e30;border:1px solid #26384e;border-radius:20px;box-shadow:0 32px 95px #0208109c}
.k{font-size:12px;letter-spacing:.21em;color:#6cc7de;font-weight:750}.logo{font-size:25px;font-weight:850;margin:8px 0 25px}
h1{font-size:26px;margin:0 0 8px}p{color:#93a8bc;line-height:1.5;margin:0 0 28px}
label{font-size:13px;color:#bbcadb;display:block;margin-bottom:9px}input{padding:14px 15px;border:1px solid #34465b;background:#091728;color:#f1f7ff;border-radius:10px;font:inherit;width:100%;outline:0}
input:focus{border-color:#4bd5c9}button{width:100%;padding:14px;color:#061923;background:#56debf;border:0;border-radius:10px;font-weight:800;margin-top:16px;font-size:15px;cursor:pointer}
small{color:#728ca4;display:block;margin-top:25px;line-height:1.6}.err{color:#ff8d94;margin:12px 0}
</style></head><body><main><div class="k">PRIVATE RESEARCH TERMINAL</div><div class="logo">S / ARBITRAGE</div>
<h1>Войти в панель</h1><p>Мониторинг спредов между биржами. Только исследовательские данные.</p>
<form action="/login" method="post"><label for="password">Пароль панели</label>
<input id="password" name="password" type="password" autocomplete="current-password" required autofocus>
<button type="submit">Открыть мониторинг</button></form>""" + error_html + """
<small>Нет реальных ордеров, торговых API-ключей и операций с активами.</small></main></body></html>"""

async def login(req):
    if req.method == 'GET':
        return _headers(web.Response(text=_login_page(),content_type='text/html'))
    if not _password():
        return _headers(web.Response(status=503,text='Dashboard password is not configured'))
    data = await req.post()
    supplied = str(data.get('password',''))[:256]
    if not hmac.compare_digest(supplied, _password()):
        await asyncio.sleep(0.5)
        return _headers(web.Response(status=401,text=_login_page(True),content_type='text/html'))
    expires = int(time.time()) + 24*3600
    resp = web.HTTPSeeOther('/')
    resp.set_cookie(COOKIE, f'{expires}:{_sign(expires)}', max_age=24*3600,
                    httponly=True, secure=True, samesite='Strict',path='/')
    raise resp

async def logout(req):
    resp=web.HTTPSeeOther('/login')
    resp.del_cookie(COOKIE,path='/')
    raise resp

def require(req):
    if not _authenticated(req):
        raise web.HTTPSeeOther('/login') if req.path == '/' else web.HTTPUnauthorized(text='Login required')

async def home(req):
    require(req)
    return _headers(web.Response(text=Path(__file__).with_name('dashboard.html').read_text(encoding='utf-8'),
                                content_type='text/html'))

def read_reports(db_path):
    if not Path(db_path).exists():
        return []
    con=sqlite3.connect(db_path,timeout=2)
    try:
        rows=con.execute('SELECT data_json FROM spread_reports ORDER BY id DESC LIMIT 36').fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()
    reports=[]
    for (text,) in reversed(rows):
        try:
            obj=json.loads(text)
            if isinstance(obj,dict):
                reports.append(obj)
        except (json.JSONDecodeError,TypeError):
            pass
    return reports

async def api(req):
    require(req)
    db=os.environ.get('SQLITE_PATH','/data/signals.sqlite3')
    reports=await asyncio.to_thread(read_reports,db)
    latest=reports[-1] if reports else None
    output={
        'mode':'RESEARCH_ONLY',
        'live':latest,
        'history':[{
            'timestamp':r.get('timestamp'),
            'universe_count':r.get('universe_count',0),
            'evaluations':r.get('evaluations',0),
            'net_positive':r.get('net_positive',0),
            'net_above_signal_threshold':r.get('net_above_signal_threshold',0),
            'best_net':max((x.get('estimated_net_pct',-100) for x in r.get('top_estimated_net',[])),default=None),
            'books':r.get('live_books_by_venue',{}),
        } for r in reports],
    }
    return _headers(web.json_response(output))

async def health(req):
    return _headers(web.json_response({'status':'ok','service':'S-Arbitrage Research Dashboard'}))

async def run_dashboard():
    # Fail closed: never serve the research data publicly without a password.
    if not _password():
        LOG.error('DASHBOARD_PASSWORD not configured; web dashboard disabled')
        return
    app=web.Application(client_max_size=2048)
    app.router.add_route('GET','/',home)
    app.router.add_route('GET','/login',login)
    app.router.add_route('POST','/login',login)
    app.router.add_route('POST','/logout',logout)
    app.router.add_route('GET','/api/reports',api)
    app.router.add_route('GET','/health',health)
    runner=web.AppRunner(app,access_log=None)
    await runner.setup()
    port=int(os.environ.get('PORT','8080'))
    site=web.TCPSite(runner,'0.0.0.0',port)
    await site.start()
    LOG.info('Dashboard online on port %d: authenticated, read-only',port)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
