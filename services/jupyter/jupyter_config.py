# ═══════════════════════════════════════════════════════════════
# DISSMODEL PLATFORM - Jupyter Configuration
# ═══════════════════════════════════════════════════════════════

c = get_config()

# Server settings
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_remote_access = True

# Security (configure password in production)
c.ServerApp.token = ''
c.ServerApp.password = ''

# File browser
c.ContentsManager.allow_hidden = True

# Terminal
c.TerminalManager.terminate_signal = 'SIGINT'

# Resource limits
c.ServerApp.max_body_size = 536870912  # 512 MB

# Logging
c.Application.log_level = 'INFO'