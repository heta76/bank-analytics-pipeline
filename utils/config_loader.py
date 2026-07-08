import os
import re
import yaml

import os
import re
import yaml

def load_config(config_path):
    """Загружает конфиг из указанного файла с подстановкой переменных окружения."""
    with open(config_path, "r") as f:
        raw = f.read()
    def env_replace(match):
        expr = match.group(1)
        if ":-" in expr:
            var, default = expr.split(":-", 1)
            return os.environ.get(var.strip(), default.strip())
        else:
            return os.environ.get(expr.strip(), "")
    raw = re.sub(r'\$\{([^}]+)\}', env_replace, raw)
    return yaml.safe_load(raw)