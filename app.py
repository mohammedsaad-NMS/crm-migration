# app.py  ── place at repo root (~/crm-migration/app.py)

from flask import Flask, jsonify, render_template_string, request, abort
import importlib

# ─── map url-keys to script modules ─────────────────────────────────────────
SCRIPT_MAP = {
    "load-households": "scripts.load_households",
    "load-districts":  "scripts.load_districts",
    # add more entries: "friendly-key": "scripts.your_script"
}

app = Flask(__name__)

# ─── home page with buttons ────────────────────────────────────────────────
HOME_HTML = """
<!doctype html>
<title>CRM Migration Utilities</title>
<h2>Select a script to run</h2>
<ul>
  {% for key in scripts %}
    <li>
      <form action="{{ url_for('run_script', key=key) }}" method="post" style="display:inline">
        <button type="submit">{{ key.replace('-', ' ').title() }}</button>
      </form>
    </li>
  {% endfor %}
</ul>
"""

@app.route("/")
def index():
    return render_template_string(HOME_HTML, scripts=SCRIPT_MAP.keys())

# ─── dispatcher endpoint ───────────────────────────────────────────────────
@app.route("/run/<key>", methods=["POST"])
def run_script(key):
    module_path = SCRIPT_MAP.get(key)
    if not module_path:
        abort(404, f"Unknown script '{key}'")

    mod = importlib.import_module(module_path)
    if not hasattr(mod, "main"):
        abort(500, f"'{module_path}' has no main()")

    try:
        result = mod.main()           # run the script
        return jsonify(status="success", script=key, result=result)
    except Exception as e:
        return jsonify(status="error", script=key, message=str(e)), 500

if __name__ == "__main__":
    app.run()
