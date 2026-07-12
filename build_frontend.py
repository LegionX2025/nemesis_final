import os
import shutil
import json

dist_dir = "dist"
if os.path.exists(dist_dir):
    shutil.rmtree(dist_dir)
os.makedirs(dist_dir)

files_to_copy = [
    "landing.html",
    "tracer.html",
    "nemesis_id.html",
    "about.html",
    "admin.html",
    "api_docs.html",
    "nemesis-ui.css",
    "nemesis-ui.js",
    "logo_nemesis.jpeg",
    "report_template.html",
    "subpoena_template.html"
]

for f in os.listdir("."):
    if f.endswith(".js") and f.startswith("script_"):
        files_to_copy.append(f)
    elif f.endswith(".js") and f.startswith("nid_script_"):
        files_to_copy.append(f)

for f in files_to_copy:
    if os.path.exists(f):
        shutil.copy(f, os.path.join(dist_dir, f))

# Default index
shutil.copy("landing.html", os.path.join(dist_dir, "index.html"))

# Cloudflare Pages configuration is handled by the existing wrangler.toml file

print("Frontend built successfully in dist/")
