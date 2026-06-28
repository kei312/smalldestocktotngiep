#!/usr/bin/env python3
"""
scripts/generate_figures.py - Automatically render Mermaid diagrams to PNG files using Kroki API.

Usage:
    python3 scripts/generate_figures.py
"""

import os
import re
import urllib.request
import urllib.error

# Project paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MD_FILE = os.path.join(PROJECT_ROOT, "docs", "MERMAID_DIAGRAMS.md")
IMAGE_DIR = os.path.join(PROJECT_ROOT, "docs", "images")

def parse_mermaid_diagrams(filepath):
    """Parses MERMAID_DIAGRAMS.md and extracts figure names and their raw Mermaid code."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found.")
        return {}

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Match sections like:
    # ## Hình X.Y — Title
    # ```mermaid
    # ...code...
    # ```
    pattern = r"##\s+Hình\s+(\d+\.\d+)[^\n]*\n+```mermaid\n(.*?)\n```"
    matches = re.findall(pattern, content, re.DOTALL)

    diagrams = {}
    for fig_num, code in matches:
        filename = f"fig_{fig_num.replace('.', '_')}.png"
        diagrams[filename] = code.strip()
    
    return diagrams

def render_diagram(mermaid_code, output_path):
    """Sends raw Mermaid code to Kroki API and saves the returned PNG image."""
    url = "https://kroki.io/mermaid/png"
    req = urllib.request.Request(
        url,
        data=mermaid_code.encode("utf-8"),
        headers={
            "Content-Type": "text/plain",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    )
    try:
        with urllib.request.urlopen(req) as response:
            with open(output_path, "wb") as f:
                f.write(response.read())
        print(f"✓ Generated: {os.path.basename(output_path)}")
        return True
    except urllib.error.URLError as e:
        print(f"✗ Failed to render to {os.path.basename(output_path)}: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error for {os.path.basename(output_path)}: {e}")
        return False

def main():
    print("Starting thesis figure generation...")
    
    # Create output directory
    os.makedirs(IMAGE_DIR, exist_ok=True)
    
    diagrams = parse_mermaid_diagrams(MD_FILE)
    if not diagrams:
        print("No diagrams found to parse. Make sure MERMAID_DIAGRAMS.md is correctly formatted.")
        return

    print(f"Found {len(diagrams)} diagrams in {os.path.basename(MD_FILE)}:")
    for filename in diagrams:
        print(f" - {filename}")

    success_count = 0
    for filename, code in diagrams.items():
        output_path = os.path.join(IMAGE_DIR, filename)
        if render_diagram(code, output_path):
            success_count += 1

    print(f"\nCompleted! Successfully generated {success_count}/{len(diagrams)} diagrams under docs/images/")

if __name__ == "__main__":
    main()
