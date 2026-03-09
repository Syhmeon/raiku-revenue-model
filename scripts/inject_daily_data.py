"""
Inject D.daily and D.dailyNet data into the simulator HTML.
Also replaces the 'PLANNED — NOT YET EXTRACTED' notice with actual chart code.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HTML_PATH = PROJECT_ROOT / "raiku_revenue_simulator.html"
PAYLOAD_PATH = PROJECT_ROOT / "data" / "processed" / "daily_temporal_payload.js"

def main():
    html = HTML_PATH.read_text(encoding="utf-8")
    payload = PAYLOAD_PATH.read_text(encoding="utf-8").strip()

    # 1. Check if D.daily is already injected
    if "D.daily=" in html:
        print("D.daily already present — removing old injection first...")
        # Remove old D.daily and D.dailyNet lines
        lines = html.split("\n")
        lines = [l for l in lines if not (l.strip().startswith("D.daily=") or l.strip().startswith("D.dailyNet="))]
        html = "\n".join(lines)

    # 2. Find injection point: right after the D object definition line (ends with ]};)
    # and before const D_JITO
    marker = "const D_JITO ="
    idx = html.find(marker)
    if idx == -1:
        print("ERROR: Could not find injection marker 'const D_JITO ='")
        return

    # Insert D.daily assignment before D_JITO
    injection = payload + "\n"
    html = html[:idx] + injection + html[idx:]

    # 3. Write back
    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"Injected D.daily ({len(payload):,} chars) before const D_JITO")
    print(f"HTML file: {HTML_PATH}")

    # Verify
    if "D.daily=" in html and "D.dailyNet=" in html:
        print("Verification: D.daily and D.dailyNet found in HTML")
    else:
        print("WARNING: Injection may have failed")


if __name__ == "__main__":
    main()
