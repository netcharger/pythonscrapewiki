"""
generate_status_report.py
==========================
Generates a beautiful HTML status dashboard for the Wikipedia extraction progress.
Run standalone OR call generate_report() from any script to auto-refresh every N records.

Output: status_report.html
"""

import os
import sys
from datetime import datetime

# Allow import from same folder
sys.path.insert(0, os.path.dirname(__file__))
from db_config import get_db


def get_stats(cur, table, key_col):
    """Return {total, found, not_found, pending, error, pct_found}"""
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE status='FOUND'")
    found = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE status='NOT_FOUND'")
    not_found = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE status='PENDING' OR status IS NULL")
    pending = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE status='ERROR'")
    error = cur.fetchone()[0]

    pct = round(100 * found / total, 1) if total else 0
    pct_done = round(100 * (found + not_found) / total, 1) if total else 0

    return {
        "total": total,
        "found": found,
        "not_found": not_found,
        "pending": pending,
        "error": error,
        "pct_found": pct,
        "pct_done": pct_done,
    }


def get_state_breakdown(cur, table, name_col):
    """Get per-state stats."""
    cur.execute(f"""
        SELECT state_name,
               COUNT(*) as total,
               SUM(status='FOUND') as found,
               SUM(status='NOT_FOUND') as not_found,
               SUM(status='PENDING' OR status IS NULL) as pending
        FROM `{table}`
        GROUP BY state_name
        ORDER BY state_name
    """)
    return cur.fetchall()


def get_recent_found(cur, table, name_col):
    """Get 10 most recently found items."""
    try:
        cur.execute(f"""
            SELECT {name_col}, state_name, wiki_url, wiki_title
            FROM `{table}`
            WHERE status='FOUND' AND wiki_url IS NOT NULL
            ORDER BY last_checked DESC
            LIMIT 10
        """)
        return cur.fetchall()
    except Exception:
        return []


def bar(found, not_found, pending, total):
    if total == 0:
        return '<div class="bar-bg"><div class="bar-found" style="width:0%"></div></div>'
    pf = round(100 * found / total, 1)
    pnf = round(100 * not_found / total, 1)
    pp = round(100 * pending / total, 1)
    return f"""<div class="bar-bg">
        <div class="bar-found"   style="width:{pf}%"  title="Found: {found}"></div>
        <div class="bar-nf"      style="width:{pnf}%" title="Not Found: {not_found}"></div>
        <div class="bar-pending" style="width:{pp}%"  title="Pending: {pending}"></div>
    </div>"""


def generate_html(stats, state_data, now):
    tables = ["wikipedia_districts", "wikipedia_subdistricts", "wikipedia_ulbs", "wikipedia_villages"]
    labels = ["Districts", "Subdistricts", "ULBs / Towns", "Villages"]
    icons  = ["🏛️", "🗺️", "🏙️", "🏘️"]

    # Summary cards
    cards_html = ""
    for t, label, icon in zip(tables, labels, icons):
        s = stats[t]
        cards_html += f"""
        <div class="card">
            <div class="card-icon">{icon}</div>
            <div class="card-label">{label}</div>
            <div class="card-total">{s['total']:,}</div>
            {bar(s['found'], s['not_found'], s['pending'], s['total'])}
            <div class="card-row">
                <span class="badge green">✓ {s['found']:,}</span>
                <span class="badge red">✗ {s['not_found']:,}</span>
                <span class="badge grey">⏳ {s['pending']:,}</span>
            </div>
            <div class="card-pct">{s['pct_found']}% matched</div>
        </div>"""

    # State breakdown table (districts + subdistricts)
    def state_table(table, rows, label):
        html = f"<h2 class='section-title'>{label} — State Breakdown</h2>"
        html += """<div class="table-wrap"><table>
            <thead><tr>
                <th>State</th><th>Total</th><th>Found</th><th>Not Found</th><th>Pending</th><th>Progress</th>
            </tr></thead><tbody>"""
        for r in rows:
            state = r[0] or "—"
            total = r[1] or 0
            found = int(r[2] or 0)
            not_found = int(r[3] or 0)
            pending = int(r[4] or 0)
            pct = round(100 * found / total, 1) if total else 0
            html += f"""<tr>
                <td>{state}</td>
                <td>{total:,}</td>
                <td class="green">{found:,}</td>
                <td class="red">{not_found:,}</td>
                <td class="grey">{pending:,}</td>
                <td>{bar(found, not_found, pending, total)} <small>{pct}%</small></td>
            </tr>"""
        html += "</tbody></table></div>"
        return html

    state_html = ""
    for t, label, rows in state_data:
        state_html += state_table(t, rows, label)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Wikipedia Extraction Status</title>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3a;
    --green: #22c55e;
    --red: #ef4444;
    --yellow: #f59e0b;
    --blue: #3b82f6;
    --grey: #64748b;
    --text: #e2e8f0;
    --muted: #94a3b8;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'Segoe UI', system-ui, sans-serif;
    padding: 24px;
    min-height: 100vh;
  }}
  header {{
    text-align: center;
    margin-bottom: 32px;
  }}
  header h1 {{ font-size: 2rem; font-weight: 700; }}
  header p  {{ color: var(--muted); margin-top: 6px; font-size: 0.9rem; }}

  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 20px;
    margin-bottom: 40px;
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 22px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }}
  .card-icon  {{ font-size: 2rem; }}
  .card-label {{ color: var(--muted); font-size: 0.85rem; text-transform: uppercase; letter-spacing: .05em; }}
  .card-total {{ font-size: 2.2rem; font-weight: 700; }}
  .card-pct   {{ font-size: 0.85rem; color: var(--green); font-weight: 600; }}
  .card-row   {{ display: flex; gap: 8px; flex-wrap: wrap; }}

  .badge {{
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 600;
  }}
  .badge.green {{ background: rgba(34,197,94,.15); color: var(--green); }}
  .badge.red   {{ background: rgba(239,68,68,.15);  color: var(--red);   }}
  .badge.grey  {{ background: rgba(100,116,139,.15);color: var(--grey);  }}

  .bar-bg {{
    height: 8px;
    background: var(--border);
    border-radius: 999px;
    overflow: hidden;
    display: flex;
  }}
  .bar-found   {{ background: var(--green);  height: 100%; transition: width .6s; }}
  .bar-nf      {{ background: var(--red);    height: 100%; }}
  .bar-pending {{ background: var(--grey);   height: 100%; }}

  .section-title {{
    font-size: 1.2rem;
    font-weight: 600;
    margin: 32px 0 14px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }}
  .table-wrap {{
    overflow-x: auto;
    margin-bottom: 32px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.87rem;
  }}
  th, td {{
    padding: 10px 14px;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }}
  th   {{ color: var(--muted); font-weight: 600; background: var(--card); }}
  tr:hover td {{ background: rgba(255,255,255,.03); }}
  .green {{ color: var(--green); }}
  .red   {{ color: var(--red);   }}
  .grey  {{ color: var(--grey);  }}

  footer {{
    text-align: center;
    color: var(--muted);
    font-size: 0.8rem;
    margin-top: 40px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
  }}
</style>
</head>
<body>

<header>
  <h1>🇮🇳 Wikipedia Extraction — Status Dashboard</h1>
  <p>Generated: {now} &nbsp;|&nbsp; Database: census_india_2011</p>
</header>

<div class="cards">
{cards_html}
</div>

{state_html}

<footer>
  Auto-generated by generate_status_report.py &mdash; run again to refresh.
</footer>

</body>
</html>"""


def generate_report():
    """Generate the HTML status report. Call this from extraction scripts every N records."""
    conn = get_db()
    cur  = conn.cursor()

    tables = {
        "wikipedia_districts":    "district_name",
        "wikipedia_subdistricts": "subdistrict_name",
        "wikipedia_ulbs":         "ulb_name",
        "wikipedia_villages":     "village_name",
    }

    stats = {}
    for table, col in tables.items():
        try:
            stats[table] = get_stats(cur, table, col)
        except Exception as e:
            print(f"  [WARN] Could not query {table}: {e}")
            stats[table] = {"total":0,"found":0,"not_found":0,"pending":0,"error":0,"pct_found":0,"pct_done":0}

    state_data = []
    for table, label in [
        ("wikipedia_districts",    "Districts"),
        ("wikipedia_subdistricts", "Subdistricts"),
        ("wikipedia_ulbs",         "ULBs / Towns"),
    ]:
        try:
            rows = get_state_breakdown(cur, table, tables[table])
            state_data.append((table, label, rows))
        except Exception as e:
            state_data.append((table, label, []))

    cur.close()
    conn.close()

    now = datetime.now().strftime("%d %b %Y, %I:%M %p")
    html = generate_html(stats, state_data, now)

    out = os.path.join(os.path.dirname(__file__), "status_report.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  📊 Status report updated: {out}")
    return stats


def main():
    print("Generating Wikipedia Extraction Status Report...")
    stats = generate_report()
    for t, s in stats.items():
        print(f"  {t}: {s['found']}/{s['total']} ({s['pct_found']}%)")
    print("\n✅ Done! Open status_report.html in your browser.")


if __name__ == "__main__":
    main()
