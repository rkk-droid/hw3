"""HTML-отчёт data quality рядом с каталогом моделей."""

import html
import logging
from datetime import datetime, timezone

from pipeline.settings import DQ_REPORTS_DIR

logger = logging.getLogger(__name__)


def _render(verdict: dict) -> str:
    rows_html = []
    for rule in verdict["rules"]:
        status = "PASS" if rule["passed"] else "FAIL"
        css = "pass" if rule["passed"] else "fail"
        rows_html.append(
            "<tr>"
            f"<td class='{css}'>{status}</td>"
            f"<td><code>{html.escape(rule['rule_id'])}</code></td>"
            f"<td>{html.escape(rule['title'])}</td>"
            f"<td>{html.escape(rule['observed'])}</td>"
            f"<td>{html.escape(rule['threshold'])}</td>"
            "</tr>"
        )
    failed = len(verdict["failed_rules"])
    total = len(verdict["rules"])
    summary_css = "pass" if failed == 0 else "fail"
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Data Quality labeled_transactions</title>
  <style>
    body {{ font-family: Georgia, serif; margin: 32px; color: #1c1c1c; background: #f7f4ef; }}
    h1 {{ font-weight: normal; }}
    table {{ border-collapse: collapse; width: 100%; background: white; }}
    th, td {{ border: 1px solid #d9d3c7; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ background: #efeae2; }}
    .pass {{ color: #1b7f3a; font-weight: bold; }}
    .fail {{ color: #a32020; font-weight: bold; }}
    code {{ font-family: ui-monospace, monospace; }}
  </style>
</head>
<body>
  <h1>Data Quality: labeled_transactions</h1>
  <p>Сформирован {html.escape(verdict['generated_at'])}. Строк в витрине: {verdict['rows']}.</p>
  <p class="{summary_css}">Правил: {total}. Провалено: {failed}.</p>
  <table>
    <thead>
      <tr><th>Статус</th><th>Правило</th><th>Что проверяем</th><th>Наблюдение</th><th>Порог</th></tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</body>
</html>
"""


def publish_quality_report(verdict: dict) -> str:
    """Пишет HTML-отчёт в каталог рядом с моделями.

    Читает вердикт evaluate_quality_rules. Пишет dq_reports/latest.html и
    копию с меткой времени. Витрину и каталог моделей не меняет. Повтор
    перезаписывает latest.html и добавляет новый файл, старые отчёты не удаляет.
    Возвращает путь к latest.html.
    """
    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    document = _render(verdict)
    latest = DQ_REPORTS_DIR / "latest.html"
    stamped = DQ_REPORTS_DIR / f"dq_report_{stamp}.html"
    latest.write_text(document, encoding="utf-8")
    stamped.write_text(document, encoding="utf-8")
    logger.info("DQ-отчёт записан: %s и %s", latest, stamped)
    return str(latest)
