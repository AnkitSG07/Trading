import json
import subprocess
from pathlib import Path


def _extract_js_functions():
    template = Path("templates/copy-trading.html").read_text()

    def grab(name: str) -> str:
        start = template.find(f"function {name}")
        if start == -1:
            raise AssertionError(f"Could not find {name} in template")

        next_candidates = [
            idx
            for idx in (
                template.find("\n  function ", start + 1),
                template.find("\nfunction ", start + 1),
            )
            if idx != -1
        ]
        end = min(next_candidates) if next_candidates else len(template)
        return template[start:end].rstrip()

    function_names = [
        "getValueInsensitive",
        "getOrderField",
        "withFallback",
        "parseNumeric",
        "formatOrderStatus",
        "formatOrderTimeDisplay",
        "decorateOrderForView",
    ]
    return "\n\n".join(grab(name) for name in function_names)


def test_decorate_order_handles_placeholder_side_and_sell_quantity():
    script_body = _extract_js_functions()
    order = {
        "side": "N/A",
        "transactionType": None,
        "quantity": -50,
        "filled_qty": -50,
    }

    node_script = f"""
    {script_body}
    const order = {json.dumps(order)};
    const result = decorateOrderForView(order);
    console.log(JSON.stringify({{ side: result.side, label: result.sideLabel, className: result.typeClass }}));
    """

    completed = subprocess.run(
        ["node", "-e", node_script], capture_output=True, text=True, check=True
    )
    payload = json.loads(completed.stdout.strip())
    assert payload["side"] == "SELL"
    assert payload["label"] == "SELL"
    assert payload["className"] == "sell
