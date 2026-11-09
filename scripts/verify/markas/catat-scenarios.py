"""Writes the Catat and global-surface scenarios into scenarios-app.json (apply-markas-catat D24). Local tooling only.

Each scenario opens Catat the way the owner does (N, the bell, the phone tab) and fills fields through the page, so
the same steps run in Indonesian and English: a step picks its text by the document language.
"""
import json
from pathlib import Path

HERE = Path(__file__).parent
# Helpers kept on window for the later steps of the same capture; they set a field the way React expects.
PRELUDE = r"""(() => {
  const dlg = () => document.querySelector('dialog[open]');
  const en = () => document.documentElement.lang === 'en';
  const setValue = (el, value) => {
    const proto = el instanceof HTMLSelectElement ? HTMLSelectElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, 'value').set.call(el, value);
    el.dispatchEvent(new Event(el instanceof HTMLSelectElement ? 'change' : 'input', { bubbles: true }));
  };
  const optionByText = (select, re) => [...select.options].find((o) => re.test(o.textContent));
  window.__ct = {
    type: (id, en_) => { const f = dlg().querySelector('[data-capture-field]'); setValue(f, en() ? en_ : id); return 'typed'; },
    amount: (value) => { setValue(dlg().querySelector('#capture-amount'), value); return 'amount ' + value; },
    pick: (selector, pattern) => { const s = dlg().querySelector(selector); const o = s && optionByText(s, new RegExp(pattern)); if (!o) return 'NOT FOUND'; setValue(s, o.value); return 'picked ' + o.textContent.slice(0, 30); },
    radio: (pattern) => { const r = [...dlg().querySelectorAll('[role=radio]')].find((b) => new RegExp(pattern).test(b.textContent)); if (!r) return 'NOT FOUND'; r.click(); return 'radio ' + r.textContent; },
    open: () => { const d = dlg().querySelector('details'); if (!d) return 'NOT FOUND'; d.open = true; d.dispatchEvent(new Event('toggle')); return 'details'; },
    value: (selector, value) => { setValue(dlg().querySelector(selector), value); return 'set ' + selector; },
    scroll: (selector) => { const e = dlg().querySelector(selector); if (!e) return 'NOT FOUND'; e.scrollIntoView({ block: 'center' }); return 'scrolled'; },
  };
  return 'prelude';
})()"""

N = {"key": ["n", "KeyN", 78, 0, "n"]}
P = {"eval": PRELUDE, "after": 200}
ISI = {"click": "^(Isi manual|Fill in manually)$"}
def ev(code, after=700):
    return {"eval": code, "after": after}

SCENARIOS = [
    ("catat-tulis", "/", [N]),
    ("catat-tulis-terbaca", "/", [N, P, ev("__ct.type('makan siang 35rb', 'lunch 35k')", 1500)]),
    ("catat-tulis-tolak", "/", [N, P, ev("__ct.type('bayar parkir', 'parking fee')", 1500)]),
    ("catat-tulis-kredit", "/", [N, P, ev("__ct.type('sepatu 120rb pakai spaylater', 'shoes 120k spaylater')", 1500)]),
    ("catat-manual", "/", [N, P, ev("__ct.type('makan siang 35rb', 'lunch 35k')", 1500), ISI, ev("__ct.pick('#capture-category', 'Makan')")]),
    ("catat-manual-detail", "/", [N, ISI, P, ev("__ct.amount('25000+15000')"), ev("__ct.open()"), ev("__ct.pick('#capture-goal', 'Dana Darurat|Emergency')"), ev("__ct.scroll('#capture-goal')")]),
    ("catat-pemasukan", "/", [N, ISI, P, ev("__ct.radio('^(Pemasukan|Income)$')"), ev("__ct.amount('150000')"), ev("__ct.pick('#capture-category', 'Patungan')"), ev("__ct.scroll('#capture-offset')")]),
    ("catat-perpindahan", "/", [N, ISI, P, ev("__ct.radio('^(Perpindahan|Transfer)$')"), ev("__ct.amount('200000')")]),
    ("catat-kredit", "/", [N, ISI, P, ev("__ct.amount('1000000')"), ev("__ct.pick('#capture-account', '^SPayLater')"), ev("__ct.radio('^3x$')"), ev("__ct.scroll('.capture-credit')")]),
    ("catat-kredit-lainnya", "/", [N, ISI, P, ev("__ct.amount('900000')"), ev("__ct.pick('#capture-account', '^SPayLater')"), ev("__ct.radio('^(Lainnya|Other)$')"), ev("__ct.value('#capture-credit-count', '18')"), ev("__ct.scroll('.capture-credit')")]),
    ("catat-kredit-lewat", "/", [N, ISI, P, ev("__ct.amount('2500000')"), ev("__ct.pick('#capture-account', '^SPayLater')"), ev("__ct.scroll('.capture-credit')")]),
    ("catat-kredit-tanpa-tanggal", "/", [N, ISI, P, ev("__ct.amount('50000')"), ev("__ct.pick('#capture-account', '^Kredivo')"), ev("__ct.scroll('.capture-credit')")]),
    ("catat-perpindahan-kredit", "/", [N, ISI, P, ev("__ct.radio('^(Perpindahan|Transfer)$')"), ev("__ct.pick('#capture-target', '^SPayLater')"),
        {"click": "^(Bayar sisa tagihan|Pay the amount left)"}, ev("__ct.value('#capture-fee', '2.500')")]),
    ("catat-perpindahan-tanpa-tagihan", "/", [N, ISI, P, ev("__ct.radio('^(Perpindahan|Transfer)$')"), ev("__ct.pick('#capture-target', '^Kredivo')")]),
    # A key press first gives the page focus, which a script click does not, so the focused field's style can be read.
    ("catat-dari-bel", "/", [{"key": ["Shift", "ShiftLeft", 16]}, {"click": "^(Notifikasi|Notifications)"}, {"click": "Kartu Kredit Contoh"}]),
    ("catat-galat-sama", "/", [N, ISI, P, ev("__ct.radio('^(Perpindahan|Transfer)$')"), ev("__ct.pick('#capture-source', '^Jago')"), ev("__ct.pick('#capture-target', '^Kantong Utama')"),
        ev("__ct.amount('1000')"), {"click": "^(Pindahkan saldo|Move balance)$", "after": 1500}]),
    ("catat-galat-nominal", "/", [N, ISI, P, ev("(() => { document.querySelector('dialog[open] form.capture-manual').requestSubmit(); return 'submitted'; })()")]),
    ("catat-sembunyi", "/", [N, ISI, P, ev("__ct.amount('1000000')"), ev("__ct.pick('#capture-account', '^SPayLater')"), ev("__ct.radio('^3x$')")]),
    # Stress seed: a 60-character pocket on both sides and as "Bayar dari", and Rp 1.234.567.890 in the amount.
    ("catat-panjang", "/", [N, ISI, P, ev("__ct.amount('1234567890')"), ev("__ct.pick('#capture-account', '^Kantong Tabungan Pendidikan Anak dan Keperluan Sekolah')")]),
    ("catat-panjang-perpindahan", "/", [N, ISI, P, ev("__ct.radio('^(Perpindahan|Transfer)$')"), ev("__ct.amount('1234567890')"),
        ev("__ct.pick('#capture-source', '^Kantong Tabungan Pendidikan Anak dan Keperluan Sekolah')"), ev("__ct.pick('#capture-target', '^Kantong Tabungan Pendidikan Anak dan Keperluan Sekol 02')")]),
    ("glob-notifikasi", "/", [{"click": "^(Notifikasi|Notifications)"}]),
    ("glob-tema", "/", [{"click": "^(Tema|Theme)"}]),
    ("glob-tema-ponsel", "/", [{"click": "^(Lainnya|More)$"}, {"click": "^(Tema|Theme)"}]),
    ("glob-akun", "/", [{"click": "^(Akun|Account)"}]),
    ("glob-lainnya", "/", [{"click": "^(Lainnya|More)$"}]),
    ("glob-bantuan", "/", [{"key": ["?", "Slash", 191, 8, "?"]}]),
    ("glob-rencana-jatuh-tempo", "/", [{"click": "^(Notifikasi|Notifications)"}, {"click": "Netflix|Rencana|plan"}]),
    ("glob-toast", "/", [N, P, ev("__ct.type('parkir 5rb', 'parking 5k')", 1500), {"key": ["Enter", "Enter", 13], "after": 1500}]),
]

PROTOTYPE = [
    ("catat-tulis", "catat"), ("catat-manual", "catat-manual"), ("catat-pemasukan", "catat-pemasukan"),
    ("catat-perpindahan", "catat-perpindahan"), ("catat-perpindahan-kredit", "catat-perpindahan-kredit"),
    ("glob-notifikasi", "notifikasi"), ("glob-tema", "tema"), ("glob-akun", "akun"), ("glob-lainnya", "lainnya"), ("glob-bantuan", "bantuan"),
]


def main() -> None:
    path = HERE / "scenarios-app.json"
    scenarios = [s for s in json.loads(path.read_text()) if not s["name"].startswith(("catat-", "glob-"))]
    for name, route, steps in SCENARIOS:
        scenario = {"name": name, "path": route, "catat": True, "steps": steps}
        if name in ("glob-tema", "glob-akun"):
            scenario["cells"] = [f"{width}-{theme}" for width in (1440, 1024) for theme in ("light", "dark")]
        if name in ("glob-lainnya", "glob-tema-ponsel"):
            scenario["cells"] = [f"{width}-{theme}" for width in (390, 320) for theme in ("light", "dark")]
        if name == "catat-sembunyi":
            scenario["storage"] = {"hideBalances": "1"}
        scenarios.append(scenario)
    path.write_text(json.dumps(scenarios, indent=1, ensure_ascii=False) + "\n")
    path = HERE / "scenarios-prototype.json"
    prototype = [s for s in json.loads(path.read_text()) if not s["name"].startswith(("catat-", "glob-"))]
    prototype += [{"name": name, "path": f"/beranda?d={dialog}", "catat": True, "steps": [{"wait": 400}]} for name, dialog in PROTOTYPE]
    path.write_text(json.dumps(prototype, indent=1, ensure_ascii=False) + "\n")
    print(f"{len(SCENARIOS)} app and {len(PROTOTYPE)} prototype Catat scenarios written")

if __name__ == "__main__":
    main()
