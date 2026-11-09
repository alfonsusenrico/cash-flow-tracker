import { stripGrouping } from "@/lib/utils";

/** Whether the amount field holds an expression (`+ − * /`) rather than a plain amount. */
export function isExpression(text: string): boolean {
  return /[+\-*/]/.test(text);
}

/**
 * The whole-rupiah value of the amount field: digits with the region's grouping, or an expression of `+ - * /`
 * evaluated with the usual precedence and without `eval` (the page's CSP forbids it). Division rounds down; a
 * result that is not positive reads 0.
 */
export function parseAmountExpression(text: string): number {
  const sanitized = stripGrouping(text).trim();
  if (!sanitized) return 0;
  if (/^[0-9]+$/.test(sanitized)) return parseInt(sanitized, 10) || 0;
  const tokens = sanitized.match(/([0-9]+|[+\-*/])/g);
  if (!tokens) return 0;

  const terms: (number | string)[] = [];
  let index = 0;
  while (index < tokens.length) {
    const token = tokens[index];
    if (token === "*" || token === "/") {
      const previous = terms.pop();
      const next = tokens[index + 1];
      if (typeof previous !== "number" || !next || Number.isNaN(Number(next))) return 0;
      const value = Number(next);
      terms.push(token === "*" ? previous * value : value !== 0 ? Math.floor(previous / value) : 0);
      index += 2;
    } else {
      terms.push(Number.isNaN(Number(token)) ? token : Number(token));
      index += 1;
    }
  }

  let total = typeof terms[0] === "number" ? terms[0] : 0;
  let operator = "+";
  for (const item of terms.slice(1)) {
    if (item === "+" || item === "-") operator = item;
    else if (typeof item === "number") total = operator === "+" ? total + item : total - item;
  }
  return Number.isFinite(total) && total > 0 ? Math.round(total) : 0;
}
