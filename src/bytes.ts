export function parseByteSize(input: string | number | undefined): number | undefined {
  if (input === undefined) return undefined;
  if (typeof input === 'number') return input;

  const match = /^(\d+(?:\.\d+)?)\s*([KMGTP]?(?:i?B)?)?$/i.exec(input.trim());
  if (!match) {
    const direct = Number(input);
    return Number.isFinite(direct) ? direct : undefined;
  }

  const value = Number(match[1]);
  const unit = (match[2] || 'B').toUpperCase();
  if (unit === 'B') return Math.floor(value);

  const prefix = unit[0]!;
  const exponent = { K: 1, M: 2, G: 3, T: 4, P: 5 }[prefix as 'K'] ?? 0;
  const binary = unit.includes('I');
  const base = binary ? 1024 : 1000;
  return Math.floor(value * base ** exponent);
}
