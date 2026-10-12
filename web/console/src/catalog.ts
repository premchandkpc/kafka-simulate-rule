// Example domain definitions bundled at build time. They are only seed sources:
// the console writes them to the FlowRule database through the management API,
// and the catalog page reads the stored copies back from the API.
export type CatalogKind = "contract" | "rule";
export interface CatalogItem { domain: string; kind: CatalogKind; name: string; source: string; }

const files = import.meta.glob("../../../examples/*/{contracts,rules}/*.yaml", {
  query: "?raw", import: "default", eager: true,
}) as Record<string, string>;

export const DOMAIN_LABELS: Record<string, string> = {
  ecommerce: "E-commerce", banking: "Banking", "food-delivery": "Food delivery",
};

export const CATALOG: CatalogItem[] = Object.entries(files).flatMap(([path, source]) => {
  const match = path.match(/examples\/([^/]+)\/(contracts|rules)\/(.+)\.yaml$/);
  if (!match) return [];
  return [{ domain: match[1], kind: match[2] === "contracts" ? "contract" : "rule", name: match[3], source } as CatalogItem];
}).sort((a, b) => a.domain.localeCompare(b.domain) || a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name));

export interface FlowRuleBranch { id: string; emits: string[]; }
export interface FlowNode { ruleSet: string; level: number; branches: FlowRuleBranch[]; viaService?: boolean; }

// Derives the rule-set chain for a domain from its rule YAML: a rule set that emits
// an event type named like another rule set hands the event on to that rule set;
// any other emitted type leaves the domain (a registered service or terminal event).
export function buildRuleFlow(domain: string, startRuleSet: string): FlowNode[] {
  const parsed = new Map<string, FlowRuleBranch[]>();
  for (const item of CATALOG) {
    if (item.domain !== domain || item.kind !== "rule") continue;
    const branches: FlowRuleBranch[] = [];
    let ruleSet = item.name, current: FlowRuleBranch | undefined, expectType = false;
    for (const raw of item.source.split("\n")) {
      const line = raw.trim();
      const rs = line.match(/^rule_set:\s*"?([^"\s]+)"?/);
      if (rs) ruleSet = rs[1];
      const id = line.match(/^-\s*id:\s*"?([^"\s]+)"?/);
      if (id) { current = { id: id[1], emits: [] }; branches.push(current); expectType = false; }
      if (line.startsWith("- emit_event:") || line === "emit_event:") expectType = true;
      const type = line.match(/^type:\s*"?([^"\s]+)"?/);
      if (type && expectType && current) { current.emits.push(type[1]); expectType = false; }
    }
    parsed.set(ruleSet, branches);
  }
  const levels = new Map<string, number>([[startRuleSet, 0]]);
  const queue = [startRuleSet];
  while (queue.length) {
    const name = queue.shift()!;
    for (const branch of parsed.get(name) ?? []) {
      for (const next of branch.emits) {
        if (parsed.has(next) && !levels.has(next)) { levels.set(next, levels.get(name)! + 1); queue.push(next); }
      }
    }
  }
  const reached = Math.max(...levels.values());
  const nodes: FlowNode[] = [...levels.entries()].map(([ruleSet, level]) => ({ ruleSet, level, branches: parsed.get(ruleSet) ?? [] }));
  // Rule sets no other rule set hands off to are resumed by a service replying with their event type.
  for (const [ruleSet, branches] of parsed) if (!levels.has(ruleSet)) nodes.push({ ruleSet, level: reached + 1, branches, viaService: true });
  return nodes.sort((a, b) => a.level - b.level || a.ruleSet.localeCompare(b.ruleSet));
}

export function ruleSetKnown(domain: string, ruleSet: string): boolean {
  return CATALOG.some(item => item.domain === domain && item.kind === "rule" && item.name === ruleSet);
}
