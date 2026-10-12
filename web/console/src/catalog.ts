// Example domain definitions bundled at build time. They are only seed sources for "Save to database":
// the console writes them to the FlowRule database through the management API,
// and the catalog page reads the stored copies back from the API.
export type CatalogKind = "contract" | "rule";
export interface CatalogItem { domain: string; kind: CatalogKind; name: string; source: string; }

const files = import.meta.glob("../../../domains/*/{contracts,rules}/*.yaml", {
  query: "?raw", import: "default", eager: true,
}) as Record<string, string>;

export const DOMAIN_LABELS: Record<string, string> = {
  ecommerce: "E-commerce", banking: "Banking", "food-delivery": "Food delivery",
};

export const CATALOG: CatalogItem[] = Object.entries(files).flatMap(([path, source]) => {
  const match = path.match(/domains\/([^/]+)\/(contracts|rules)\/(.+)\.yaml$/);
  if (!match) return [];
  return [{ domain: match[1], kind: match[2] === "contracts" ? "contract" : "rule", name: match[3], source } as CatalogItem];
}).sort((a, b) => a.domain.localeCompare(b.domain) || a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name));

export interface FlowRuleBranch { id: string; emits: string[]; }
export interface FlowNode { ruleSet: string; level: number; branches: FlowRuleBranch[]; viaService?: boolean; }

// Lays out the rule-set chain from the rule sets stored in the FlowRule database: a rule
// set that emits an event type named like another rule set hands the event on to it; any
// other emitted type leaves the domain (a registered service or terminal event).
export function layoutRuleFlow(parsed: Map<string, FlowRuleBranch[]>, startRuleSet: string): FlowNode[] {
  if (!parsed.has(startRuleSet)) return [];
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
  // Rule sets this entry flow never emits to are not reachable from it. Whether an external service triggers them is not known from rules alone.
  for (const [ruleSet, branches] of parsed) if (!levels.has(ruleSet)) nodes.push({ ruleSet, level: reached + 1, branches, viaService: true });
  return nodes.sort((a, b) => a.level - b.level || a.ruleSet.localeCompare(b.ruleSet));
}

// Reads the branches and emitted event types out of a stored rule revision (the API's "source" document).
export function branchesFromRevision(revision: any): FlowRuleBranch[] {
  const rules: any[] = revision?.source?.rules ?? revision?.compiled ?? [];
  return rules.map(rule => ({
    id: String(rule.id),
    emits: (rule.then ?? []).flatMap((action: any) => {
      const type = action?.emit_event?.type ?? action?.emit?.type;
      return typeof type === "string" ? [type] : [];
    }),
  }));
}
