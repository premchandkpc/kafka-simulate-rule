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
export type FlowReachability = "entry" | "rule-set" | "unreachable";
export interface FlowNode { ruleSet: string; level: number; branches: FlowRuleBranch[]; reachability: FlowReachability; }

// The rule graph only proves rule-set-to-rule-set edges. External service subscriptions
// are resolved separately from the domain/service registry and must never be inferred here.
export function layoutRuleFlow(parsed: Map<string, FlowRuleBranch[]>, startRuleSet: string): FlowNode[] {
  if (!parsed.has(startRuleSet)) return [];
  const levels = new Map<string, number>([[startRuleSet, 0]]);
  const queue = [startRuleSet];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const name = queue[cursor];
    for (const branch of parsed.get(name) ?? []) {
      for (const next of branch.emits) {
        if (parsed.has(next) && !levels.has(next)) {
          levels.set(next, levels.get(name)! + 1);
          queue.push(next);
        }
      }
    }
  }
  const reached = Math.max(...levels.values());
  const nodes: FlowNode[] = [...levels.entries()].map(([ruleSet, level]) => ({
    ruleSet, level, branches: parsed.get(ruleSet) ?? [],
    reachability: level === 0 ? "entry" : "rule-set",
  }));
  for (const [ruleSet, branches] of parsed) {
    if (!levels.has(ruleSet)) nodes.push({ ruleSet, level: reached + 1, branches, reachability: "unreachable" });
  }
  return nodes.sort((a, b) => a.level - b.level || a.ruleSet.localeCompare(b.ruleSet));
}

type UnknownRecord = Record<string, unknown>;
const isRecord = (value: unknown): value is UnknownRecord => typeof value === "object" && value !== null && !Array.isArray(value);
const asRecord = (value: unknown): UnknownRecord => isRecord(value) ? value : {};
const asArray = (value: unknown): unknown[] => Array.isArray(value) ? value : [];

// Parse the supported stored revision shapes defensively. Malformed rows are ignored rather
// than crashing the whole visualization, and IDs remain stable enough to render diagnostics.
export function branchesFromRevision(revision: unknown): FlowRuleBranch[] {
  const root = asRecord(revision);
  const source = asRecord(root.source);
  const rules = Array.isArray(source.rules) ? source.rules : asArray(root.compiled);
  return rules.filter(isRecord).map((rule, index) => {
    const actions = asArray(rule.then).filter(isRecord);
    const emits = actions.flatMap(action => {
      const emitEvent = asRecord(action.emit_event);
      const emit = asRecord(action.emit);
      const candidate = emitEvent.type ?? emit.type;
      return typeof candidate === "string" && candidate.trim() ? [candidate.trim()] : [];
    });
    const id = typeof rule.id === "string" || typeof rule.id === "number" ? String(rule.id) : `rule-${index + 1}`;
    return { id, emits: [...new Set(emits)] };
  });
}
