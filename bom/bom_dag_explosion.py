# File: bom/bom_dag_explosion.py
"""
Cycle-tolerant, DAG-based multi-level BOM explosion (fast version).

• Builds the entire BOM graph once (vectorised, deduped)
• Breaks cycles by snipping the *lowest-qty* edge in each loop (logged)
• Explodes only those parents whose purchase_output contains 'Output'
• Returns a flat DataFrame:  order | parent_item | level | parent_index |
                              component_item | qty_per | total_qty
"""

from __future__ import annotations

import os
import sys
from functools import lru_cache
from typing import Dict, List

import pandas as pd
import networkx as nx

# ─── Project helpers ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bom.bom_data       import get_all_bom_data     # production_bom_no, component_no, total
from item.item_data     import get_all_item_data    # item_no, purchase_output …
from utils.config_utils import configure_logging, set_pandas_display_options

logger = configure_logging()

# ──────────────────────────────────────────────────────────────────────────────
# 1. FAST, DEDUPED GRAPH BUILD
# ──────────────────────────────────────────────────────────────────────────────
def build_bom_graph(bom_df: pd.DataFrame, tolerate_cycles: bool = True) -> nx.DiGraph:
    """
    Build a directed BOM graph from **deduped** bom_df.

    If cycles exist and `tolerate_cycles` is True, removes the single edge with the
    smallest qty_per in each cycle and logs removals.  Raises ValueError otherwise.
    """
    # ── 1A.  Keep only positive-qty rows and SUM duplicates
    edges = (
        bom_df.loc[bom_df["total"] > 0,
                   ["production_bom_no", "component_no", "total"]]
                .groupby(["production_bom_no", "component_no"], as_index=False)
                .total.sum()
                .rename(columns={"production_bom_no": "parent",
                                 "component_no": "child",
                                 "total": "qty_per"})
    )

    # ── 1B.  Vectorised build
    g: nx.DiGraph = nx.from_pandas_edgelist(
        edges,
        source="parent",
        target="child",
        edge_attr="qty_per",
        create_using=nx.DiGraph,
    )

    if nx.is_directed_acyclic_graph(g):
        logger.info("BOM graph: %d nodes, %d edges (acyclic).",
                    g.number_of_nodes(), g.number_of_edges())
        return g

    # ── 1C.  Cycle handling
    cycles = list(nx.simple_cycles(g))
    msg = f"Detected {len(cycles)} BOM cycle(s)"
    if not tolerate_cycles:
        raise ValueError(f"{msg}: {cycles}")

    removed_edges: list[tuple[str, str]] = []
    for cyc in cycles:
        # Build (tail, head) list for this cycle
        arc_pairs = list(zip(cyc, cyc[1:] + cyc[:1]))
        # Select the arc with the smallest qty_per (least impact)
        tail, head = min(arc_pairs, key=lambda t: g[t[0]][t[1]]["qty_per"])
        g.remove_edge(tail, head)
        removed_edges.append((tail, head))

    # Verify cleanup
    if not nx.is_directed_acyclic_graph(g):
        raise ValueError("Cycle removal failed — graph still cyclic.")

    # Persist removed arcs for data-governance
    if removed_edges:
        pd.DataFrame(removed_edges, columns=["tail", "head"]).to_csv(
            "bom_cycle_edges_removed.csv", index=False)
        logger.warning(
            "%s.  Removed %d edge(s) (logged to bom_cycle_edges_removed.csv).",
            msg, len(removed_edges)
        )

    logger.info("Cycle-free BOM graph: %d nodes, %d edges.",
                g.number_of_nodes(), g.number_of_edges())
    return g


# ──────────────────────────────────────────────────────────────────────────────
# 2.  EXPLOSION UTILITIES (DESCENDANT-CACHED)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=None)
def successors_cached(g: nx.DiGraph, node: str) -> List[str]:
    """Tiny helper with LRU-cache to avoid repeated successor look-ups."""
    return list(g.successors(node))


def explode_parent(g: nx.DiGraph, root: str, root_qty: float = 1.0) -> List[Dict]:
    """
    Iterative depth-first explosion of a single top-level parent.

    Returns list[dict] with keys:
        parent_item | level | parent_index | component_item | qty_per | total_qty
    """
    records: List[Dict] = []
    stack: list[tuple[str, float, int, str]] = [(root, root_qty, 0, root)]

    while stack:
        node, req_qty, level, p_index = stack.pop()
        kids = successors_cached(g, node)

        # Root leaf (FG with no components) – skip; not useful downstream
        if not kids:
            # qty_per is 1.0 only for self-leaf; otherwise real edge qty
            edge_qty = 1.0 if node == p_index else g[p_index][node]["qty_per"]
            records.append({
                "parent_item":    root,
                "level":          level,
                "parent_index":   p_index,
                "component_item": node,
                "qty_per":        edge_qty,
                "total_qty":      req_qty,
            })
            continue

        for child in kids:
            edge_qty = g[node][child]["qty_per"]
            stack.append((child, req_qty * edge_qty, level + 1, node))

    return records


# ──────────────────────────────────────────────────────────────────────────────
# 3.  EXPLODE “OUTPUT” PARENTS ONLY
# ──────────────────────────────────────────────────────────────────────────────
def explode_output_boms(bom_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    g = build_bom_graph(bom_df, tolerate_cycles=True)

    output_parents = (
        item_df.loc[item_df.purchase_output.str.contains("Output", na=False), "item_no"]
               .unique()
               .tolist()
    )

    logger.info("Exploding %d top-level parents flagged as Output …", len(output_parents))

    all_rows: List[Dict] = []
    for parent in output_parents:
        if parent in g:                         # ignore orphan codes
            all_rows.extend(explode_parent(g, parent, 1.0))

    if not all_rows:
        cols = ["order", "parent_item", "level",
                "parent_index", "component_item", "qty_per", "total_qty"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(all_rows)
    df.insert(0, "order", range(1, len(df) + 1))
    return (df
            .sort_values(["parent_item", "level", "component_item"])
            .reset_index(drop=True))


# ──────────────────────────────────────────────────────────────────────────────
# 4.  MAIN (stand-alone use)
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    set_pandas_display_options()

    bom_df  = get_all_bom_data()
    item_df = get_all_item_data()

    if bom_df is None or item_df is None:
        logger.error("Missing BOM or Item data — aborting.")
        sys.exit(1)

    exploded = explode_output_boms(bom_df, item_df)

    print("\nExploded BOM preview:")
    print(exploded.head(15).to_string(index=False))
    print("\nRows:", len(exploded), " | Columns:", list(exploded.columns))

    out_path = os.path.join(os.path.dirname(__file__), "exploded_bom_output.parquet")
    exploded.to_parquet(out_path, index=False)
    logger.info("Exploded BOM saved → %s", out_path)
