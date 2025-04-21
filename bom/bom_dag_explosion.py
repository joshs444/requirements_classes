# File: bom/bom_dag_explosion.py
"""
DAG‑based multi‑level BOM explosion (cycle‑tolerant).

• Only explodes parents whose replenishment_system == 'Output'
• Detects BOM cycles, snips one edge per cycle, logs the removals
• Produces a flat DataFrame: order, parent_item, level,
  parent_index, component_item, qty_per, total_qty
"""

import os
import sys
from typing import Dict, List

import pandas as pd
import networkx as nx

# ─── Project helpers ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bom.bom_data    import get_all_bom_data      # loads production_bom_no, component_no, total
from item.item_data import get_all_item_data      # loads item_no, replenishment_system, etc.
from utils.config_utils import configure_logging, set_pandas_display_options

logger = configure_logging()


# ─── 1. Build a BOM graph (cycle‑tolerant) ────────────────────────────────────
def build_bom_graph(
    bom_df: pd.DataFrame,
    tolerate_cycles: bool = True,
) -> nx.DiGraph:
    """
    Build a directed BOM graph from bom_df (cols: production_bom_no, component_no, total).
    If cycles exist and tolerate_cycles=True, removes one edge per cycle to break loops.
    """
    g = nx.DiGraph()
    # Add all positive‑qty edges
    for _, r in bom_df.iterrows():
        parent = r["production_bom_no"]
        child  = r["component_no"]
        qty    = float(r["total"])
        if qty > 0:
            g.add_edge(parent, child, qty_per=qty)

    # If already acyclic, return immediately
    if nx.is_directed_acyclic_graph(g):
        logger.info("BOM graph built: %d nodes, %d edges (no cycles).",
                    g.number_of_nodes(), g.number_of_edges())
        return g

    # Otherwise detect cycles
    cycles = list(nx.simple_cycles(g))
    msg = f"Detected {len(cycles)} BOM cycle(s): {cycles}"
    if not tolerate_cycles:
        raise ValueError(msg)

    # Remove the edge that closes each cycle
    removed = []
    for cyc in cycles:
        if len(cyc) >= 2:
            tail, head = cyc[-2], cyc[-1]
            if g.has_edge(tail, head):
                g.remove_edge(tail, head)
                removed.append((tail, head))

    logger.warning(
        "%s  Removed %d edge(s) to break cycles: %s",
        msg, len(removed), removed
    )

    # Verify it's now acyclic
    if not nx.is_directed_acyclic_graph(g):
        raise ValueError("Graph still contains cycles after removal. Please inspect BOM data.")

    logger.info("Cycle‑tolerant BOM graph: %d nodes, %d edges.",
                g.number_of_nodes(), g.number_of_edges())
    return g


# ─── 2. Explosion utility ─────────────────────────────────────────────────────
def explode_parent(
    g: nx.DiGraph,
    parent: str,
    parent_qty: float = 1.0,
) -> List[Dict]:
    """
    Depth‑first walk multiplying qty_per across edges.
    Returns list of dicts with keys:
      parent_item, level, parent_index, component_item, qty_per, total_qty
    """
    records = []
    # stack entries: (current_node, qty_so_far, level, direct_parent)
    stack = [(parent, parent_qty, 0, parent)]

    while stack:
        node, req_qty, level, direct_parent = stack.pop()
        children = list(g.successors(node))
        # Leaf component?
        if not children:
            records.append({
                "parent_item":    parent,
                "level":          level,
                "parent_index":   direct_parent,
                "component_item": node,
                "qty_per":        1.0 if level == 0 else g[direct_parent][node]["qty_per"],
                "total_qty":      req_qty,
            })
            continue

        # Otherwise descend
        for child in children:
            edge_qty = g[node][child]["qty_per"]
            stack.append((child, req_qty * edge_qty, level + 1, node))

    return records


# ─── 3. End‑to‑end explosion for Output parents ───────────────────────────────
def explode_output_boms(bom_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode multi‑level BOMs only for items where replenishment_system == 'Output'.
    Returns a DataFrame with exploded hierarchy.
    """
    g = build_bom_graph(bom_df)

    # Select only parents of type 'Output'
    output_parents = item_df.loc[
        item_df.replenishment_system == "Output", "item_no"
    ].unique().tolist()

    logger.info("Exploding %d top‑level Output parents", len(output_parents))

    all_records: List[Dict] = []
    for parent in output_parents:
        if parent in g:
            all_records.extend(explode_parent(g, parent, 1.0))

    # If nothing exploded, return empty schema
    if not all_records:
        cols = ["order","parent_item","level","parent_index","component_item","qty_per","total_qty"]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(all_records)
    df.insert(0, "order", range(1, len(df) + 1))
    return df.sort_values(["parent_item","level","component_item"])


# ─── 4. Main script entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    set_pandas_display_options()

    bom_df  = get_all_bom_data()
    item_df = get_all_item_data()

    if bom_df is None or item_df is None:
        logger.error("Missing BOM or Item data — aborting.")
        sys.exit(1)

    exploded = explode_output_boms(bom_df, item_df)

    # Preview
    print("\nExploded BOM preview:")
    print(exploded.head(20).to_string(index=False))
    print("\nColumns:", list(exploded.columns))
    print("Total rows:", len(exploded))

    # Save to Parquet for downstream
    out_path = os.path.join(os.path.dirname(__file__), "exploded_bom_output.parquet")
    exploded.to_parquet(out_path, index=False)
    logger.info("Exploded BOM written to %s", out_path)
